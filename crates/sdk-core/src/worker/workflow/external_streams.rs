//! External Workflow Stream wait tracking (C2).
//!
//! Plain types plus the transition logic that decides whether a readiness notification, a park
//! confirmation, or a timer expiry is acted on or discarded as stale. Nothing here starts a timer,
//! issues an activation, or touches a `ManagedRun` -- the state machine is a pure function so both
//! orderings of the park/readiness race can be tested without a running worker.
//!
//! Three generations exist and are deliberately named separately:
//!
//! | Name                    | Scope                       | Increments when                                |
//! |-------------------------|-----------------------------|------------------------------------------------|
//! | `wait_generation`       | one subscription            | that subscription re-enters the blocked state  |
//! | `quiescence_generation` | one complete blocked snapshot | the Workflow becomes quiescent again         |
//! | `park_generation`       | one confirmed park of a set | a park set is confirmed                        |
//!
//! `park_generation` is not a fourth counter: it takes the value of the `quiescence_generation`
//! that was parked.

// These types land before the code that drives them: C3 routes the local inputs, C4 exposes the
// readiness and status calls, and C6 gives `ManagedRun` a wait set to hold. Until then only the
// unit tests below construct them.
#![allow(dead_code)]

use std::{
    collections::{HashMap, HashSet},
    time::Duration,
};

/// The reserved `park_generation` meaning "the sender knows of no confirmed park".
///
/// Quiescence generations start at 1, so this cannot collide with a real one.
pub(crate) const UNPARKED_WAKE_GENERATION: u64 = 0;

/// Where one subscription's wait sits in the retention/park lifecycle.
///
/// Only `BlockedWftOpen`, `Ready`, and `Parking` retain the Workflow Task. A `Parked` wait is
/// still logically pending -- the subscription has not gone away -- but it no longer holds the
/// task open, which is the whole point of parking.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ExternalWaitStatus {
    /// Blocked, with the Workflow Task retained on its behalf.
    BlockedWftOpen,
    /// Core has accepted readiness for this wait and owes lang an activation.
    Ready,
    /// A park handshake is in flight for the set this wait belongs to.
    Parking,
    /// The park was confirmed through the backend. No longer retains the task.
    Parked,
}

impl ExternalWaitStatus {
    /// Whether a wait in this state holds the current Workflow Task open.
    pub(crate) fn retains_wft(self) -> bool {
        match self {
            ExternalWaitStatus::BlockedWftOpen
            | ExternalWaitStatus::Ready
            | ExternalWaitStatus::Parking => true,
            ExternalWaitStatus::Parked => false,
        }
    }
}

/// One subscription's wait, as Core tracks it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ExternalWaitState {
    pub(crate) wait_id: u32,
    /// Increments each time this wait re-enters the blocked state. A readiness notification
    /// naming an older value refers to a block that has already been resolved.
    pub(crate) wait_generation: u64,
    pub(crate) status: ExternalWaitStatus,
    /// Normally set after a write fence. One fenced stream does not park the Workflow Task.
    pub(crate) immediately_parkable: bool,
}

impl ExternalWaitState {
    pub(crate) fn new(wait_id: u32, wait_generation: u64, immediately_parkable: bool) -> Self {
        Self {
            wait_id,
            wait_generation,
            status: ExternalWaitStatus::BlockedWftOpen,
            immediately_parkable,
        }
    }
}

/// What a readiness notification did, and what the caller must do about it.
///
/// The five variants are not cosmetic: `NoOpenWorkflowTask` is the healthy state between Workflow
/// Tasks after a command-producing completion or a rollover, and reporting it as a missing Run
/// would both corrupt the metric and tell the watcher to tear itself down while it is still
/// needed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ReadinessOutcome {
    /// Readiness was serialized while the Workflow Task was still open. The caller cancels the
    /// idle timer, aborts any in-flight park, and issues or augments a resolve activation.
    Accepted,
    /// The wait exists but its `wait_generation` moved on. Re-probe; do not signal.
    Stale,
    /// A confirmed `park_generation` exists for this wait. The watcher sends the wake Signal.
    Parked,
    /// The Run is cached and its waits are registered, but no Workflow Task is open.
    NoOpenWorkflowTask,
}

/// The read-only answer to "what state is this Run's wait set in?".
///
/// Deliberately *not* the readiness call: readiness means "a record is buffered", so probing with
/// it would assert something false and manufacture a spurious activation during shutdown.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RunStreamStatus {
    /// A Workflow Task is open and retained by this wait set.
    WftOpen,
    /// The complete set is parked; a confirmed park generation exists.
    Parked,
    /// Waits are registered but no Workflow Task is open.
    NoOpenWorkflowTask,
}

/// Why a set of waits is being parked.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ParkTrigger {
    /// The global quiescence timer expired.
    IdleTimeout,
    /// Every wait in the quiescent snapshot is `immediately_parkable`.
    AllWriteFenced,
}

/// The result of asking the set to begin parking.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ParkStartOutcome {
    /// Every wait moved to `Parking`; issue `PrepareExternalStreamPark`.
    Started(ParkTrigger),
    /// The generation named is not the current one, so nothing was done.
    StaleGeneration,
    /// At least one wait is `Ready`, so parking would race a readiness Core already accepted.
    AlreadyReady,
}

/// The result of applying lang's `ExternalStreamParkResult`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ParkResolution {
    /// The park stands. The caller writes the marker and completes the Workflow Task.
    Confirmed,
    /// A recheck found records. The caller issues a normal resolve activation instead of running
    /// user code from inside the park path.
    Aborted,
    /// The confirmation named a generation that is no longer parking -- readiness beat it, or a
    /// later quiescent snapshot replaced it. Discarded with no effect.
    StaleGeneration,
}

/// Whether the accumulated annotation may be cleared, and the invariant that answers it.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
#[error(
    "external stream annotation of {0} byte(s) is non-empty with no Workflow Task open; an \
     accumulated, unwritten annotation exists only while a Workflow Task is open"
)]
pub(crate) struct UnwrittenAnnotationError(usize);

/// Core's view of one Run's complete external stream wait set.
///
/// Core is annotation-blind: `replay_annotation` is an opaque byte buffer it appends to and hands
/// back, and no method here parses any of it.
#[derive(Debug, Default)]
pub(crate) struct ExternalWaitSet {
    /// Identifies the current complete blocked snapshot. Starts at 0 meaning "never quiescent";
    /// the first snapshot is 1, which is why `UNPARKED_WAKE_GENERATION` can be 0.
    quiescence_generation: u64,
    waits: HashMap<u32, ExternalWaitState>,
    /// Wait ids known ready but not yet shipped in an activation. Coalescing lives here so
    /// several notifications arriving while an activation is outstanding become one next time.
    ready_wait_ids: HashSet<u32>,
    idle_timeout: Option<Duration>,
    /// Set when a park is confirmed; cleared when the set becomes quiescent again.
    park_generation: Option<u64>,
    /// True while a Workflow Task is open for this Run.
    wft_open: bool,
    /// The accumulated, unwritten replay annotation for the current Workflow Task.
    replay_annotation: Vec<u8>,
}

impl ExternalWaitSet {
    pub(crate) fn new() -> Self {
        Self::default()
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.waits.is_empty()
    }

    pub(crate) fn quiescence_generation(&self) -> u64 {
        self.quiescence_generation
    }

    pub(crate) fn park_generation(&self) -> Option<u64> {
        self.park_generation
    }

    pub(crate) fn idle_timeout(&self) -> Option<Duration> {
        self.idle_timeout
    }

    pub(crate) fn replay_annotation(&self) -> &[u8] {
        &self.replay_annotation
    }

    pub(crate) fn wait(&self, wait_id: u32) -> Option<&ExternalWaitState> {
        self.waits.get(&wait_id)
    }

    /// The wait ids Core has accepted readiness for and not yet shipped.
    pub(crate) fn take_ready_wait_ids(&mut self) -> Vec<u32> {
        let mut ids: Vec<_> = self.ready_wait_ids.drain().collect();
        ids.sort_unstable();
        ids
    }

    pub(crate) fn has_pending_readiness(&self) -> bool {
        !self.ready_wait_ids.is_empty()
    }

    /// Whether this set holds the current Workflow Task open.
    ///
    /// A set with no waits retains nothing; a set every member of which is `Parked` retains
    /// nothing either. Core must not complete the task merely because *one* member went idle.
    pub(crate) fn retains_wft(&self) -> bool {
        self.waits.values().any(|w| w.status.retains_wft())
    }

    /// Whether the whole set is parkable without waiting out the idle timeout.
    ///
    /// A fence on one stream alone does not qualify: the Workflow Task parks early only when
    /// *every* active wait is immediately parkable.
    pub(crate) fn all_immediately_parkable(&self) -> bool {
        !self.waits.is_empty() && self.waits.values().all(|w| w.immediately_parkable)
    }

    pub(crate) fn set_wft_open(&mut self, open: bool) {
        self.wft_open = open;
    }

    /// Records a complete quiescent snapshot, starting a new `quiescence_generation`.
    ///
    /// The waits given are the complete set the Workflow is blocked on. Any wait not listed is
    /// dropped: the subscription was cancelled or the Workflow is no longer blocked on it.
    pub(crate) fn become_quiescent(
        &mut self,
        waits: impl IntoIterator<Item = ExternalWaitState>,
        idle_timeout: Duration,
    ) -> u64 {
        self.quiescence_generation += 1;
        self.waits = waits.into_iter().map(|w| (w.wait_id, w)).collect();
        self.ready_wait_ids.clear();
        self.idle_timeout = Some(idle_timeout);
        // A new quiescent snapshot supersedes any earlier park: the Workflow ran again, so
        // whatever generation a producer may still be holding is no longer the live one.
        self.park_generation = None;
        self.wft_open = true;
        self.quiescence_generation
    }

    /// Applies a readiness notification, resolving the park/readiness race.
    ///
    /// This is the pure function both orderings of that race resolve to. Readiness accepted
    /// *before* a park is confirmed wins, and the confirmation for that generation is then stale;
    /// a confirmation that lands first wins, and readiness after it reports `Parked` so the
    /// watcher sends a wake Signal instead of assuming Core will activate.
    pub(crate) fn notify_ready(&mut self, wait_id: u32, wait_generation: u64) -> ReadinessOutcome {
        let Some(wait) = self.waits.get_mut(&wait_id) else {
            // The wait is not registered on this Run at all. That is not "stale" -- there is
            // nothing here whose generation could have moved on.
            return ReadinessOutcome::NoOpenWorkflowTask;
        };

        if wait_generation != wait.wait_generation {
            return ReadinessOutcome::Stale;
        }

        match wait.status {
            // A confirmed park is the one state local readiness cannot be delivered into: the
            // Workflow Task is gone and a producer's wake Signal is the only way back.
            ExternalWaitStatus::Parked => ReadinessOutcome::Parked,
            ExternalWaitStatus::BlockedWftOpen
            | ExternalWaitStatus::Ready
            | ExternalWaitStatus::Parking => {
                if !self.wft_open {
                    return ReadinessOutcome::NoOpenWorkflowTask;
                }
                // Readiness during `Parking` aborts that parking generation -- see
                // `resolve_park`, which will report the later confirmation as stale.
                wait.status = ExternalWaitStatus::Ready;
                self.ready_wait_ids.insert(wait_id);
                ReadinessOutcome::Accepted
            }
        }
    }

    /// The read-only status probe, which must leave the set untouched.
    pub(crate) fn run_status(&self) -> RunStreamStatus {
        if self.park_generation.is_some()
            && self
                .waits
                .values()
                .all(|w| w.status == ExternalWaitStatus::Parked)
        {
            RunStreamStatus::Parked
        } else if self.wft_open && self.retains_wft() {
            RunStreamStatus::WftOpen
        } else {
            RunStreamStatus::NoOpenWorkflowTask
        }
    }

    /// Moves the complete set into `Parking` so `PrepareExternalStreamPark` can be issued.
    pub(crate) fn start_parking(
        &mut self,
        quiescence_generation: u64,
        trigger: ParkTrigger,
    ) -> ParkStartOutcome {
        if quiescence_generation != self.quiescence_generation || self.waits.is_empty() {
            return ParkStartOutcome::StaleGeneration;
        }
        if self
            .waits
            .values()
            .any(|w| w.status == ExternalWaitStatus::Ready)
        {
            return ParkStartOutcome::AlreadyReady;
        }
        for wait in self.waits.values_mut() {
            wait.status = ExternalWaitStatus::Parking;
        }
        ParkStartOutcome::Started(trigger)
    }

    /// Applies lang's answer to `PrepareExternalStreamPark`.
    pub(crate) fn resolve_park(
        &mut self,
        quiescence_generation: u64,
        confirmed: bool,
    ) -> ParkResolution {
        if quiescence_generation != self.quiescence_generation {
            return ParkResolution::StaleGeneration;
        }
        // Parking is all-or-nothing across the complete set, so readiness accepted while the
        // handshake was in flight makes this confirmation stale even though it names the current
        // generation -- and even if it moved only *one* wait out of `Parking`. Checking `any`
        // here instead of `all` would confirm a park for a set with a ready wait in it, losing
        // that wait's record until a producer happened to signal.
        if !self
            .waits
            .values()
            .all(|w| w.status == ExternalWaitStatus::Parking)
        {
            return ParkResolution::StaleGeneration;
        }

        if confirmed {
            for wait in self.waits.values_mut() {
                wait.status = ExternalWaitStatus::Parked;
            }
            // A park generation *is* the quiescence generation that was parked, not a fourth
            // counter, which is what lets a wake Signal carry one number that Core can match.
            self.park_generation = Some(quiescence_generation);
            self.wft_open = false;
            ParkResolution::Confirmed
        } else {
            self.abort_parking();
            ParkResolution::Aborted
        }
    }

    /// Returns every wait in `Parking` to `BlockedWftOpen`, bumping each `wait_generation`.
    ///
    /// Bumping is what makes an in-flight readiness notification for the aborted attempt stale
    /// rather than resolving against the new block.
    fn abort_parking(&mut self) {
        for wait in self.waits.values_mut() {
            if wait.status == ExternalWaitStatus::Parking {
                wait.status = ExternalWaitStatus::BlockedWftOpen;
                wait.wait_generation += 1;
            }
        }
    }

    /// Validates a wake Signal's `park_generation` against this Run.
    ///
    /// `park_generation = 0` is the unparked wake and is always accepted as a recheck request:
    /// the runtime rechecks every active subscription on wakeup regardless, so an unnecessary one
    /// costs at most one empty Workflow Task. A *non-zero* generation the Run does not recognize
    /// is a claim that turned out to be wrong, and is ignored as stale.
    pub(crate) fn accepts_wake_generation(&self, park_generation: u64) -> bool {
        if park_generation == UNPARKED_WAKE_GENERATION {
            return true;
        }
        self.park_generation == Some(park_generation)
    }

    /// Injects a confirmed park generation directly.
    ///
    /// Used by the wake-Signal path's tests before the live park handshake exists, and by
    /// nothing else.
    #[cfg(test)]
    pub(crate) fn force_parked(&mut self, park_generation: u64) {
        for wait in self.waits.values_mut() {
            wait.status = ExternalWaitStatus::Parked;
        }
        self.park_generation = Some(park_generation);
        self.wft_open = false;
    }

    /// Appends an opaque observation delta.
    pub(crate) fn accumulate_annotation(&mut self, delta: &[u8]) {
        self.replay_annotation.extend_from_slice(delta);
    }

    /// Takes the accumulated annotation, asserting the unwritten-annotation invariant.
    ///
    /// Deltas arrive only on activation completions, activations exist only under a Workflow
    /// Task, and every completion path writes the accumulated annotation as exactly one marker
    /// and clears it. So a non-empty annotation with no Workflow Task open is a bug in one of
    /// those paths, not a state to recover from.
    pub(crate) fn take_annotation(&mut self) -> Result<Vec<u8>, UnwrittenAnnotationError> {
        if !self.wft_open && !self.replay_annotation.is_empty() {
            return Err(UnwrittenAnnotationError(self.replay_annotation.len()));
        }
        Ok(std::mem::take(&mut self.replay_annotation))
    }

    /// The marker's wait list: every wait and the generation it was at.
    pub(crate) fn marker_waits(&self) -> Vec<(u32, u64)> {
        let mut out: Vec<_> = self
            .waits
            .values()
            .map(|w| (w.wait_id, w.wait_generation))
            .collect();
        out.sort_unstable();
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const IDLE: Duration = Duration::from_secs(1);

    fn quiescent_set(wait_ids: &[u32]) -> ExternalWaitSet {
        let mut set = ExternalWaitSet::new();
        set.become_quiescent(
            wait_ids
                .iter()
                .map(|id| ExternalWaitState::new(*id, 0, false)),
            IDLE,
        );
        set
    }

    #[test]
    fn quiescence_generations_start_at_one() {
        // 0 is reserved as the unparked-wake sentinel, so a real generation must never be 0.
        let set = quiescent_set(&[1]);
        assert_eq!(set.quiescence_generation(), 1);
        assert_ne!(set.quiescence_generation(), UNPARKED_WAKE_GENERATION);
    }

    #[test]
    fn readiness_at_the_current_generation_is_accepted() {
        let mut set = quiescent_set(&[1, 2]);

        assert_eq!(set.notify_ready(1, 0), ReadinessOutcome::Accepted);
        assert_eq!(set.wait(1).unwrap().status, ExternalWaitStatus::Ready);
        assert_eq!(set.take_ready_wait_ids(), vec![1]);
    }

    #[test]
    fn a_stale_wait_generation_is_rejected() {
        let mut set = quiescent_set(&[1]);

        assert_eq!(set.notify_ready(1, 7), ReadinessOutcome::Stale);
        assert_eq!(
            set.wait(1).unwrap().status,
            ExternalWaitStatus::BlockedWftOpen
        );
        assert!(!set.has_pending_readiness());
    }

    #[test]
    fn readiness_for_an_unregistered_wait_is_not_reported_as_stale() {
        // "Stale" tells the watcher to re-probe. There is nothing here to re-probe against, and
        // reporting it as stale would spin the watcher forever.
        let mut set = quiescent_set(&[1]);
        assert_eq!(
            set.notify_ready(99, 0),
            ReadinessOutcome::NoOpenWorkflowTask
        );
    }

    #[test]
    fn readiness_with_no_open_workflow_task_reports_the_healthy_state() {
        let mut set = quiescent_set(&[1]);
        set.set_wft_open(false);

        assert_eq!(set.notify_ready(1, 0), ReadinessOutcome::NoOpenWorkflowTask);
        // Not `Parked`: there is no confirmed park generation, so a producer has nothing to
        // observe and the watcher must send an *unparked* wake.
        assert_eq!(set.park_generation(), None);
    }

    #[test]
    fn readiness_coalesces_until_taken() {
        let mut set = quiescent_set(&[1, 2, 3]);

        set.notify_ready(3, 0);
        set.notify_ready(1, 0);
        set.notify_ready(1, 0);

        assert_eq!(set.take_ready_wait_ids(), vec![1, 3]);
        assert!(!set.has_pending_readiness());
    }

    // --- the park/readiness race, both orderings ---------------------------

    #[test]
    fn readiness_before_confirmation_wins_and_the_confirmation_is_stale() {
        let mut set = quiescent_set(&[1, 2]);
        let quiesc = set.quiescence_generation();

        assert_eq!(
            set.start_parking(quiesc, ParkTrigger::IdleTimeout),
            ParkStartOutcome::Started(ParkTrigger::IdleTimeout)
        );
        // Readiness lands while the handshake is in flight.
        assert_eq!(set.notify_ready(1, 0), ReadinessOutcome::Accepted);

        // The confirmation names the current generation but no wait is `Parking` any more.
        assert_eq!(
            set.resolve_park(quiesc, true),
            ParkResolution::StaleGeneration
        );
        assert_eq!(set.park_generation(), None);
        assert!(set.retains_wft());
    }

    #[test]
    fn confirmation_before_readiness_wins_and_readiness_reports_parked() {
        let mut set = quiescent_set(&[1, 2]);
        let quiesc = set.quiescence_generation();

        set.start_parking(quiesc, ParkTrigger::IdleTimeout);
        assert_eq!(set.resolve_park(quiesc, true), ParkResolution::Confirmed);

        assert_eq!(set.notify_ready(1, 0), ReadinessOutcome::Parked);
        assert_eq!(set.park_generation(), Some(quiesc));
        assert!(!set.retains_wft());
    }

    #[test]
    fn a_confirmation_for_an_old_generation_is_discarded() {
        let mut set = quiescent_set(&[1]);
        let first = set.quiescence_generation();
        set.start_parking(first, ParkTrigger::IdleTimeout);

        // The Workflow ran again and became quiescent at a new snapshot.
        set.become_quiescent([ExternalWaitState::new(1, 1, false)], IDLE);

        assert_eq!(
            set.resolve_park(first, true),
            ParkResolution::StaleGeneration
        );
        assert_eq!(set.park_generation(), None);
    }

    #[test]
    fn an_aborted_park_bumps_wait_generations() {
        // Otherwise a readiness notification issued for the aborted attempt would resolve
        // against the new block and deliver twice.
        let mut set = quiescent_set(&[1, 2]);
        let quiesc = set.quiescence_generation();
        set.start_parking(quiesc, ParkTrigger::IdleTimeout);

        assert_eq!(set.resolve_park(quiesc, false), ParkResolution::Aborted);

        assert_eq!(set.wait(1).unwrap().wait_generation, 1);
        assert_eq!(set.notify_ready(1, 0), ReadinessOutcome::Stale);
        assert_eq!(set.notify_ready(1, 1), ReadinessOutcome::Accepted);
    }

    #[test]
    fn parking_a_set_with_a_ready_wait_is_refused() {
        let mut set = quiescent_set(&[1, 2]);
        let quiesc = set.quiescence_generation();
        set.notify_ready(2, 0);

        assert_eq!(
            set.start_parking(quiesc, ParkTrigger::IdleTimeout),
            ParkStartOutcome::AlreadyReady
        );
    }

    #[test]
    fn parking_a_stale_generation_is_refused() {
        let mut set = quiescent_set(&[1]);
        let quiesc = set.quiescence_generation();

        assert_eq!(
            set.start_parking(quiesc + 1, ParkTrigger::IdleTimeout),
            ParkStartOutcome::StaleGeneration
        );
    }

    // --- retention ---------------------------------------------------------

    #[test]
    fn an_empty_set_retains_nothing() {
        let set = ExternalWaitSet::new();
        assert!(!set.retains_wft());
        assert!(!set.all_immediately_parkable());
    }

    #[test]
    fn one_parked_member_does_not_release_the_task() {
        let mut set = quiescent_set(&[1, 2]);
        set.waits.get_mut(&1).unwrap().status = ExternalWaitStatus::Parked;

        assert!(set.retains_wft());
    }

    #[test]
    fn one_fenced_stream_does_not_make_the_set_parkable() {
        let mut set = ExternalWaitSet::new();
        set.become_quiescent(
            [
                ExternalWaitState::new(1, 0, true),
                ExternalWaitState::new(2, 0, false),
            ],
            IDLE,
        );

        assert!(!set.all_immediately_parkable());

        set.waits.get_mut(&2).unwrap().immediately_parkable = true;
        assert!(set.all_immediately_parkable());
    }

    // --- the read-only status probe ----------------------------------------

    #[test]
    fn the_status_probe_leaves_the_set_untouched() {
        let mut set = quiescent_set(&[1, 2]);
        set.notify_ready(1, 0);
        let before = set.marker_waits();
        let ready_before = set.has_pending_readiness();

        assert_eq!(set.run_status(), RunStreamStatus::WftOpen);
        assert_eq!(set.run_status(), RunStreamStatus::WftOpen);

        assert_eq!(set.marker_waits(), before);
        assert_eq!(set.has_pending_readiness(), ready_before);
        assert_eq!(set.wait(1).unwrap().status, ExternalWaitStatus::Ready);
    }

    #[test]
    fn status_distinguishes_parked_from_no_open_task() {
        let mut set = quiescent_set(&[1]);
        let quiesc = set.quiescence_generation();

        set.set_wft_open(false);
        assert_eq!(set.run_status(), RunStreamStatus::NoOpenWorkflowTask);

        set.set_wft_open(true);
        set.start_parking(quiesc, ParkTrigger::IdleTimeout);
        set.resolve_park(quiesc, true);
        assert_eq!(set.run_status(), RunStreamStatus::Parked);
    }

    // --- wake generations --------------------------------------------------

    #[test]
    fn an_unparked_wake_is_always_accepted() {
        let set = quiescent_set(&[1]);
        assert!(set.accepts_wake_generation(UNPARKED_WAKE_GENERATION));
    }

    #[test]
    fn an_unrecognized_nonzero_wake_generation_is_stale() {
        let mut set = quiescent_set(&[1]);
        assert!(!set.accepts_wake_generation(42));

        set.force_parked(7);
        assert!(set.accepts_wake_generation(7));
        assert!(!set.accepts_wake_generation(6));
        // Still accepted -- an unparked wake costs at most one empty Workflow Task.
        assert!(set.accepts_wake_generation(UNPARKED_WAKE_GENERATION));
    }

    // --- the annotation ----------------------------------------------------

    #[test]
    fn deltas_accumulate_in_order_and_are_taken_once() {
        let mut set = quiescent_set(&[1]);
        set.accumulate_annotation(b"one");
        set.accumulate_annotation(b"two");

        assert_eq!(set.replay_annotation(), b"onetwo");
        assert_eq!(set.take_annotation().unwrap(), b"onetwo".to_vec());
        assert!(set.replay_annotation().is_empty());
    }

    #[test]
    fn an_empty_annotation_may_be_taken_with_no_task_open() {
        let mut set = quiescent_set(&[1]);
        set.set_wft_open(false);
        assert_eq!(set.take_annotation().unwrap(), Vec::<u8>::new());
    }

    #[test]
    fn the_unwritten_annotation_invariant_fires() {
        let mut set = quiescent_set(&[1]);
        set.accumulate_annotation(b"delta");
        set.set_wft_open(false);

        assert!(set.take_annotation().is_err());
    }
}
