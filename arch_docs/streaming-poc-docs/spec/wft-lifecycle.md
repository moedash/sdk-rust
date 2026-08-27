# Workflow Task lifecycle

When a Workflow Task is retained, how it ends, and how a subscription is woken in each state it can
be left in.

## Live consumption

While a Workflow Task is open, the runtime reads from the last committed offset and resumes the
Workflow for each data record, across as many activations as draining takes — one activation
delivers a bounded number of records and then blocks so the activation can end, even where more
records are already buffered (`python-runtime.md`).

The Workflow Task ends when either every active subscription has reached a write fence with no
subsequent record immediately available, or no record arrives on any of them before the idle timeout.
The timeout covers producers that crash, omit `finish_writing()`, or do not use the SDK.

## The idle timeout is a Workflow-Task policy

It applies to the **complete set** of subscriptions the Workflow is blocked on, not to each
subscription independently: one idle stream cannot park a Workflow Task another stream is still
driving, and a fence on one stream cannot bypass the timer. So a set whose members are configured
differently must reduce to one value — by `min`, in `wait_id` order, over the quiescent set and
nothing else, which is what makes the result reproduce on replay (ADR-016). Core clamps the result
below the rollover deadline so rollover stays authoritative, and rejects a non-positive timeout as a
malformed completion rather than coercing it.

## Workflow Task rollover is mandatory

A retained Workflow Task is bounded by the server's Workflow Task timeout, and a continuously fed
stream never reaches the idle parking path at all. The runtime therefore always runs a rollover
deadline derived from the Workflow Task timeout and independent of the idle timeout, and completes
the current task early when it wins; every active subscription, cursor, annotation delta, and
readiness generation survives. It needs a timer facility that does not depend on the local-activity
request sink (ADR-017).

**Rollover bounds the Workflow Task, not an activation.** Core decides it between activations and
cannot interrupt one in progress, so it is no protection against a single activation that never
returns. What bounds an activation is the per-activation record budget in `python-runtime.md`.

Rollover also bounds a second effect of holding a task open: while it is retained the server cannot
start another one, so Signals, Updates, and non-legacy Queries queue until it completes. Outstanding
local activities already have this property, but a stream can hold a task open far longer. Retention
latency for those inputs is bounded by the rollover deadline and nothing else, so callers who need
lower latency must lower the Workflow Task timeout rather than the idle timeout.

## Output visibility has its own deadline

Workflow-originated output cannot wait for an arbitrarily long input idle timeout. Each output topic
has `max_publish_latency`, defaulting to 100 milliseconds in the first release. A retained batch uses
the minimum policy of every non-empty topic in that batch; Core preserves the earliest deadline and
clamps it below ordinary Workflow Task rollover (ADR-045).

Python sends `WorkflowOutputStreamBuffered` only when the completion has a quiescent input snapshot,
no user/server command, and no output-capacity rollover. Core retains the task and arms the output
deadline. When it expires, Core issues `FinalizeExternalStreams{OUTPUT_LATENCY}`; Python stages the
batch and returns both the existing finalization command and `WorkflowOutputStreamCommit`. Core then
writes the shared marker and forces a replacement task.

Without a quiescent input snapshot — including an output-only Workflow — or when an ordinary command
already requires reporting the task, output is staged immediately. Capacity also bypasses buffering:
the commit requests a replacement so a blocked `publish()` can continue in a fresh batch. The
latency bound is a healthy-path visibility guarantee, not an availability SLA while the provider or
Temporal service is unavailable.

## Workflow Task admission is lossless

A Run owns the task token of its current Workflow Task until that task is reported. A polled
replacement can arrive before the local completion message clears the original even when no
activation jobs remain, because poll results and local completions use different input lanes.

Admission therefore buffers whenever `ManagedRun` already owns a Workflow Task; pending jobs are
not the only evidence of outstanding work. The lower-level `_incoming_wft` invariant remains a debug
panic, while its release path defensively queues the replacement and returns without overwriting the
original slot. The original token is reported first, and only then does the replacement drain
(ADR-043). Substitution is never recovery: it loses the token for a task the Worker still owns.

## Three independent questions at every activation return

1. **Did replay-visible stream state change?** If yes, a `WorkflowStreamProgress` command carries the
   observation delta, on *every* completion path. This is what commits the cursor boundary, and it is
   not conditional on why the Workflow Task ended: if a consumed record influences a command that
   lands in History while the consumption itself is never marked, replay re-delivers that record
   while the command it produced is already durable, and the divergence surfaces as an unrelated
   nondeterminism error much later (ADR-005).
2. **Is the Workflow blocked on streams?** If yes, a `WorkflowStreamQuiescent` command reports the
   complete quiescent snapshot. This registers the wait set with Core and — on the completions that
   can be held open — starts the idle timer.
3. **Does this Workflow Task hold logical output?** If yes, Python either stages it and sends
   `WorkflowOutputStreamCommit`, or sends `WorkflowOutputStreamBuffered` when and only when the
   quiescent completion is retainable. Output and input use the same marker and activation-segment
   schedule.

A completion may carry any compatible combination of those commands. `WorkflowStreamProgress` never
implies retention, and `WorkflowStreamQuiescent` carries no annotation data (ADR-004).

| Condition at return | Python sends | Core does |
|---|---|---|
| No subscriptions, nothing observed | Normal completion | Reports the WFT to the server |
| Records consumed, no pending stream waits | `WorkflowStreamProgress` | Records the marker, reports the WFT |
| Records consumed **and** a terminal command (complete, fail, cancel, continue-as-new) | `WorkflowStreamProgress` **ordered before** the terminal command | Records the marker, then applies the terminal command |
| Stream waits pending **and** server-bound commands (timer, activity, child workflow, signal) | `WorkflowStreamProgress` ordered before those commands, then `WorkflowStreamQuiescent` with the same complete snapshot | Records the marker, **registers the waits, arms no timer**, reports the WFT |
| Stream waits pending, no other command, **nothing consumed** | `WorkflowStreamProgress` carrying the header on first observation plus an **empty segment**, then `WorkflowStreamQuiescent` | Accumulates the delta, registers the waits, retains the WFT, starts the idle timer |
| Stream waits pending, no other command, records consumed | `WorkflowStreamProgress`, then `WorkflowStreamQuiescent` with the **complete** set | Accumulates the delta, `ActivationCompleteOutcome::DoNothing`, retains the WFT, starts **one** idle timer for the set |
| Logical output, no retainable quiescent snapshot or a user/server command | `WorkflowOutputStreamCommit` after provider staging | Records the output manifest in the shared marker and reports the WFT |
| Logical output with a retainable quiescent snapshot and no command or capacity rollover | `WorkflowOutputStreamBuffered`, plus the ordinary input progress/quiescence commands | Retains the WFT and arms the earliest output deadline without provider staging |
| Logical output reaches record or logical-byte capacity | `WorkflowOutputStreamCommit{request_rollover=true}` after staging | Records the shared marker, reports the WFT, and requests a replacement |

**Command ordering is normative**: `WorkflowStreamProgress` precedes every command whose value could
depend on consumed data, and so does the marker recording it. On replay that is what guarantees a
record is validated before the command derived from it is matched.

Successive deltas accumulate while the task is retained and are written as **one** marker when it
finally completes, whether from parking, rollover, or a later activation that produces commands.

### Registering a wait set is not retaining the Workflow Task

The two are decided separately, and only the second is refusable. A completion carrying server-bound
commands must be reported so the server can act on them, so it cannot ask for retention — but the
Workflow is no less blocked on those subscriptions for it, and **Core is the only place that set is
recorded**. Core therefore registers the waits and arms nothing: no idle timer, no rollover deadline,
no all-fenced immediate park, because each of those exists to end a *retained* task.

Welding the two together — withholding the snapshot on any completion that cannot be retained for —
leaves a Workflow that starts a timer and first blocks on a stream in the same activation registered
nowhere. Readiness then has no wait to resolve against, a wake Signal marks nothing ready, and every
Workflow Task a wake produces completes empty: the Run is unresumable by any wake, with its records
sitting in the stream. That is a permanent deadlock reachable from ordinary user code.

Two more completions register without retaining. A **replayed** completion must, because the wait set
is per-Worker runtime state rather than History and nothing else rebuilds it, while retaining a
replayed task would arm wall-clock deadlines against a boundary the marker has already fixed. A
**query answered on its own activation** refuses retention in order to report the answer, which makes
it a boundary Core decided and so one owed a finalization round trip before any marker is written.

The mirror-image rule: a boundary that tears the Run down, or that has already asked lang to
finalize, registers **nothing** — re-recording a snapshot there would bump the quiescence generation
underneath a job already in flight against it.

## Race-free parking

Parking is all-or-nothing across the complete set of subscriptions the Workflow is blocked on, and
uses a generation-based handshake coordinated through the backend:

1. Core marks every wait `Parking` and sends `PrepareExternalStreamPark`. This is runtime-internal;
   no user Workflow code runs.
2. Python installs a park intent per **subscription**, keyed `(stream key, wait_id)`, carrying its
   cursor boundary and the park generation. **The set is the one the job names**, not every
   subscription the Worker holds: a registered subscription nobody is blocked on could otherwise
   abort a legitimate park on a record that belongs to no wait Core is parking, and it would leave
   an intent behind for a park that never covered it.
3. After all intents are installed, it rechecks every stream in that set.
4. All still empty → `ParkSetConfirmed`, carrying the terminal `final_observation_delta`. Core
   appends it, records the marker, and completes the WFT.
5. Any stream ready → Python removes every installed intent and returns `StreamSetBecameReady`. Core
   aborts that parking generation and issues `ResolveExternalStreamWaits`.

### Output and parking use the same terminal race

If the runtime holds logical output while it answers `PrepareExternalStreamPark`, the answer depends
on the backend recheck (ADR-045):

- Park confirmed: Python stages output and orders `WorkflowOutputStreamCommit` before
  `ExternalStreamParkResult(confirmed)`. Core records one shared park marker and does not force a
  replacement.
- Stream became ready: Python sends `WorkflowOutputStreamBuffered` before
  `ExternalStreamParkResult(became_ready)`. No output is staged and Core retains the original
  earliest deadline.
- Readiness wins after Python has already staged for a confirmation: Core rejects the stale park
  terminal, resolves the abandoned park to remove every installed intent, then finalizes with
  `TASK_COMPLETED`, records the already staged commit once, and forces a replacement.
- The output deadline wins while preparation is outstanding: Core invalidates the park generation,
  resolves it for the same cleanup, then finalizes with `OUTPUT_LATENCY`. A commit already carried by
  the losing result is retained; otherwise Python stages during finalization. Core records it once
  and forces a replacement.

The output timer remains armed during park preparation. Suppressing it would let a stalled park
provider violate the output visibility bound indefinitely. Late timer and park results are stale and
cannot produce another terminal or marker.

An attempt that ends before Core confirms it withdraws every intent it installed — whether an
install raised, a recheck raised, or **the attempt was cancelled**. Cancellation is not the exotic
member of that list but the likeliest one: an activation withdrawn by Core and a Worker shutting
down both abandon a half-installed park that way, and in Python cancellation does not arrive as an
ordinary exception, so a rollback written for ordinary exceptions is exactly the rollback that
misses it (ADR-032). A park visible to producers for a generation Core never confirmed is one they
send wakes against and Core discards as stale, and the eviction that follows takes with it the only
record that those intents were installed at all.

A producer appends its record *before* it observes or claims the park generation, and only then sends
the wake Signal. That ordering plus the recheck at step 3 is what closes the empty-check/completion
race: an append is either seen by the recheck or paired with a Signal. Readiness accepted before
`ParkSetConfirmed` wins, and the confirmation for that generation is then stale.

**A park intent exists only while its park is outstanding.** Holding that true takes more than the
moments at which a removal is decided, because the intent is durable backend state while nothing that
knows about it is: the record of which Worker installed it dies with that Worker, and the
subscription that record hangs off can be closed while the Run runs on for another week.

Three moments decide a removal:

- **A resolve ends a park this Run is sitting in.** `ResolveExternalStreamWaits` is the notice,
  because it covers both ways a confirmed park ends and Core's own state moving on is not something
  the backend can observe. Python removes the intents it installed for that park.
- **Registration reconciles an intent this Worker inherited.** A Worker registering a wait that
  already has an intent in the backend, having installed none itself, removes it. Registration is
  where such an intent becomes visible and where its status is unambiguous: a subscription is
  registered by user Workflow code running, and no user code runs inside a park, so an intent found
  there belongs to a park that is over. *Reading* it is what records it. An inherited intent is
  mirrored nowhere on this Worker, so until the read writes it down there is nothing for any removal
  path to work from, and the resolve and the cancellation both look at the same absence and do
  nothing.
- **A cancellation takes back the intent of the wait it drops.** Closing a subscription removes it
  from the manager, so its intent is removed there — under the same serialization as this Run's
  parking and resolving, and before the watcher stops — rather than left to either point above. A
  closed wait is never registered again for the life of the Run, and the resolve iterates the
  subscriptions the manager still holds, so neither of them ever reaches it (ADR-030).

A removal that one of those decided on and the backend did not confirm is **owed**: recorded per
Run against `(stream key, wait_id)` and retried autonomously with bounded exponential backoff.
Parks, resolves, registrations and evictions still drain it eagerly under the Run's park lock, but
backend recovery alone is sufficient for progress. A ledger entry says *a removal was decided on
and has not been confirmed*, not *an intent exists* — which is what makes draining it safe from any
of those paths rather than only from the one that recorded it. The entry holds nothing that a
teardown could invalidate: no watcher, no buffer, no connection beyond the backend the removal has
to go through. It is made **before** the backend call, because a call that never comes back — the
backend raised, the task was cancelled mid-await — owes the removal exactly as an error does, and
the subscription it was reached through is usually dropped in the same breath. A fresh install at
that key supersedes it. Why the ledger is manager state rather than a field on the subscription is
ADR-031.

The retry task is strongly held per Run by the manager. Eviction drops the cached Run but not an
owed-removal ledger or its task, so cleanup can finish while that Run remains absent. Worker
shutdown cancels and awaits the manager-owned tasks within its shutdown grace period, and then makes
one last bounded pass over whatever is still owed: cancelling a loop that is sleeping out a backoff
throws away an attempt the recovered backend would have answered, and eviction cannot make that
attempt while shutting down without waiting on the very lock a stuck loop is holding. What survives
that pass is logged, because an intent still installed as the process exits suppresses the unparked
wake for whoever picks the Run up next.

**Every hold of the park lock that is not inside an activation is time-bounded.** The lock serializes
one Run's park work, and the paths that take it off the activation path -- the retry loop, the
registration-time reconciliation, the close, the last pass at shutdown -- all reach a backend that
can hang rather than raise. Counting attempts does not bound an attempt that never returns: the task
stops retrying, and the lock the next park, resolve or eviction needs stops being available. A hold
that runs out of time is released and treated as a failed attempt, which leaves the entry owed
exactly as an error does. Holds taken *by* an activation are not bounded, because the wait they own
is the backend exposure the park handshake already has.

**Retiring an intent is not the same as delivering what it silenced.** Every wake sent while a stale
intent was installed named the generation behind it, and Core discards a non-zero generation that is
not the park it holds -- so a record that arrived in that window is buffered on the Worker with its
wake already counted as sent, and the watcher will not report it again without a new append. Every
path that retires an intent therefore hands that record off, one of two ways.

**With the wait still registered here, through local readiness.** That is the drain of a cached Run,
and the registration-time reconciliation whose removal succeeds first time -- the ordinary case for an
inherited intent, and the one that never goes near the ledger. The reconciliation runs concurrently
with the watcher it races, which is how the record gets silenced in the first place. Narrow, because
the subscription is there to be read: only a wait still holding records, so a healthy park costs no
Workflow Task, and never during the sweep, which owns every wake from that point and accounts for
each one.

**With the wait gone, through one unparked wake.** A close drops the subscription and an eviction
drops the Run's map, both deliberately leaving the ledger and its retry alive -- so the removal that a
recovered backend finally lets through is frequently the one with no buffer left to consult and no
readiness channel to use. Conditioning the announcement on a subscription being there is what keeps
the leak fixed while putting the record loss straight back. The wake is unconditional: whether a
remote producer appended during the suppression window was never visible from this Worker, so "a
record arrived" and "none did" are the same observation from here, and only one of them is silent if
the guess goes wrong. It costs one empty Workflow Task, which this design permits. Sent as work of
its own rather than under the park lock, whose budget bounds a single-key backend call and not a
Signal with retries behind it, and awaited by shutdown -- the last of these handoffs is created by the
final owed-removal pass, and a wake merely scheduled as the process exits was never sent.

Both provider outcomes that clear the key hand off -- *removed* and *absent*. An absent key is
cleanup that happened, including a delete this Worker committed and never got the reply to
(`backend-contract.md`), and reading it as a mismatch would strand the record for good. A *mismatch*
hands off nothing: an intent is still installed, so the suppression has not ended, and it belongs to
a park the entry knows nothing about.

**No wake this Worker sends names an intent it has already decided to remove.** An owed removal says
the generation installed at that key is one Core has discarded, so composing a wake from it produces
a Signal the service accepts and Core ignores -- which is worse than a failure, because the send
reports success. The manager holds the ledger, so the manager decides what a wake names rather than
the sender reading `current_park_generation` for itself: an entry in the ledger, or a handoff for an
intent just retired, both compose the unparked wake. That is what makes the shutdown sweep's
accounting true, since the sweep wakes the subscriptions of a Run whose stale intents the final
removal pass has not reached yet. Its wake and that removal's handoff are then one obligation, and it
is recorded as discharged on the entry so the pass does not send it twice.

**Discovery gets the same autonomous retry the removal does.** An owed removal is recorded from an
intent's identity, so a read that never succeeds records nothing -- no ledger entry, no retry task, no
owner. A reconciliation whose reads all fail therefore keeps retrying, with capped backoff, for as
long as this Worker holds the subscription; once a read succeeds and only the removal fails, it hands
the entry to the ledger rather than draining it twice. The subscription is what bounds it, and not
only for tidiness: removing whatever is installed at that key is justified by this Worker holding the
Run with this wait registered on it, and once the subscription is gone the intent found there could
be a park another Worker is sitting in.

**A drain removes only the intent it recorded.** The provider atomically compares the
`park_generation` and Run ID and deletes only on a match; anything else is forgotten rather than
removed. The stream key is stable across a Continue-As-New chain while `wait_id` restarts at 1 in
the successor, so an entry a predecessor Run left behind can name the key of a park a **successor**
is sitting in, and taking that out is strictly worse than the leak the ledger exists to close: it
unparks a Run whose park is real and whose producers have no reason to send anything. A process-local
park lock cannot close this cross-Run, cross-Worker race; the conditional backend operation does
(`backend-contract.md`).

What a left-behind intent costs is the invariant's whole point: `current_park_generation` keeps
answering a generation Core has discarded, each producer wake names it and Core discards it as
stale, and because a parked wake's request ID ignores sender identity the second such wake is
byte-identical to the first and the server deduplicates it away. The Workflow waits forever on a
record that is durably present. Nor is the damage confined to the wait that owns the intent: a
stale intent keeps `parked_wait_ids` non-empty for the whole stream, which suppresses the unparked
fallback for every **other** wait on it, so a Workflow that closed one subscription can lose the
wakes of the ones it is still reading.

Reconciliation reads before it writes, and is serialized against this Run's own parking and
resolving: a Run that never parked owes the backend nothing, and a reconciliation overlapping a
confirming park must not remove the intent that park has just installed — an unwakeable Run produced
by the mechanism that exists to prevent one. It retries a bounded number of times before leaving the
rest to the ledger, and takes that lock **per attempt** rather than holding it across the backoff:
parking is answered inside an activation under Core's deadlock timeout, so a reconciliation asleep
with the Run's park lock would spend an activation's budget on cleanup for a park that is already
over.

The provider holds up the rest: it must stop reporting a generation once its intent is removed
(`backend-contract.md`), or every reader of that call acts on a park that is over.

## Three generations, named separately

| Name | Scope | Increments when | Where it appears |
|---|---|---|---|
| `wait_generation` | one subscription | that subscription re-enters the blocked state | the wait entry in the quiescence report and in readiness notifications |
| `quiescence_generation` | one complete blocked snapshot | the Workflow becomes quiescent again after any resumption | the quiescence report, the park preparation job, and the park result |
| `park_generation` | one confirmed park of a set | a park set is confirmed | the backend park intent and the wake Signal |

`park_generation` takes the value of the `quiescence_generation` that was parked rather than being a
fourth counter. A wake Signal carries `(wait_id, park_generation)`, and those two values are the only
generation state that reaches the backend or the wire; `wait_generation` never leaves the Core/lang
boundary.

A Signal naming a generation the current Run does not recognize is harmless: the runtime rechecks
every active subscription on wakeup regardless — the stream a wake names is only a hint — so a stale
Signal costs at most one empty Workflow Task, which this design permits. A wake sent when **no** park
generation exists carries the reserved `park_generation = 0` and is a recheck request rather than a
stale Signal (ADR-023).

## Completions that leave subscriptions active but unparked

A completion carrying server-bound commands, and a rollover, both end the Workflow Task without a
park handshake. In that state there is no retained task to notify locally and no park generation for
a producer to observe, so a later append has nothing to wake. Three mechanisms cover it, and one of
them always applies:

1. **The consumer's own watchers.** Watchers stay live while the Run is cached, and one that observes
   an append with no Workflow Task open sends the wake Signal itself. This is the normal path, and it
   also covers a producer that crashed after appending.

   That Signal is retried in place until the server acknowledges it, because nothing behind it will
   try again: the watcher moved its prefetch position past the record when it buffered it and goes
   back to waiting for a *new* append, re-arming readiness needs the activation the lost wake was
   supposed to cause, and the idle timer runs only while a Workflow Task is retained, which is
   exactly what this state does not have. A single unacknowledged attempt is a lost record on a
   Worker that is otherwise healthy and may stay running for hours; the shutdown sweep is a backstop
   for the end of that Worker's life, not a retry policy. The retries are the **same** wake — every
   attempt derives the identical request ID (`wake-signal.md`) — so one that did in fact arrive is
   deduplicated server-side rather than producing a second empty Workflow Task.
2. **A forced replacement Workflow Task — only while a Workflow Task is open.** A Run being shut down
   that still holds a task finalizes its marker and completes the task requesting a replacement.
3. **An unparked wake Signal — when no Workflow Task is open.** A replacement is requested *on the
   completion of an existing task*, and there is none, so the runtime sends the reserved wake Signal
   itself and waits for the server to acknowledge it before the watchers go away.

Mechanisms (2) and (3) are runtime-owned state transitions, not something the consuming Workflow
requests (ADR-009), and both are offers to the task queue rather than promises by the shutting-down
Worker: any eligible Worker may pick the task up and reconstruct the subscription from the marker,
and if none is available the task times out and the server retries it.

**A marker is never written without its terminal.** If the boundary cannot be finalized the Workflow
Task is abandoned and retried rather than committing a partial annotation as a best effort: an
abandoned task commits no cursor and loses no record, while a truncated annotation is durable and
wrong (ADR-008).

## Shutdown and eviction are two transitions

| Run state at shutdown/eviction | Marker | Server-visible replacement |
|---|---|---|
| Workflow Task open — retained by the wait set, or open with an unfinished activation | Core issues `FinalizeExternalStreams{SHUTDOWN}` and writes the marker from the finalized annotation | Same completion sets `force_new_wft = true` |
| No open Workflow Task — the window after a command-producing or rolled-over completion | **None.** Nothing is accumulated; see the invariant below | Python's manager sends the reserved wake Signal for each active subscription and awaits acknowledgement, then tears the watcher down |
| No open Workflow Task and the wait set is parked | None | None needed. A confirmed park generation exists, so the producer's wake path applies unchanged |

Core and Python must classify a Run into the same row — Core when it decides whether to finalize,
Python's sweep when it decides whether a wake is owed — or a Run is handled by both mechanisms or by
neither.

### The invariant that makes the first row safe

> An accumulated, unwritten annotation exists only while a Workflow Task is open.

Deltas arrive only on activation completions, activations exist only under a Workflow Task, and every
completion path writes the accumulated annotation as exactly one marker and clears it. So a
Core-decided boundary either has a Workflow Task to finalize against or has nothing to write; there
is no third state in which Core holds a partial annotation with no way to complete it.

### Sequencing: teardown cannot precede finalization

Eviction is produced only once no activation is outstanding, so a `FinalizeExternalStreams` issued
before eviction is always answered before `RemoveFromCache`; Python's per-Run teardown is driven by
`RemoveFromCache` rather than by the shutdown signal; and pending evictions count as pending work, so
shutdown does not finish underneath them. Two consequences constrain the implementation:

- The marker is written by the **finalization completion**, never by the eviction completion. An
  eviction completion reports nothing, so a marker attached there would be silently dropped.
- If finalization cannot be answered, Python fails the activation rather than returning a partial
  terminal. Core writes **no** marker and the task is retried from the previous one. Nothing consumed
  during the abandoned Workflow Task was committed, so the cost is one repeated Workflow Task.

### The no-open-WFT transition

Nothing local can create server-visible work here, so the wake Signal does it. The manager runs this
as an explicit shutdown sweep rather than leaving it to watchers, because watchers only fire on an
append and the point is to hand the Run over whether or not one arrives. An idle cached Run receives
no eviction activation at shutdown at all, so the sweep cannot be folded into the eviction path.

**The sweep is in two halves, at two points in shutdown**, because the moment the answers still exist
is not the moment the wakes may be sent:

1. **Before Core's shutdown is initiated**, the manager calls `external_stream_run_status(run_id)`
   once for every Run with active subscriptions and **records** the answer — a **read-only** probe,
   answered on the same serialized local-input lane as readiness, returning
   `WftOpen | Parked | NoOpenWorkflowTask | RunNotFound`. It is deliberately not the readiness call:
   readiness means "a record is buffered", and probing with it would be a false claim that
   manufactures a spurious activation on the way out.
2. **After the pollers have stopped and every activation has been answered**, the manager acts on the
   recorded answers. `WftOpen` — the finalization is already answered and the first table row
   applies, so no wake is owed. `Parked` — nothing to do. `NoOpenWorkflowTask` or `RunNotFound` —
   send the wake Signal and await the server's acknowledgement. A Run the first half never reached is
   probed again here, on whatever Core has left to say.
3. Teardown of a Run happens only after step 2 resolves for it, or after the graceful-shutdown grace
   period expires.

Neither half can move to where the other is. Core's workflow-state lane ends when shutdown is
initiated, so every later probe answers `RunNotFound` — which owes a wake exactly as
`NoOpenWorkflowTask` does, so a late-probing sweep still sends its wake and still looks correct while
the two answers that mean *do not send one* have silently stopped being reachable. And a wake sent
before the pollers stop offers the Run to a task queue this Worker will answer itself. What the split
gives up is the guarantee that no Run acquires a Workflow Task after the probe — costing at most one
extra empty Workflow Task, against an ordering that would buy the guarantee with an answer describing
nothing.

A wake still unacknowledged when the grace period expires is counted on a distinct
`temporal_external_stream_shutdown_wake_failed` metric and shutdown proceeds. Retries within the period reuse
the derived request ID and deduplicate server-side; past it, the Run falls back to the durability
boundary below rather than blocking shutdown or pretending the wake happened.

**Every subscription the sweep does not resolve is counted, not only the wakes it attempted.** The
sweep starts by treating each subscription as unaccounted for and resolves it exactly once: when its
Run's status says nothing is owed, or when a wake has been attempted and its result recorded. Whatever
is left when the sweep stops is counted on the same metric. Three cases reach that point and all three
are silent otherwise:

- the grace period expiring **inside** a hanging wake send, which cancels the sweep where it stands;
- every subscription **after** that one in the serial loop, which is never visited;
- a Run whose **status probe could not answer**, which the sweep deliberately sends no wake for — it
  might have an open Workflow Task — but which may equally be holding a buffered record with nowhere
  to announce it. "We could not tell" is not "nothing was owed."

Accounting only where the sweep reached reported `shutdown_wake_failures == 0` for a Worker that had
just abandoned every one of its handoffs, which is exactly the silence the metric exists to break: a
dropped wake looks identical to a producer with nothing to say.

The same metric counts a **cleanup handoff** the exit could not wait out. A stale intent retired by
the final owed-removal pass owes an unparked wake for the record it silenced, and that pass runs after
the sweep, so its handoff is as much a part of this shutdown as a wake the sweep sent itself — and as
silent if abandoned. What the grace period cuts off there is counted, not dropped.

Three cases are deliberately *not* counted, because a metric that fires on a clean shutdown is not
alertable. A Run reported `Parked` or `WftOpen` owes nothing — the first is woken by a producer's
append through the ordinary path, the second is Core's to finish. A manager with **no status probe
wired** has no sweep at all: the mechanism is defined in terms of what Core answers, so there is no
obligation it could have failed to discharge. And a subscription the **live path removed** while the
sweep ran has had its handoff made: readiness answered `RunNotFound` owes a wake, sends it, and drops
the subscription only afterwards — a likely answer during shutdown, with watchers running for the whole
grace window and awaits inside the sweep for them to interleave with. The sweep then reaches that Run,
finds no subscriptions, and moves on; counting what it skipped there reports a loss that did not
happen.

## The durability boundary, stated honestly

Mechanisms (1) and (3) are **not durable**. Both depend on the consuming Worker being alive — (1) with
the Run cached and a watcher running, (3) for as long as the shutdown grace period lasts. Once the
Workflow is parked and the Run is evicted or the Worker restarts, no watcher exists, and a producer
that crashed between its append and its wake Signal leaves the Workflow parked with data available
and nothing to wake it. Nothing in the consumer can repair that, because the consumer is not running.

This design does **not** claim to solve that case implicitly. One of the following must be chosen per
deployment, and the choice is explicit:

- **Durable producer (default expectation).** `publish()` is complete only when its wake step has been
  acknowledged. Producers that are themselves durable — an Activity, which Temporal retries — satisfy
  this for free. A plain external process must retry until acknowledged, and the API surfaces the
  un-acknowledged state rather than hiding it.
- **Backend outbox.** The provider durably records the pending wake and a relay delivers it. This
  removes the requirement on the producer at the cost of a component that must itself be operated.
  Out of scope; future work.
- **Consumer-side sweep.** A periodic Workflow Timer rechecks parked subscriptions. This bounds the
  stall by the sweep interval instead of eliminating it, and costs History events.

## Multiple subscriptions to one stream

Cursors are per **subscription**, not per stream. Two subscriptions in one Workflow to the same stream
name have independent wait IDs, cursors, park intents keyed by `(stream key, wait_id)`,
`wait_generation`s, and entries in the annotation header and the Continue-As-New continuation state.
Delivery is **broadcast**: each sees every record from its own cursor, and work-sharing between two
subscriptions inside one Workflow is not supported (ADR-021).

Cancelling a subscription commits its cursor at the next marker and removes it from the wait set.
Re-subscribing to the same stream name creates a *new* subscription with a new wait ID rather than
resuming the cancelled one. Resumption across Continue-As-New is by wait ID.

## What a failure costs

**Marker recording commits the cursor. Reading or delivering a record does not.** So a backend read or
coordination failure fails the current Workflow Task for normal Temporal retry, and a Worker crash
discards uncommitted reads and replays from the last committed marker — in both cases the cost is a
repeated Workflow Task and no record is lost. Spurious wakeups and empty Workflow Tasks are permitted
correctness-wise, which is what makes stale readiness and stale wake Signals safe to ignore rather
than something to resolve exactly. Backend unavailability, retention loss, and integrity violations
must surface distinctly — see `failure-taxonomy.md`.

### A wake can race a terminal command

A wake Signal may enter History while the Workflow Task is issuing its terminal command. The server
can reject that task with `UnhandledCommand` so the external event is not lost, then schedule replay.
That retry is a normal Temporal ordering outcome: the Workflow may execute again and still complete
with the same stream observations. Tests for terminal wake races therefore allow
`UnhandledCommand` specifically, reject every other Workflow Task failure cause, and compare the
observations of every execution rather than requiring an exact workflow-body start count.
