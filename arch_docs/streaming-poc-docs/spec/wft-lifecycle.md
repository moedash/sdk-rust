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

## Two independent questions at every activation return

1. **Did replay-visible stream state change?** If yes, a `WorkflowStreamProgress` command carries the
   observation delta, on *every* completion path. This is what commits the cursor boundary, and it is
   not conditional on why the Workflow Task ended: if a consumed record influences a command that
   lands in History while the consumption itself is never marked, replay re-delivers that record
   while the command it produced is already durable, and the divergence surfaces as an unrelated
   nondeterminism error much later (ADR-005).
2. **Is the Workflow blocked on streams?** If yes, a `WorkflowStreamQuiescent` command reports the
   complete quiescent snapshot. This registers the wait set with Core and — on the completions that
   can be held open — starts the idle timer.

A completion may carry either command, both, or neither. `WorkflowStreamProgress` never implies
retention, and `WorkflowStreamQuiescent` carries no annotation data (ADR-004).

| Condition at return | Python sends | Core does |
|---|---|---|
| No subscriptions, nothing observed | Normal completion | Reports the WFT to the server |
| Records consumed, no pending stream waits | `WorkflowStreamProgress` | Records the marker, reports the WFT |
| Records consumed **and** a terminal command (complete, fail, cancel, continue-as-new) | `WorkflowStreamProgress` **ordered before** the terminal command | Records the marker, then applies the terminal command |
| Stream waits pending **and** server-bound commands (timer, activity, child workflow, signal) | `WorkflowStreamProgress` ordered before those commands, then `WorkflowStreamQuiescent` with the same complete snapshot | Records the marker, **registers the waits, arms no timer**, reports the WFT |
| Stream waits pending, no other command, **nothing consumed** | `WorkflowStreamProgress` carrying the header on first observation plus an **empty segment**, then `WorkflowStreamQuiescent` | Accumulates the delta, registers the waits, retains the WFT, starts the idle timer |
| Stream waits pending, no other command, records consumed | `WorkflowStreamProgress`, then `WorkflowStreamQuiescent` with the **complete** set | Accumulates the delta, `ActivationCompleteOutcome::DoNothing`, retains the WFT, starts **one** idle timer for the set |

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
Run against `(stream key, wait_id)`, and retried by the next park, resolve, registration or eviction
of that Run, each of which drains what is owed under the Run's park lock before doing its own work. A
ledger entry says *a removal was decided on and has not been confirmed*, not *an intent exists* —
which is what makes draining it safe from any of those rather than only from the path that recorded
it. Removal is idempotent (`backend-contract.md`), so a drain that duplicates a removal that
succeeded costs a round trip, and the entry holds nothing that a teardown could invalidate: no
watcher, no buffer, no connection beyond the backend the removal has to go through. The entry is
made **before** the backend call, because a call that never comes back — the backend raised, the
task was cancelled mid-await — owes the removal exactly as an error does, and the subscription it
was reached through is usually dropped in the same breath. A fresh install at that key supersedes
it, since retrying a removal recorded against a generation that has since been overwritten would
take out the park now sitting behind it. Why the ledger is a Run's own state rather than a field on
the subscription is ADR-031.

**A drain removes only the intent it recorded.** It re-reads the intent and removes it only if the
`park_generation` and Run ID it carries both still match what was written down; anything else is
forgotten rather than removed. The stream key is stable across a Continue-As-New chain while
`wait_id` restarts at 1 in the successor, so an entry a predecessor Run left behind can name the key
of a park a **successor** is sitting in, and taking that out is strictly worse than the leak the
ledger exists to close: it unparks a Run whose park is real and whose producers have no reason to
send anything. The read narrows the window to a single round trip, and the Run's park lock closes it
for parks of that same Run; across Runs it is narrowed rather than closed, which would need a
compare-and-delete the provider contract does not require.

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
