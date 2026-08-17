# Workflow Task lifecycle

When a Workflow Task is retained, how it ends, and how a subscription is woken in each state it
can be left in.

Owned by C6, C7, C8, C12a, C12b, C13, C15b (Core) and P10a, P11, P20 (Python).

## Live consumption

When `subscribe()` has demand during an open Workflow Task, the SDK runtime reads from the last
committed offset and resumes the Workflow for each data record. It continues draining while
records are available, across as many activations as that takes: one activation delivers at most
`MAX_RECORDS_PER_ACTIVATION` records and then blocks so the activation can end, even where more
records are already buffered (`python-runtime.md`).

The runtime parks the subscription and allows the Workflow Task to complete when either:

1. every active subscription has reached a write fence with no subsequent record immediately
   available; or
2. no record arrives on any active subscription before the configured idle timeout.

The idle timeout covers producers that crash, omit `finish_writing()`, or do not use the SDK.

## The idle timeout is a Workflow-Task policy

It applies to the **complete set** of subscriptions the Workflow is blocked on, not to each
subscription independently: one idle stream cannot park the Workflow Task while another is still
delivering records. A later record does not violate a fence; it simply wakes the Workflow and
consumption resumes.

Because the timeout is a property of the set, subscriptions in one quiescent set configured with
different idle timeouts must reduce to one value. **The reduction is `min`** (ADR-016), applied
over the quiescent set in `wait_id` order, then clamped below the rollover deadline. The inputs
are the configured values of the quiescent set and nothing else, so the result is deterministic
and reproduces on replay.

Default: one second, set through `with_options`. A non-positive or absent timeout is a
configuration error rejected at `with_options` time, not silently coerced; Core independently
rejects a `WorkflowStreamQuiescent` whose `idle_timeout` is non-positive as a malformed
completion.

## Workflow Task rollover is mandatory

A retained Workflow Task is bounded by the server's Workflow Task timeout. A continuously fed
stream whose inter-record gaps stay below the idle timeout never reaches the idle parking path, so
without rollover the retained task simply times out — the Workflow Task is not merely held too
long, it *fails*. The runtime therefore always runs a rollover deadline derived from the Workflow
Task timeout, independent of the idle timeout, and completes the current task early when rollover
wins. Every active subscription, cursor, annotation delta, and readiness generation survives
rollover. The idle timeout is clamped below the rollover deadline so rollover stays authoritative.

**Rollover bounds the Workflow Task, not an activation.** Core decides it between activations and
cannot interrupt one in progress, so it is no protection against a single activation that never
returns. What bounds an activation is the per-activation record budget in `python-runtime.md`.

Rollover also bounds a second effect of holding a Workflow Task open: while the task is retained,
the server cannot start another one, so Signals, Updates, and non-legacy Queries queue until the
task completes. This is the same property outstanding local activities already have, but a stream
can hold a task open far longer. Retention latency for those inputs is therefore bounded by the
rollover deadline and nothing else, and callers who need lower latency must lower the Workflow Task
timeout rather than the idle timeout.

Rollover needs a timer facility that does not depend on the local-activity request sink — see
ADR-017 and C13.

## Two independent questions at every activation return

1. **Did replay-visible stream state change?** If yes, a `WorkflowStreamProgress` command carries
   the observation delta, on *every* completion path. This is what commits the cursor boundary.
2. **Should the Workflow Task be retained?** If yes, a `WorkflowStreamQuiescent` command asks Core
   to hold it open. This is what starts the idle timer.

A completion may carry either command, both, or neither. `WorkflowStreamProgress` never implies
retention, and `WorkflowStreamQuiescent` carries no annotation data (ADR-004).

| Condition at return | Python sends | Core does |
|---|---|---|
| No subscriptions, nothing observed | Normal completion | Reports the WFT to the server |
| Records consumed, no pending stream waits | `WorkflowStreamProgress{observation_delta}` | Records the marker, reports the WFT |
| Records consumed **and** a terminal command (complete, fail, cancel, continue-as-new) | `WorkflowStreamProgress` **ordered before** the terminal command | Records the marker, then applies the terminal command |
| Stream waits pending **and** server-bound commands (timer, activity, child workflow, signal) | `WorkflowStreamProgress` ordered before those commands; no `WorkflowStreamQuiescent` | Records the marker, completes the WFT normally; subscriptions stay registered |
| Stream waits pending, no other command, **nothing consumed** | `WorkflowStreamProgress` carrying the header on first observation plus an **empty segment**, then `WorkflowStreamQuiescent` | Accumulates the delta, retains the WFT, starts the idle timer |
| Stream waits pending, no other command, records consumed | `WorkflowStreamProgress`, then `WorkflowStreamQuiescent{quiescence_generation, waits[], idle_timeout}` with the **complete** set | Accumulates the delta, `ActivationCompleteOutcome::DoNothing`, retains the WFT, starts **one** idle timer for the set |

**Command ordering is normative**: `WorkflowStreamProgress` precedes every command whose value
could depend on consumed data. On replay this guarantees that integrity validation for a record
runs before the command derived from it is matched.

While a Workflow Task is retained, successive deltas accumulate in
`ExternalWaitSet.replay_annotation` and are written as one marker when the task finally completes
— whether that completion comes from parking, rollover, or a later activation that produces
commands. **Core never writes two markers for one Workflow Task.**

## Progress is committed on every completion path

Consuming a record and committing that consumption are separate steps, and the second is not
conditional on *why* the Workflow Task ended. The failure this rules out is specific and silent:
if a consumed record influences a command that lands in History while the consumption itself is
never marked, replay re-reads that record from the last committed cursor and delivers it again,
while the command it produced is already durable. The result is a divergence that surfaces as an
unrelated nondeterminism error much later. See ADR-005.

## Race-free parking

Parking is all-or-nothing across the complete set of subscriptions the Workflow is blocked on, and
uses a generation-based handshake coordinated through the backend:

1. Core marks every wait `Parking` and sends `PrepareExternalStreamPark`. This is runtime-internal;
   no user Workflow code runs.
2. Python installs a park intent per **subscription**, keyed `(stream key, wait_id)`, containing
   its cursor boundary and the park generation.
3. After all intents are installed, it rechecks every stream.
4. All still empty → `ParkSetConfirmed`, carrying the terminal `final_observation_delta`. Core
   appends it, records the marker, and completes the WFT.
5. Any stream ready → Python removes every installed intent and returns `StreamSetBecameReady`.
   Core aborts that parking generation and issues `ResolveExternalStreamWaits`.

A producer first appends its record, then observes or claims the current park generation and sends
a lightweight Temporal Signal. This ordering closes the empty-check/completion race: an append is
either seen by the active consumer or paired with a wake Signal.

Readiness accepted before `ParkSetConfirmed` wins; the confirmation for that generation is then
stale. If the rollover deadline beats the idle timer, Core finalizes through
`FinalizeExternalStreams{ROLLOVER}`, records one marker, completes with `force_new_wft = true`, and
the subscriptions resume on the replacement Workflow Task.

A provider may use an atomic backend transaction, but it is not required if per-stream intent
installation plus the final all-stream recheck closes every append race. Core treats the result as
one atomic state transition.

## Three generations, named separately

| Name | Scope | Increments when | Where it appears |
|---|---|---|---|
| `wait_generation` | one subscription | that subscription re-enters the blocked state | the wait entry in the quiescence report and in readiness notifications |
| `quiescence_generation` | one complete blocked snapshot | the Workflow becomes quiescent again after any resumption | the quiescence report, the park preparation job, and the park result |
| `park_generation` | one confirmed park of a set | a park set is confirmed | the backend park intent and the wake Signal |

`park_generation` takes the value of the `quiescence_generation` that was parked, rather than being
a fourth independent counter. A wake Signal carries `(wait_id, park_generation)`, and those two
values are the only generation state that reaches the backend or the wire; `wait_generation` never
leaves the Core/lang boundary.

A Signal naming a generation the current Run does not recognize is harmless: the runtime rechecks
every active subscription on wakeup regardless, so a stale Signal costs at most one empty Workflow
Task, which this design permits. A wake sent when **no** park generation exists carries the reserved
value `park_generation = 0` and is a recheck request, not a stale Signal (ADR-023).

A wake Signal names one stream, but it is only a hint: on wakeup the runtime rechecks every active
subscription.

## Completions that leave subscriptions active but unparked

Parking is not the only way a Workflow Task can end with subscriptions still waiting. A completion
carrying server-bound commands, and a rollover, both end the task without a park handshake. In that
state there is no retained task to notify locally and no park generation for a producer to observe,
so a later append has nothing to wake.

Three mechanisms cover it, and one of them always applies:

1. **The consumer's own watchers.** Watchers stay live while the Run is cached. A watcher that
   observes an append when no Workflow Task is open sends the wake Signal itself. This is the
   normal path, and it also covers a producer that crashed after appending.
2. **A forced replacement Workflow Task — only while a Workflow Task is open.** When the runtime is
   shutting a Run down and that Run still holds a Workflow Task, it finalizes the marker and
   completes the task requesting a replacement.
3. **An unparked wake Signal — when no Workflow Task is open.** Forcing a replacement is not
   available there, because a replacement is requested *on the completion of an existing task* and
   there is none. The runtime instead sends the reserved wake Signal itself and waits for the
   server to acknowledge it before the watchers go away.

Mechanisms (2) and (3) are **runtime-owned state transitions, not something the consuming Workflow
requests** (ADR-009). Both are, deliberately, offers to the task queue rather than promises by the
shutting-down Worker: any eligible Worker may pick the task up and reconstruct the subscription
from the marker. If none is available, the task times out and the server retries it, which is
ordinary Worker-shutdown behavior.

**A marker is never written without its terminal.** Whichever mechanism applies, the runtime does
not commit a partial replay annotation to History as a best effort. If the boundary cannot be
finalized, the Workflow Task is abandoned and retried instead — an abandoned task commits no cursor
and loses no record, while a truncated annotation is durable and wrong (ADR-008).

## Shutdown and eviction are two transitions

| Run state at shutdown/eviction | Marker | Server-visible replacement |
|---|---|---|
| Workflow Task open — retained by the wait set, or open with an unfinished activation | Core issues `FinalizeExternalStreams{SHUTDOWN}` and writes the marker from the finalized annotation | Same completion sets `force_new_wft = true` |
| No open Workflow Task — the window after a command-producing or rolled-over completion | **None.** Nothing is accumulated; see the invariant below | Python's manager sends the reserved wake Signal for each active subscription and awaits acknowledgement, then tears the watcher down |
| No open Workflow Task and the wait set is parked | None | None needed. A confirmed park generation exists, so the producer's wake path applies unchanged |

### The invariant that makes the first row safe

> An accumulated, unwritten annotation exists only while a Workflow Task is open.

Deltas arrive only on activation completions, activations exist only under a Workflow Task, and
every Workflow Task completion path writes the accumulated annotation as exactly one marker and
clears it. So a Core-decided boundary either has a Workflow Task to finalize against, or has
nothing to write. There is no third state in which Core holds a partial annotation with no way to
complete it. Core asserts the invariant when it clears `ExternalWaitSet.replay_annotation`.

### Sequencing: teardown cannot precede finalization

The ordering falls out of structure Core already has (see `code-anchors.md`):

1. `_check_more_activations` returns early while an activation is outstanding, and the eviction
   activation is produced in its final branch. A `FinalizeExternalStreams` activation issued before
   eviction is therefore always completed before `RemoveFromCache` is issued.
2. Python's per-Run teardown is driven by `RemoveFromCache`, not by the shutdown signal itself. The
   manager must keep a Run's subscriptions, buffers, and cursor state alive until that job arrives.
3. `shutdown_done` requires every Run to have no pending work, and with
   `ignore_evicts_on_shutdown = false` — the Core default, which Python does not override — pending
   evictions and their replies count as pending work.

Two consequences that constrain the implementation:

- The marker is written by the **finalization completion**, never by the eviction completion. An
  eviction completion may carry no commands and reports nothing, so a marker attached there would
  be silently dropped.
- If finalization cannot be answered, Python fails the activation rather than returning a partial
  terminal. Core writes **no** marker, the Workflow Task fails and is retried by the server, and
  the replacement attempt replays from the previous marker. Nothing consumed during the abandoned
  Workflow Task was committed, so no record is lost and no cursor moves; the cost is one repeated
  Workflow Task.

### The no-open-WFT transition

Nothing local can create server-visible work here, so the wake Signal does it. The manager runs
this as an explicit shutdown sweep rather than leaving it to watchers, because watchers only fire
on an append and the point is to hand the Run to another Worker whether or not one arrives:

1. Worker shutdown begins. Core stops polling for new Workflow Tasks, so the Run cannot acquire a
   Workflow Task after this point on this Worker.
2. For every Run with active subscriptions, the manager calls `external_stream_run_status(run_id)`
   once per Run — a **read-only** probe answered on the same serialized local-input lane as
   readiness, returning `WftOpen | Parked | NoOpenWorkflowTask | RunNotFound`. It is a separate
   call from `notify_external_stream_ready` on purpose: readiness means "a record is buffered", and
   using it as a probe would be a false claim that manufactures a spurious activation on the way
   out.
3. `WftOpen` — Core is handling it; wait for the resulting activation and the first table row
   applies. `Parked` — nothing to do. `NoOpenWorkflowTask` or `RunNotFound` — send the wake Signal
   and await the server's acknowledgement before tearing the subscription down.
4. Teardown happens only after step 3 resolves for that Run, or after the Worker's graceful-shutdown
   grace period expires.

An idle cached Run receives no eviction activation at shutdown at all — `shutdown_done` treats a Run
with no pending work as finished — so the sweep cannot be folded into the eviction path.

**Failure policy for an unacknowledged shutdown wake.** The Signal is retried within the grace
period using the same derived request ID, so retries deduplicate server-side. If it is still
unacknowledged when the grace period expires, the Worker logs and increments a distinct
`external_stream_shutdown_wake_failed` metric, then completes shutdown; it does not block shutdown
indefinitely and it does not pretend the wake happened. The Run then falls back to the durability
boundary below.

Python's obligations on shutdown are therefore: answer finalization while a Workflow Task is open,
sweep the no-open-WFT Runs, and only then tear down watchers, buffers, and backend connections.

## The durability boundary, stated honestly

Mechanisms (1) and (3) are **not durable**. Both depend on the consuming Worker being alive — (1)
with the Run cached and a watcher running, (3) for as long as the shutdown grace period lasts. Once
the Workflow is parked and the Run is evicted or the Worker restarts, no watcher exists, and a
producer that crashed between its append and its wake Signal leaves the Workflow parked with data
available and nothing to wake it. Nothing in the consumer can repair that, because the consumer is
not running.

This design does **not** claim to solve that case implicitly. One of the following must be chosen
per deployment, and the choice is explicit:

- **Durable producer (default expectation).** `publish()` is documented as complete only when its
  wake step has been acknowledged. Producers that are themselves durable — an Activity, which is
  retried by Temporal — satisfy this for free. A plain external process must retry `publish()`
  until acknowledged, and the API surfaces the un-acknowledged state rather than hiding it (P6b).
- **Backend outbox.** The provider durably records the pending wake and a relay delivers it. This
  removes the requirement on the producer at the cost of a component that must itself be operated.
  Out of scope; future work.
- **Consumer-side sweep.** A periodic Workflow Timer rechecks parked subscriptions. This bounds the
  stall by the sweep interval instead of eliminating it, and costs History events.

## Multiple subscriptions to one stream

Cursors are per **subscription**, not per stream. Two subscriptions in one Workflow to the same
stream name have independent wait IDs, independent cursors, independent park intents keyed by
`(stream key, wait_id)`, independent `wait_generation`s, and independent entries in the annotation
header and the Continue-As-New continuation state.

Delivery is **broadcast**: each subscription sees every record from its own cursor. Work-sharing
between two subscriptions inside one Workflow is not supported (ADR-021).

Cancelling a subscription commits its cursor at the next marker and removes it from the wait set.
Re-subscribing to the same stream name creates a *new* subscription with a new wait ID; it does not
resume the cancelled one. Resumption across Continue-As-New is by wait ID.

## Failure semantics

- Backend read or coordination failure fails the current Workflow Task so normal Temporal retry can
  reattempt it; the cursor is not advanced until the task's marker is committed.
- **Marker recording commits the cursor. Reading or delivering a record does not.**
- A producer crash before a fence is handled by the idle timeout. A crash after append but before
  wakeup is handled by the consumer's own watcher while the Run is cached, and otherwise by the
  chosen durability mechanism.
- Stale readiness notifications are ignored by wait and quiescence generation.
- Spurious wakeups and empty Workflow Tasks are allowed correctness-wise, though implementations
  should suppress them.
- A worker crash discards uncommitted reads. Replay re-reads from the last committed marker.
- Backend unavailability, retention loss, and integrity violations must surface distinctly — see
  `failure-taxonomy.md`.
