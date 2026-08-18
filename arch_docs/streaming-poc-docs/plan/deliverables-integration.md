# Integration deliverables — need both tracks

**P7 — Bridge: expose the readiness call and the run-status probe** ⇢ C4
`temporalio/bridge/src/worker.rs` (copy the synchronous `record_activity_heartbeat` shape) +
`temporalio/bridge/worker.py`. Thread-safe and acknowledged, surfacing both result enums to
Python — the five-valued readiness result and the four-valued read-only
`external_stream_run_status`.
*Done when:* both calls are reachable from Python against a Core worker with no cached Run and
return `RunNotFound` rather than raising, and calling the readiness path from several threads is
safe under a concurrency test.

**P8 — Subscription/watcher manager** ⇢ P2, P7, P17
Per-worker, **outside the Workflow sandbox** — register the module for passthrough in
`worker/workflow_sandbox/_restrictions.py`. Owns backend connections and watcher tasks,
coalesces readiness, maps `wait_id → subscription`.
The Workflow-thread contract is the deliverable, not just the plumbing: `activate()` is
synchronous and runs on an executor thread under a **2-second** deadlock timeout, so `_apply`
must never perform I/O. The manager therefore prefetches into a **bounded, thread-safe buffer
per subscription** and reports readiness to Core only once a record is *buffered*, not merely
available. `_apply` pops from the buffer and nothing else. See `spec/python-runtime.md`.
Also owns: backpressure (a full buffer stops prefetch, never drops and never blocks),
cancellation, Run eviction, Worker shutdown, and stale-Run cleanup keyed by run ID.
Keeps three cursors distinct — `committed_cursor` (advances only on marker commit, reconstructed
from History), `delivery_cursor` (advances on hand-off to Workflow code), and a speculative
`prefetch_cursor` (advances on buffering, discarded on eviction). Prefetch may run ahead but
claims nothing durable; eviction restarts from `committed_cursor`.
The async handling of runtime-only jobs is **P19**, not this deliverable.
Watchers survive Workflow Task completion; they are torn down only on cancellation, eviction, or
shutdown, and on `Parked`/`NoOpenWorkflowTask`/`RunNotFound` they send the wake Signal themselves
— tearing the watcher down only in the `RunNotFound` case.
*Done when:* driven directly against the manager — no Workflow API and no Core activations,
neither of which is in this closure — readiness is reported only after a record is buffered; a
provider with an injected delay longer than the deadlock timeout delays that report instead of
blocking the caller; a full buffer stops prefetch without dropping or blocking; eviction discards
prefetch state and restarts from `committed_cursor`; and a provider that raises when called from
any thread other than the manager's loop is never called from one. The end-to-end form — the same
slow provider not failing a Workflow Task — is a P16a case.

**P9 — Workflow-facing API + sandbox wiring** ⇢ P1, P4, P17
`external_stream.topic(name, backend=...)`, `.subscribe()` async iterator,
`with_options(idle_timeout=...)`, in `temporalio.contrib.external_workflow_streams`.
`wait_id` assigned from a per-Run counter in `subscribe()` call order. Sandbox passthrough for
the manager handle. Cursors are per subscription: two subscriptions to one stream name are two
independent waits, each receiving every record from its own cursor (ADR-021).
No name may collide with `temporalio.contrib.workflow_streams` (ADR-001). **Not exported until
Milestone 1** (ADR-024).
*Done when:* a Workflow names a registered backend it never imports and gets a subscription
handle; `wait_id`s are assigned 1..n in `subscribe()` call order and reproduce across two runs of
the same Workflow code; two subscriptions to one stream name get distinct wait IDs; and no
exported name matches `__temporal_workflow_stream*`.

**P10a — Quiescence command emission** ⇢ C1, P9
After `_run_once` drains: emit `WorkflowStreamQuiescent` with the complete wait set and the
`min`-reduced effective idle timeout, only when stream waits are pending and no server-bound
command accompanies the completion (ADR-016). No annotation involved, so this is the part that
can run in the Milestone 0 spike.
*Done when:* a Workflow blocked on one subscription completes with `WorkflowStreamQuiescent`
carrying the complete wait set; the same Workflow with a pending timer emits none; and differing
configured idle timeouts reduce to their `min` in `wait_id` order.

**P10b — Observation delta emission** ⇢ C1, C14a, P5, P8, P9
Emit `WorkflowStreamProgress` whenever replay-visible stream state changed — on **every**
completion path, ordered before any command whose value depends on consumed data. The emission
rule is *not* "if anything was consumed": the first observation of a subscription, an activation
that observed nothing, and the boundary the activation returned on all produce deltas, because a
subscription to an empty stream must still record provider, stream key, and start cursor or
replay has no starting point (ADR-005).
*Done when:* a first subscription to an empty stream emits a delta carrying provider identity,
stream key, and explicit start cursor; an activation that drains nothing emits an empty segment; a
completion carrying server-bound commands emits its delta ordered before them; and Core accumulates
each into one annotation.

**P11 — Handle `ResolveExternalStreamWaits`** ⇢ C7, P8, P9, P10a
New branch in the `_apply` dispatch. Probe **every** active wait — the job's `ready_hints` are
hints, not an exhaustive availability claim — drain from the buffer, resolve futures. Performs no
I/O.
*Done when:* a readiness activation naming one wait probes every active wait; the Workflow resumes
from buffered records only; and `_apply` makes no provider call, verified by a provider that raises
if called from the Workflow thread.

**P19 — Async partition of runtime-only stream jobs** ⇢ P8, C1
`ReplayExternalStreams` needs recorded-range reads plus integrity validation, and
`PrepareExternalStreamPark` needs a backend transaction; both are I/O, and `activate()` is
synchronous under a 2-second deadlock timeout. Partition stream jobs in `_handle_activation`, which
is already async and already awaits `decode_activation` before handing the activation to the
executor (ADR-011):
- `PrepareExternalStreamPark` and `FinalizeExternalStreams` complete entirely there, synthesizing
  `ExternalStreamParkResult` / `ExternalStreamFinalized` without calling `activate()`.
- `ReplayExternalStreams` has its buffers filled and validated there, then passes through to
  `_apply` for deterministic in-memory delivery.
- Transient storage failure and integrity failure propagate through the defined activation-failure
  path, not as a `_DeadlockError`.
*Done when:* driven with **synthetic activations** — Core-issued park and replay jobs come from C8
and C10, which are outside this closure — a replay job and a park job that each take longer than the
deadlock timeout are answered without calling `activate()`, without a `_DeadlockError`, and without
provider code running on the Workflow thread; a finalization job is answered with a provider that
raises on every call, proving finalization performs no backend I/O from any layer (ADR-010); and a
provider error surfaces as an activation failure with the storage cause rather than a deadlock
timeout.

**P13 — Replay read path** ⇢ P4, P5, P8, P9, P11, P18, P19, C10
Fill the per-subscription buffers from the recorded annotation: inclusive
`[first_offset, last_offset]` reads, validated on presence of both endpoints, exact count, strictly
increasing order under the provider's comparator, and matching control positions. Deliver in
recorded order, one event-loop drain per recorded segment, so `wait_condition` predicates fire as
often as they did live (ADR-018). Classify failures through the four-way taxonomy (storage /
integrity / decode / nondeterminism) using P18's types.
*Done when:* replay performs no live waiting; deleting the first, middle, or last record of a range
each fails as integrity loss rather than substituting a later record; an intact-but-undecodable
record fails as a decode error; and an unreachable backend fails as transient storage failure.

**P14 — Producer wake-signal path** ⇢ C1, C11, P2b, P3b, P6
Append, then observe or lease-claim the park generation, then send the reserved Signal idempotently
through a raw `SignalWorkflowExecution` built with the protocol's own serialization — never the
user's `DataConverter` — with the request ID derived per `spec/wake-signal.md`. Claims are leased and
renewable so a producer crashing between claim and Signal does not strand the generation.
*Done when:* a producer that appends and finds a wakeable generation sends exactly one Signal that
Core accepts and turns into a resolve activation (C11's path, with the generation state injected —
the park handshake that would produce it live is C8, outside this closure); two producers retrying
one wake derive the identical request ID and the server deduplicates it; two unparked wakes from
different senders derive different ones; and an expired claim is taken over rather than stranding
the wake. The end-to-end "append after a confirmed park wakes the Workflow" is a P16a case.

**P15 — Continue-As-New cursor via reserved internal header** ⇢ P5, P9, P10b, C14b
Attach each subscription's committed continuation state to a reserved internal header on the
Continue-As-New command, persisted in the new Run's `WorkflowExecutionStarted` and restored as the
annotation header's `AFTER(offset)` start cursor before any subscription is established (ADR-022).
Depends on the observation delta being committed on the terminal-command path (C14b), not only on
header propagation: a Continue-As-New that drops its final segment restarts the new Run at a stale
cursor.
*Done when:* a chain restores its cursor from History with no live backend read, and two same-stream
subscriptions restore independently.

**P20 — Worker shutdown wake sweep** ⇢ C4, C11, C15b, P8, P9, P11, P14
Two obligations the Python manager owes at shutdown, neither of which Core can discharge:
- **Teardown ordering.** Per-Run teardown is driven by `RemoveFromCache`, never by the shutdown
  hook, so a `FinalizeExternalStreams` in flight is always answered before the manager's Run state
  disappears.
- **The sweep, in two halves at two points in shutdown.** *Ask first*: before Core's
  `initiate_shutdown()`, call C4's read-only `external_stream_run_status` once for every Run still
  holding active subscriptions and record the answer, bounded by a short probe grace period. *Act
  second*: once the pollers have stopped and every activation has been answered, `WftOpen` → C15b's
  first transition already applied, nothing owed; `Parked` → nothing to do;
  `NoOpenWorkflowTask`/`RunNotFound` → send the unparked wake Signal and await acknowledgement
  before tearing the subscription down. The probe is not the readiness call, which would assert a
  buffered record that does not exist. An idle cached Run gets no eviction activation at shutdown,
  so this cannot ride on the eviction path.
The split is forced, not a preference. Core's workflow-state lane ends at `initiate_shutdown()`
itself — `bump_stream()` pushes an input, `shutdown_done()` sees the shutdown token cancelled and an
idle cached Run with no pending work, and the stream returns `PollError::ShutDown` — so
`external_stream_run_status` falls through to `RunNotFound` from that point on, and a Run answers
`NoOpenWorkflowTask` one line before it and `RunNotFound` one line after. Probing after the pollers
stop therefore collapses all four answers into the one that still owes a wake, leaving the `Parked`
and `WftOpen` branches unreachable and the sweep looking correct while it wakes parked Runs and
races C15b. Moving the wakes up to join the probe is equally unavailable: teardown must stay driven
by `RemoveFromCache`, so a `FinalizeExternalStreams` in flight is answered first, and a wake offered
to a task queue this Worker is still polling is not a hand-off. The trade is explicit — the probe gives up the guarantee that no Run acquires a Workflow
Task after it, so a Run recorded `NoOpenWorkflowTask` may take one before the pollers stop and
receive both C15b's replacement task and the sweep's wake, costing one extra empty Workflow Task,
which the design permits.
An unacknowledged wake is retried within the grace period under the same request ID, then reported
through the `external_stream_shutdown_wake_failed` metric; shutdown is never blocked past the grace
period and the wake is never reported as delivered when it was not.
*Done when:* shutdown in the `NoOpenWorkflowTask` window sends an acknowledged unparked wake and the
server records a new Workflow Task for the Run, without waiting for an unrelated Workflow event; the
sweep's own probe answers `NoOpenWorkflowTask` there rather than `RunNotFound`, which is what shows
it ran early enough to distinguish anything; shutdown in the `WftOpen` state sends none and lets
C15b finish; teardown never precedes an outstanding finalization; and a wake that cannot be
acknowledged surfaces on the `external_stream_shutdown_wake_failed` metric instead of being dropped
silently. That a *second Worker* then reconstructs the subscription from the marker needs the replay
path (P13) and is a P16a case.

**P21 — Multiple streams, `merge`, and same-stream subscriptions** ⇢ C8, P2b, P3b, P5, P9, P11
Multi-stream coordination on the Python side: `merge`/`select` over several subscriptions as one wait
set, `min` reduction of differing idle timeouts in `wait_id` order, per-subscription cursors and park
intents for two subscriptions to one stream name, and the observed global delivery schedule recorded
as runs across streams.
*Done when:* one idle stream cannot park the Workflow Task while another is active; a fence on one
stream alone does not bypass the global idle timer while all-fenced streams do; two same-stream
subscriptions each receive every record from their own cursor and install distinct park intents,
verified by inspecting both in the backend; and an alternating two-stream batch encodes one run per
delivery. (P16b is the list that must *all* pass; these are this deliverable's own criteria.)

**P16a — Milestone 1 required-test list (55 cases)** ⇢ C8, C9, C10, C11, C12b, C14a, C14b, C15a, C15b, P2b, P3b, P5, P6, P6a, P6b, P10b, P13, P14, P18, P19, P20
The list in `tests-m1.md`. Its dependencies are every Milestone 1 deliverable, because a test list is
not runnable before the capabilities it exercises exist.
**This is where cross-deliverable assertions live** — every completion path producing a complete
marker, a second Worker reconstructing a subscription after shutdown in either Run state, a slow
provider not failing a Workflow Task — because it is the only deliverable that depends on all of
them.
*Done when:* all 55 cases in `tests-m1.md` pass in CI, and the count matches that file's stated count.

**P16b — Milestone 2 required-test list (12 cases)** ⇢ P15, P16a, P21
The list in `tests-m2.md`.
*Done when:* all 12 cases pass, and the two lists partition the 67 required cases exactly.
