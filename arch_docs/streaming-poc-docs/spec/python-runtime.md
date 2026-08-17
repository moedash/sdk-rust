# Python runtime: the out-of-sandbox manager

How stream I/O happens without ever touching the Workflow thread.

Owned by P8 (manager), P9 (Workflow API), P11 (`_apply` branch), P19 (async job partition), P17
(registry). Line anchors are in `code-anchors.md`.

## The constraint

Workflow activations cannot perform backend I/O. This is a hard property of the existing Python
Worker, not a stylistic preference:

- `_WorkflowInstanceImpl.activate()` is **synchronous**, so `_apply` and everything it calls are
  synchronous.
- Activations run on a thread-pool executor under `asyncio.wait_for` with a deadlock timeout of
  **2 seconds**; exceeding it raises `_DeadlockError` and fails the Workflow Task.
- The Workflow event loop is a custom deterministic loop; awaiting real network I/O inside it is not
  possible even where the code is nominally `async`.

**Any design in which `_apply` reads Redis is wrong by construction, not merely slow.**

## The protocol

The subscription manager lives outside the sandbox, on the Worker, and owns every backend connection
and watcher task on the Worker's own asyncio loop. The Workflow instance never touches a socket. The
two communicate through a bounded, thread-safe buffer per subscription:

| Step | Where | What |
|---|---|---|
| Register | Workflow thread, `subscribe()` | Registers `wait_id → (stream key, backend name, cursor)` with the manager. Non-blocking. |
| Prefetch | Manager loop | Reads ahead from the cursor into the subscription's bounded buffer. Never calls Workflow code. |
| Readiness | Manager loop | Reports readiness to Core **only after** a data or control record is buffered — never on a bare socket event. This is what makes the subsequent activation guaranteed non-blocking. |
| Drain | Workflow thread, `_apply` | Pops from the buffer only, at most `MAX_RECORDS_PER_ACTIVATION` records. Never performs I/O, so it completes in bounded time. |
| Advance | Workflow thread | Records the delivery in the observation delta; the manager advances the prefetch cursor from the committed marker, not from delivery. |

## Three cursors, not one

The word "cursor" covers three different positions in this manager, and conflating them is how
speculative reads become durable claims:

| Cursor | Owner | Advances on | Survives eviction |
|---|---|---|---|
| `committed_cursor` | the marker | marker commit only | yes — it is reconstructed from History |
| `delivery_cursor` | the Workflow instance | a record being handed to Workflow code | no — rebuilt by replay from `committed_cursor` |
| `prefetch_cursor` | the manager | a record being read into the buffer | no — discarded outright |

`prefetch_cursor` is **speculative**. It may run arbitrarily far ahead of both others, bounded only
by the buffer, and it claims nothing durable: reading a record is not consuming it, and consuming it
is not committing it. On eviction, Workflow Task failure, or Worker restart, every buffer and both
non-committed cursors are discarded, and the subscription restarts from `committed_cursor` — which
is exactly why "no cursor advances unless the marker commits" is safe to state.

The manager may only reposition `prefetch_cursor` backwards to `committed_cursor`, never forwards
past what a marker has committed, so a speculative read can never be mistaken for progress.

## Rules the implementation must not quietly violate

- **Readiness means "buffered", not "available".** A readiness notification for an unbuffered record
  would produce an activation whose drain must block, reintroducing the deadlock hazard.
- **Backpressure is the buffer bound.** A full buffer stops prefetch; it never drops records and
  never blocks the Workflow thread. What it bounds is memory held ahead of delivery, not how much
  one activation delivers: the watcher refills from the Worker's loop while the Workflow thread
  drains, so against a producer that keeps the buffer non-empty the buffer bounds nothing at all.
- **Delivery within one activation is bounded by a record count, never by elapsed time.**
  `MAX_RECORDS_PER_ACTIVATION` is 256 records handed to Workflow code per activation; the segment
  that reaches it ends with `BATCH_LIMIT` (`annotation-format.md`). A time-based bound would cut the
  segment at a nondeterministic point, and because segment boundaries are recorded in the
  annotation, replay would divide the same records differently and diverge from the live run. When
  the budget is exhausted the subscription blocks **even though records are still buffered**, and
  that block is what ends the activation — without it the drain never finishes, because the watcher
  refills concurrently, and the Workflow Task fails on the 2-second deadlock timeout on every
  attempt. Exhausting the budget therefore **re-arms readiness for the records still buffered**:
  otherwise the Workflow waits forever on data already in front of it, since `prefetch_cursor` is
  already past those records and no further notification is coming. The budget does not apply during
  replay, where delivery comes from the recorded segments, which already fix how many records each
  activation received.
- **Backend latency is invisible to the Workflow thread**, so a backend slower than the 2-second
  deadlock timeout delays readiness rather than failing an activation. A test covers exactly this.
- **Cancellation** of a subscription drains and discards its buffer, cancels its watcher, and
  deregisters the wait ID.
- **Eviction and shutdown**: `RemoveFromCache` and Worker shutdown both tear down every subscription
  for the Run, cancel watchers, and close buffers. Manager state is keyed by run ID so a stale Run
  cannot leak connections; the teardown path is the same one that must run when a Workflow Task
  fails mid-batch.
- **Replay uses the same buffer**, filled by the replay reader from recorded offsets instead of by a
  live watcher. `_apply` cannot tell the difference, which is the point.
- **Watchers survive Workflow Task completion.** They are torn down only on subscription
  cancellation, Run eviction, or Worker shutdown.

Sandbox passthrough: the manager module is registered in
`temporalio/worker/workflow_sandbox/_restrictions.py`, and only an opaque handle crosses into
Workflow code.

## Runtime-only jobs are handled before `activate()`

Keeping live reads out of the Workflow thread is not sufficient, because two of the stream
activation jobs are *themselves* backend operations:

| Job | Work it requires | Safe in `_apply`? |
|---|---|---|
| `ResolveExternalStreamWaits` | pop already-buffered records | **yes** — bounded, no I/O |
| `ReplayExternalStreams` | inclusive range reads plus integrity validation over the whole recorded range | **no** |
| `PrepareExternalStreamPark` | install intents, await the backend, recheck every stream | **no** |
| `FinalizeExternalStreams` | encode the terminal from manager state; no backend read required | no I/O, but no user code either |

Routing all four through the `job.HasField(...)` chain would put multi-second backend transactions
inside a synchronous `activate()` running under a 2-second deadlock timeout — failing the Workflow
Task for a healthy backend, and getting worse the more records replay must validate (ADR-011).

The dispatch therefore splits in `_handle_activation`, which is already `async` and already performs
pre-activation await work (`decode_activation` is awaited there before `workflow.activate` is handed
to the executor):

1. **`PrepareExternalStreamPark`** is handled entirely in `_handle_activation`. The manager installs
   the intents, awaits the backend transaction, rechecks, and the worker synthesizes
   `ExternalStreamParkResult` without calling `activate()` at all. No user Workflow code runs. If the
   recheck finds records, the completion is `StreamSetBecameReady` and Core issues a normal resolve
   activation next.
2. **`FinalizeExternalStreams`** is handled in the same place but performs **no backend work**: the
   manager reads each active subscription's current cursor boundary from its own state, encodes the
   terminal, and the worker synthesizes `ExternalStreamFinalized`. It is here rather than in `_apply`
   because it must run no user code and must be answered from out-of-sandbox state — not because it
   blocks.
3. **`ReplayExternalStreams`** is *prepared*, not handled: `_handle_activation` awaits the manager
   filling and validating every recorded range into the per-subscription buffers, then passes the job
   through to `activate()`. `_apply` performs deterministic delivery from memory only, exactly as it
   does for a live resolve.
4. **`ResolveExternalStreamWaits`** passes straight through. Readiness already means "buffered", so
   the buffer is populated before the job exists.

Failures in steps 1–3 propagate through the defined activation-failure path — a transient backend
error becomes `WORKFLOW_TASK_FAILED_CAUSE_EXTERNAL_STORAGE_FAILURE` with a retryable error type, an
integrity violation becomes `StreamIntegrityError` — rather than surfacing as a deadlock timeout,
which would misattribute a storage problem to the Workflow's own code.

This is a named deliverable (P19), not an implementation detail of the manager: the partition lives
in the worker's async layer and is the only thing standing between a slow backend and a spurious
`_DeadlockError`.

## Finalization is manager-state-only

`FinalizeExternalStreams` reads the manager's in-memory blocked cursor snapshot — one
`BEGINNING | AFTER(offset)` boundary per active subscription — encodes it as the annotation's
terminal, and returns. **It calls no provider method** (ADR-010), so it is an asynchronous control
operation and not a backend transaction. Three consequences:

- **There is no transaction to race.** The boundary is not "wherever the stream is now"; it is where
  this Workflow Task's deliveries stopped, which is fixed the moment the last activation of that task
  returned. Refreshing it against the backend would be actively wrong — it could name a position
  replay must not reproduce.
- **Watchers keep running during finalization**, exactly as they do during park preparation. A record
  arriving mid-finalization changes nothing about the terminal: it belongs to the next Workflow Task
  and reaches Core through the normal readiness path or, if none is open by then, through the wake
  Signal.
- **The only failure mode is missing state** — the Run's manager entry is gone or unreadable. There
  is no transient class here, because there is nothing to be transiently unavailable. Python fails
  the activation, Core writes no marker, and the Workflow Task is retried.

A test asserts the choice rather than trusting it: a provider that raises on every method is
registered, a finalization is driven, and the marker must still be written — proving no provider call
happens on this path from any layer.

## Wait ID assignment

`wait_id` is allocated by the Python SDK from a per-Run counter starting at 1, incremented in
`subscribe()` call order. Because Workflow code is deterministic, subscription creation order is
reproducible on replay, so the same subscription receives the same `wait_id`. A `wait_id` is stable
for the life of its subscription; its `wait_generation` increments each time that wait re-enters the
blocked state.

This makes subscription reordering a nondeterminism hazard — see `annotation-format.md`.

## Quiescence detection

Detect quiescence after `_run_once` returns. It already drains `self._ready` until empty, so "no
coroutine is runnable" is its post-condition; quiescence detection is a **registry check after it
returns**, not an event-loop change.

Python reports quiescence only after no Workflow coroutine is runnable. An idle stream cannot start
the Core timer while another stream is still driving Workflow code.

## Python SDK work items

- Add `temporalio/contrib/external_workflow_streams/` — the public consumer API, the distinct
  producer API, and the backend-provider interface, plus a Redis Streams provider as the initial
  implementation. Do not reuse any name from `temporalio/contrib/workflow_streams/` (ADR-001).
- Add an `external_stream_backends` Worker option holding named provider instances, constructed
  outside the sandbox and referenced from Workflow code by name only.
- Add the per-worker subscription/watcher manager outside the sandbox; register its module for
  sandbox passthrough.
- Coalesce watcher readiness and probe every active stream on each resume or parked wakeup.
- Add the Worker shutdown sweep: per-Run teardown is driven by `RemoveFromCache`, never by the
  shutdown hook, so finalization is always answered first; every Run still holding active
  subscriptions when no Workflow Task is open gets an unparked wake Signal, acknowledged before
  teardown, within the graceful-shutdown grace period. An idle cached Run receives no eviction
  activation at shutdown, so the sweep cannot be folded into the eviction path.
- Extend the bridge (`temporalio/bridge/src/worker.rs`, `temporalio/bridge/worker.py`), surfacing
  both result enums to Python.
- Partition stream activation jobs in `_handle_activation`; handle only `ResolveExternalStreamWaits`
  and the pre-filled `ReplayExternalStreams` in the `_apply` dispatch chain.
- Regenerate Python protos with `scripts/gen_protos.py`. Any new message carrying a `Payload` also
  requires re-running `scripts/gen_payload_visitor.py`.
