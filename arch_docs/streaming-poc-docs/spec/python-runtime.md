# Python runtime: the out-of-sandbox manager

How stream I/O happens without ever touching the Workflow thread, and which component owns which
position in a stream.

## The constraint that shapes everything else

Workflow activations cannot perform backend I/O. This is a property of the existing Python Worker,
not a stylistic preference: `_WorkflowInstanceImpl.activate()` is synchronous, runs on a thread-pool
executor under a **2-second deadlock timeout**, and drives a custom deterministic event loop in
which real network I/O cannot be awaited at all. **Any design in which `_apply` reads the backend is
wrong by construction, not merely slow.**

Everything below follows from it. The subscription manager lives outside the sandbox on the Worker's
own asyncio loop and owns every backend connection and watcher task; the Workflow thread only pops
from a bounded per-subscription buffer; readiness is reported to Core **only after** a record is
buffered, which is what makes the activation it produces guaranteed non-blocking; and the activation
jobs that need backend work are answered before `activate()` is ever called. A backend slower than
the deadlock timeout therefore delays readiness rather than failing an activation.

Only an opaque handle crosses the sandbox boundary — the manager module is registered for sandbox
passthrough, and no provider instance is reachable from Workflow code, which names a backend
registered on the Worker and nothing else.

## Four positions, one commit

"Cursor" covers four different positions, owned by four different components, and conflating any two
of them is how a speculative read becomes a durable claim:

| Position | Owner | Advances on | Survives eviction | What reads it |
|---|---|---|---|---|
| committed | the marker | a marker commit, and nothing else | yes — reconstructed from History | where replay starts |
| consumption | the runtime | a record handed to Workflow code | no | the Continue-As-New continuation |
| delivery | the runtime | a record drained out of the buffer | no | the observation delta, hence replay's recorded ranges |
| prefetch | the manager | a record read into the buffer | no — discarded outright | the watcher's next read |

The three uncommitted positions run ahead of `committed` in that order and claim nothing durable:
reading a record is not consuming it, and consuming it is not committing it. On eviction, Workflow
Task failure, or Worker restart, every buffer and all three are discarded and the subscription
restarts from `committed` — which is exactly what makes "no cursor advances unless the marker
commits" safe to state. The manager may reposition `prefetch` *backwards* to `committed`, never
forwards past what a marker has committed, so a speculative read can never be mistaken for progress.

`delivery` and `consumption` differ by whatever a drained batch left unread: a Workflow that stops
iterating part-way through a batch was delivered the whole batch and consumed only its prefix. The
annotation records `delivery`, because that is the schedule replay must reproduce; the
Continue-As-New continuation carries `consumption`, because the buffer holding the difference dies
with the Run and a successor resuming from `delivery` would step silently over records Workflow code
never saw.

## Delivery within one activation is bounded by a record count

One activation hands Workflow code at most `MAX_RECORDS_PER_ACTIVATION` records, and the
subscription then blocks **even though records are still buffered** — that block is what ends the
activation, because the watcher refills from the Worker's loop while the Workflow thread drains
(ADR-026). Without it the drain never finishes and the Workflow Task fails on the deadlock timeout
on every attempt, so the Workflow is stuck permanently rather than merely slowed. Three consequences
reach outside the runtime:

- The segment that reaches the cap ends with `BATCH_LIMIT` in the annotation, so replay divides the
  same records into the same activations (`annotation-format.md`). A time-based bound would cut the
  segment at a nondeterministic point and diverge from the live run.
- A completion the budget stopped must **re-arm readiness** for the records left buffered. The
  watcher moved `prefetch` past them when it buffered them, so no further notification is coming and
  the Workflow would otherwise wait forever on data already in front of it.
- Those waits are blocked but **not immediately parkable**. They belong in the quiescent snapshot,
  because that is what lets Core resolve them; declaring them parkable would ask Core to park a
  Workflow Task whose records have already reached the Worker.

The budget does not apply during replay, where delivery comes from the recorded segments, which
already fix how many records each activation received.

## Which side answers which activation job

Two of the four stream jobs are themselves backend operations, so the dispatch splits in
`_handle_activation` — already `async`, and already doing pre-activation await work — rather than
running through the synchronous `_apply` chain (ADR-011):

| Job | Answered | Why there |
|---|---|---|
| `ResolveExternalStreamWaits` | `_apply` | Readiness means "buffered", so the drain is a bounded buffer pop |
| `ReplayExternalStreams` | *prepared* in `_handle_activation`, delivered in `_apply` | Inclusive range reads plus integrity validation over the whole recorded range |
| `PrepareExternalStreamPark` | `_handle_activation` only | Installs intents and awaits the backend; runs no user Workflow code |
| `FinalizeExternalStreams` | `_handle_activation` only | No backend work at all (ADR-010), but it must run no user code and be answered from out-of-sandbox state |

Routing the last three through `_apply` would put multi-second backend transactions inside a
synchronous `activate()` under a 2-second timeout, failing the Workflow Task for a healthy backend
and getting worse the more records replay must validate. Answered where they are, a transient
backend error becomes `WORKFLOW_TASK_FAILED_CAUSE_EXTERNAL_STORAGE_FAILURE` and an integrity
violation becomes `StreamIntegrityError`, rather than surfacing as a deadlock timeout that
misattributes a storage problem to the Workflow's own code.

Watchers are torn down only on subscription cancellation, Run eviction, or Worker shutdown — never
by a Workflow Task completing. A watcher outliving the Workflow Task is what the first wakeup
mechanism in `wft-lifecycle.md` is made of.

## Wait ID assignment

`wait_id` comes from a per-Run counter starting at 1, incremented in `subscribe()` call order.
Workflow code is deterministic, so subscription creation order reproduces on replay and the same
subscription receives the same `wait_id`. It is stable for the life of its subscription and across a
Continue-As-New chain; its `wait_generation` increments each time that wait re-enters the blocked
state.

Everything else is keyed off it — the annotation header, the park intent, the continuation, and
Core's wait set — so inserting, removing, or reordering a `subscribe()` call renumbers every later
wait and is a nondeterminism hazard on the same footing as inserting a timer
(`annotation-format.md`).

## The runtime's half of shutdown

`wft-lifecycle.md` owns the shutdown sweep itself: what is probed, when, and which answers owe a
wake. Three obligations are the runtime's own, and they constrain this code rather than Core's:

- **Per-Run teardown is driven by `RemoveFromCache`, never by the shutdown hook.** The manager must
  keep a Run's subscriptions, buffers, and cursor state alive until that job arrives, or a
  `FinalizeExternalStreams` in flight finds nothing left to finalize from.
- **The sweep's two halves sit at two points in the Worker's own shutdown sequence** — the probe
  immediately before Core's shutdown is initiated, the wakes and teardown after every activation has
  been answered. Both live on the Worker's shutdown path rather than in the workflow worker's poll
  loop, because a clean shutdown never raises out of that loop and would otherwise leave every Run
  registered, its watchers running, and its owed wakes unsent.
- **An idle cached Run receives no eviction activation at shutdown**, so the sweep cannot be folded
  into the eviction path — and that Run is exactly the one that most needs it, with records buffered
  in a process about to exit and nothing else to tell the Workflow they arrived.

Both halves are bounded: the probe by a grace short enough that it cannot hold up stopping the
pollers, the wakes by the Worker's graceful-shutdown grace. A wake still unacknowledged when the
grace expires is counted on `external_stream_shutdown_wake_failed` and shutdown proceeds. It is
counted rather than only logged because a dropped wake is silent by nature: the Workflow simply
waits, and nothing distinguishes that from a producer having nothing to say.
