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

## Decoding is split across the boundary

"Pops from a bounded buffer" is only true if popping is the whole of getting a value, and
`DataConverter.decode` is not: it is external-payload retrieval, then the user's `PayloadCodec`,
then `from_payloads`. The first two are arbitrary asynchronous work — a network fetch, a KMS round
trip — and neither depends on the value's type. Only the third needs the topic's declared type,
and it is synchronous. The boundary falls between them:

| Half | Runs on | Steps | Needs the type |
|---|---|---|---|
| preparation | the Worker's asyncio loop | external retrieval, then the user's `PayloadCodec` | no |
| conversion | the Workflow thread | `from_payloads` with the topic's type | yes |

This is the split the Worker already applies to every other payload an activation carries:
`decode_activation` is awaited in `_handle_activation` before `activate()` reaches the executor.
Running the asynchronous half on the Workflow thread does not merely block — it **corrupts
History**, because the deterministic event loop turns a codec's `await` into a real Temporal timer
command, so History depends on the codec's internals and replays only while that codec behaves
identically. A codec that blocks rather than awaits fails against the sandbox's restrictions
instead, and either way arbitrary user I/O sits under the deadlock timeout (ADR-028).

Preparation happens at exactly two points, which are the only two ways a record reaches Workflow
code:

- **Live delivery** — in the watcher, before the record enters the subscription's buffer, and
  *before* the epoch check a reposition invalidates. Preparing awaits, so a reposition landing while
  a user's codec runs would otherwise slip past a check that had already passed and the retracted
  records would be buffered anyway.
- **Replay** — over every segment of the replay plan, while the replay job is prepared out of
  `activate()`. Replay's records never pass through a watcher, and a marker's segments are all
  delivered inside one activation, so this is both the only point at which they can be prepared and
  the last point before any of them is handed over.

**A preparation failure is carried on the record rather than raised.** The watcher has no Workflow
Task to fail and no Workflow to tell, and raising there would end the watcher for the whole Run,
taking every later record with it. The error travels with the record and is raised by the delivery
that would have yielded its value — the only point at which a Workflow exists to be told, and the
only point at which the record is known to have been wanted.

**A record that reaches the Workflow thread unprepared is refused, not converted**, whenever the
converter has a payload codec or external storage. The two cases are indistinguishable from the
bytes, because a codec's output is just another payload, so converting anyway yields a plausible
wrong value that nothing reports. A converter with neither has an empty asynchronous half and
converts correctly with no preparation at all, which is why the refusal keys on the converter rather
than on the delivery path. What it reports is a routing defect — some path delivered without
preparing — and not a user's converter mismatch.

Nothing Worker-side crosses the boundary to make this work. The Workflow-facing API asks its opaque
handle for a codec bound to the topic's type and gets back the synchronous half only; the manager
runs the asynchronous half with no type at all, because the type belongs to the topic and the topic
is Workflow code.

### Both halves carry the Workflow's serialization context

A `DataConverter`'s components may implement `WithSerializationContext`, which is how a codec derives
an encryption key from the Workflow it serves. The Worker already binds a
`WorkflowSerializationContext(namespace, workflow_id)` around every payload an activation carries, on
both sides of this same split — `decode_activation` on its own loop, the Workflow instance's
converters on the Workflow thread. A stream record is a payload like any other and must arrive the
same way. Unbound, a record delivered in the **same activation** as the Workflow's own argument would
be converted under no context while the argument was converted under one, and a per-Workflow key
would decrypt nothing. The failure is not loud either: it is carried on the record, raised at
delivery, and classified as row three of `failure-taxonomy.md`, whose operator instruction is to
align the consumer's converter with the producer's — against a deployment where the two already
agree and the SDK is what dropped the context.

Where the binding happens differs by holder, and follows from what each holder is:

- **The runtime is per Run**, so it is built with a converter that is already bound and hands
  Workflow code a codec carrying it. Bound by the Worker rather than inside the runtime, because the
  runtime crosses into the sandbox while `with_context` runs user code to clone the component
  converters — work that belongs on the Worker's side of the boundary, and that a per-Run handle
  need do only once.
- **The manager is per Worker** and prepares records for every Run at once, so a converter bound at
  construction would be bound to nothing in particular. It derives the context per record from that
  record's **own stream key**: the live path from the subscription's, replay from the annotation
  header's, which is the only place a replayed record's stream is written down while the Workflow
  has not yet run far enough to re-subscribe. The bound clones are memoized for the one preparation
  call; kept on the manager they would accumulate an entry per Workflow ID the Worker ever served.
- **The producer is per chain** and binds once, at construction (`backend-contract.md`).

All three key on `workflow_id`, never on any Run ID. A stream spans a Continue-As-New chain, so a
successor Run reads records its predecessor wrote; a Run-scoped key would make a chain's own records
unreadable from its first continuation onward.

The binding is `with_context`, not `_with_contexts`. The latter additionally names the Workflow as
the payload's **storer**, and a stream record is stored by its producer rather than by the consuming
Run — nothing on the consumer's decode path reads that context anyway, since retrieval resolves the
driver from the claim embedded in the payload. `with_context` returns the converter unchanged unless
some component implements `WithSerializationContext`, so a default converter is untouched and the
ordinary deployment sees no change at all.

**One store-context gap is open and stated rather than closed.** A producer that stores an
externally-stored payload does so with a store context naming no target, so a driver selector that
routes by target has nothing to route on. Retrieval is unaffected, for the reason above. Closing it
is a store-context question rather than a serialization-context one, and it needs a decision about
what target an Activity-hosted producer names — itself, or the Workflow the record is for — which
would change where blobs land.

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

### The reposition after a replay is synchronous

While replay hands Workflow code the records a marker recorded, the watcher has been independently
reading those same records from the subscription's start cursor into the live buffer — nothing out
there knows the marker exists. So the end of a replay moves `committed` to the marker's terminal and
retracts the buffer to it, or the first live drain hands every replayed record over a second time.
Observed end-to-end as a Workflow that received `['alpha', 'alpha', 'beta']`.

**That retraction has to have happened by the time the replay returns, not merely be scheduled.** The
drain that follows a replay is on the Workflow thread, and so is the replay itself; hopping the
retraction onto the manager's loop — as readiness re-arming legitimately does, because that only has
to happen eventually — orders it against nothing at all. One ordinary scheduler pass is enough for
Workflow code to drain first, and the fix then depends on the manager loop winning a race.

Two things make the synchronous path safe from the Workflow thread:

- **Only the watcher's wakeup is hopped.** Retracting touches the buffer, the three uncommitted
  cursors, and the prefetch epoch, all under the subscription's lock and none of them an `asyncio`
  primitive. The one asynchronous consequence — a watcher parked on backpressure now having room — is
  an `asyncio.Event`, which may not be set from another thread, so that alone is posted to the loop.
- **The epoch check moved inside the append**, under the same lock the retraction takes. A watcher
  compares the epoch it captured before its read against the one at append time, and with the
  retraction now landing on another thread a check made outside the lock could pass and *then* have
  the retraction land — putting the retracted records straight back into the buffer and re-advancing
  `prefetch` past them. Under the lock, an append is either cleared by the retraction or rejected by
  it, and there is no third interleaving.

`delivery` and `consumption` differ by whatever a drained batch left unread: a Workflow that stops
iterating part-way through a batch was delivered the whole batch and consumed only its prefix. The
annotation records `delivery`, because that is the schedule replay must reproduce; the
Continue-As-New continuation carries `consumption`, because the buffer holding the difference dies
with the Run and a successor resuming from `delivery` would step silently over records Workflow code
never saw.

## Delivery within one activation is bounded by a record count, and by annotation bytes

One activation hands Workflow code at most `MAX_RECORDS_PER_ACTIVATION` records, and the
subscription then blocks **even though records are still buffered** — that block is what ends the
activation, because the watcher refills from the Worker's loop while the Workflow thread drains
(ADR-026). Without it the drain never finishes and the Workflow Task fails on the deadlock timeout
on every attempt, so the Workflow is stuck permanently rather than merely slowed.

**The budget is charged where records move, and an activation begins already charged for what it
holds.** Both halves are what make the count a bound rather than a check, and each answers a
schedule that got past the other:

- *Charged at delivery*, the drain that moves a batch out of the manager's buffer and into a
  subscription's ready list — not at the consumption of each record afterwards. A drain that read the
  budget and charged nothing is a reservation nobody made: the next subscription's drain saw the same
  room and took it too, so two subscriptions consumed by two independent coroutines took a whole
  budget each and `n` of them took `n`. `merge()` hid it by filling one record at a time, which
  charges and checks in the same place. Delivery is also what the annotation records, so a count
  charged at consumption bounded a different quantity than the segment it exists to bound.
- *Pre-charged with the carry-over*, because a batch is delivered whole and consumed one record at a
  time. What a Workflow that stopped iterating part-way through leaves in its ready list is consumed
  by the *next* activation with no drain, and so with no check of any kind — free, and it accumulates:
  one subscription per activation can leave a nearly full list behind. Starting the count at what the
  ready lists already hold makes what an activation may hand over — carried-over plus newly
  delivered — one budget, whatever the schedule.

Three consequences reach outside the runtime:

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

A **second** budget sits beside it and the smaller of the two wins: how many more records this
Workflow Task's annotation can afford to record (`annotation-format.md`). It belongs here for the same
reason the record cap does — a record handed to Workflow code has to be recorded, a segment frame that
no longer fits cannot be moved to the next annotation, and there is no third option — so it is spent
before the record is delivered rather than discovered after the segment is built. Its three
consequences are the record cap's, with one difference each: the segment ends with `BUDGET_ROLLOVER`
rather than `BATCH_LIMIT`, and the same completion asks Core to end the Workflow Task rather than
merely the activation. Re-arming readiness and withholding immediate parkability are identical.

## Merging several subscriptions

`merge()` waits on several subscriptions at once and yields `(subscription, value)`. Each pass takes
**at most one record from each subscription, in `wait_id` order, resuming after the subscription
that last took one**, and each of those three is answering a different failure.

*In `wait_id` order*, because that is what reproduces. Records that arrived in one batch across two
streams have no inherent order between them, so an interleaving that depended on dict iteration, on
arrival time, or on which watcher ran first would replay differently than it ran. `wait_id` comes
from Workflow code's own `subscribe()` order, so replay reconstructs the same total order from the
code, and the pass then depends only on which waits have a record ready — which replay reconstructs
from the recorded segments.

*At most one record*, because the budget above covers the merged set as a whole. Draining one
subscription's ready list to the end lets the lowest `wait_id` spend the entire budget by itself,
which is a priority order rather than a merge.

*Resuming after the last take*, because the budget covering the set means a pass can be cut anywhere
inside it, and a pass that always restarted at the lowest `wait_id` is cut in the same place on
every activation. That is not a subtle bias. With more ready subscriptions than the budget has
records — 257 against 256 — the last one is never reached at all, and it is not merely served
nothing: filling stops on the spent budget before it consults the manager, so the Worker is never
asked whether that stream has anything, for the life of the Run. The general case needs only a count
that does not divide the budget: 100 always-ready waits against 256 records leaves the pass cut at
the 56th, the first 56 taking one record per activation more than the other 44, forever, with the
gap growing by one per activation and nothing bounding it. Rotating the start position is what makes
the skew between any two continuously ready streams what it is claimed to be — a single record
(ADR-034). A control record spends its subscription's turn like any other: it is consumed, because
it occupies an offset inside a run, and it advances the rotation.

**The rotation is not replay state.** It lives in the generator, is recorded nowhere, and is not
reconstructed — and under replay the budget does not apply, so passes are not cut where they were
cut live and the rotation reaches positions the live run never started from. What makes that safe is
that a replay drain serves from the front of the recorded segment and only while the front belongs
to the asking wait (`annotation-format.md`): a wait asked out of turn is told nothing and the record
stays for whoever asks next. Every active wait is still asked exactly once per pass, so the front's
owner is reached in every pass and the yielded sequence is the recorded global order whatever
position the pass started at. No change to *ask* order can reorder a replayed segment. That premise
is load-bearing rather than incidental: a variant that let a pass skip a wait — a per-subscription
budget, an early return once the budget is spent — would let a replayed segment come out in an order
the live run never delivered — fairness bought with nondeterminism, which is the one currency it
may not be paid for in.

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

## A subscription has one consumer

`__aiter__` hands out a new generator on every call, and they are not independent views: the cursor,
the readiness future, and the blocked flag belong to the subscription. The readiness future in
particular is a single slot held in two places at once — on the subscription, where closing finds it,
and in the runtime's pending map keyed by `wait_id`, where the readiness activation finds it.

So a second coroutine blocking on the same subscription is **refused**, on the single-subscription
path and inside `merge()` alike, with a non-retryable error raised on the Workflow thread. Without the
refusal the second waiter replaces the first in both places: readiness resolves only the newer future,
its cleanup removes the map entry, and the older one is left unreachable by the readiness activation
and by closing — a coroutine stranded for the life of the Run, while the shared blocked flag can
report that wait as not blocked at all, so Core is not even retaining a Workflow Task on its behalf.

The refusal is on the *waiter* rather than on the iterator, which is what keeps ordinary sequential
re-iteration working: breaking out of an `async for` leaves the generator suspended at its `yield`
rather than closed, so an iterator-level claim could not tell that shape apart from two live consumers
(ADR-037). Two consumers of one *stream* is a supported shape and the way to ask for it is a second
`subscribe()` — delivery is a broadcast, so each wait gets every record and keeps its own cursor.

## Closing a subscription

Closing runs on the Workflow thread, so it divides the same way everything else here does: the part
that is a synchronous change to in-memory state happens immediately, and the part that touches the
backend is handed to the manager.

The Workflow-side half ends the wait. It leaves the quiescent snapshot, iteration stops, and whatever
the buffer still holds is dropped **without being consumed**, so the consumption cursor stops short
of those records and a Continue-As-New successor receives them rather than stepping over them.
One property of the wait changes permanently: it can never block again. A closed wait that re-entered
the blocked set would be reported to Core, registered, and eventually retained or parked for on
behalf of a coroutine that no longer exists — a Workflow Task held open for nobody, which only a wake
would end.

The Worker-side half — stopping the watcher and taking back the wait's park intent
(`wft-lifecycle.md`) — is requested through the runtime handle, which hops it onto the manager's
loop, rather than started from the subscription itself. Scheduling Worker work from the Workflow
thread is not merely misplaced in the way ADR-011 describes: nothing runs it, and nothing reports that
nothing ran it, so the watcher goes on prefetching into a buffer no one will drain for the rest of the
Run, with every symptom of that remote from its cause. The hop is what makes the request take effect
at all.

**Closing does not forget the subscription** (ADR-029). Two things are built from the registered set
after an individual wait has ended, and both are wanted precisely for a stream the Workflow finished
reading early: the annotation binding for that wait, without which replay reads a `wait_id` no
binding covers and reports it as a wait the Workflow did not create, against code that never changed;
and the continuation cursor, without which a successor Run restarts that stream at `BEGINNING` and
re-delivers everything the closed subscription consumed. Keeping the entry is also what lets a
replayed Run satisfy the check that every wait the marker bound was recreated
(`annotation-format.md`): a closed wait was created, and a set the close had emptied would fail that
check against a Workflow that did nothing wrong. The manager's entry for the wait holds
resources and is dropped; the runtime's entry holds the record of what the wait was, and is marked
closed instead.

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
grace expires is counted on `temporal_external_stream_shutdown_wake_failed` and shutdown proceeds.
It is counted rather than only logged because a dropped wake is silent by nature: the Workflow
simply waits, and nothing distinguishes that from a producer having nothing to say.
