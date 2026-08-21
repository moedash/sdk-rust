# Replay annotation format

The opaque bytes Python encodes, Core stores in a marker, and Python decodes on replay. Core never
parses it.

## The invariant

Workflow code is deterministic given two things, and only these two need to be reproduced:

1. The identical ordered sequence of stream-future resolutions delivered to Workflow code.
2. The identical set of points at which the runtime returned control with no further stream data
   available.

Everything the annotation records exists to reproduce exactly that. A detail that affects neither
need not be recorded; a detail that affects either must be.

The second condition is not redundant. Under `_single_batch_activation`, each stream activation
drives exactly one `_run_once(check_conditions=True)` drain, and stream jobs land in the non-query,
non-signal job set, so `workflow.wait_condition` predicates are evaluated once per activation.
Reproducing only the record order while changing how many drains occurred would change when
conditions fire.

## Activation segmentation

A retained Workflow Task may span many `ResolveExternalStreamWaits` activations but produces one
marker. Replay therefore must not collapse them (ADR-018).

Core stays annotation-blind, so segmentation is reproduced inside Python rather than by issuing
multiple activations. The annotation is divided into **segments**, one per original activation. On
replay, Core issues a single `ReplayExternalStreams` job, and Python's replay driver walks the
segments in order, performing one event-loop drain per segment. The live run's *k* activations
become *k* drains inside one replay activation, so coroutine scheduling and condition evaluation
match.

*k*, and not *k + 1*. **The replay driver is not the only thing that drains.** The job lands in the
non-query job set, so the activation runs one `_run_once` of its own for that set once every job in it
has been applied — which happens whatever the driver did. So the driver drains the first *k - 1*
segments and arms the last, leaving that drain to the activation. This also mirrors the live run,
where each activation's single trailing drain served the records that activation had just been handed.

Closing the replay therefore moves to after that drain: the check that nothing recorded was left
undelivered, and the cursor reposition onto the marker's committed boundary, both belong after the
final segment has actually been delivered.

**A failing activation abandons the replay rather than closing it.** Leaving replay mode is
unconditional — a Run stuck in it has every later drain return nothing at all — but the checks and the
cursor move are skipped when an error is already propagating. The consumed check raises whenever a
recorded delivery is still armed, and after a failed drain one always is, since the drain that would
have taken it is the one that failed; an exception raised while another propagates **replaces** it, so
closing from a bare `finally` reported every failing replay activation as a nondeterminism error
blaming a `subscribe()` call nobody touched. That loses the diagnosis and the classification with it:
a `FailureError` out of Workflow code fails the Workflow, while a nondeterminism error fails the
Workflow Task and is retried. The cursor move has to be skipped in any case — an activation that
failed committed nothing.

**A marker with no segments at all defers nothing, and closes before that drain instead.** A Workflow
Task that bound a wait and blocked with the stream never delivering writes a header and a terminal and
no segment. There is then no recorded segment for the activation's drain to serve, which makes that
drain a *live* one: records that arrived while the Run was evicted are already in the buffer and it
would hand them over. Repositioning after it would retract exactly what it had just delivered — cursor
back to the marker's boundary, buffer cleared — and the watcher would re-read and re-deliver records
Workflow code already had. Closing first retracts the buffer *before* the drain, so the drain finds
nothing and the watcher re-reads from the committed boundary; the records arrive on the activation that
re-read announces.

A segment's delivery list is the recorded **global** order across waits, not one list per wait: it
is the order the runtime handed records over, which is the order Workflow code took them in. The
segmentation reproduces how many drains an activation ran; the order within a segment reproduces
which wait each of those drains served.

This is safe with respect to Workflow time: all segments of a marker belong to one Workflow Task,
so `workflow.now()` is constant across them in both the live run and replay. Commands produced
during a retained Workflow Task are buffered until the task completes, in both directions.

## Schema

The annotation is a versioned binary encoding. `schema_version` leads, so markers written by older
SDK versions stay readable.

```text
annotation := header, (bindings | segment)*, terminal

header   := schema_version, streams[]     // wait_id -> binding
bindings := streams[]                     // the same, for waits bound after the header
binding  := (stream_key, start_cursor, backend_name, provider_id, provider_format_version)

segment := run*, segment_end_reason
run     := (wait_id, first_offset, last_offset, count, control_positions)
segment_end_reason := NO_DATA_AVAILABLE | BATCH_LIMIT | FENCE_REACHED | BUDGET_ROLLOVER

terminal := blocked_snapshot[]            // wait_id -> cursor boundary: BEGINNING | AFTER(offset)
```

### Bindings are per wait

A binding names the Worker-registered backend the Workflow's own `topic(backend=...)` chose, and
carries the provider identity of whatever is registered under that name. One label for the whole
annotation cannot say which of two registered backends owns a given wait, and two instances of one
provider — two Redis clusters, two key prefixes — declare the same provider id, so a label does not
distinguish those either: replay would read a wait's recorded range out of a store that never held
it.

Which half of a binding disagrees decides how the disagreement is reported. `stream_key` and
`backend_name` are Workflow code's choice, so a mismatch is row four of `failure-taxonomy.md` —
nondeterminism, fixed by versioning the Workflow. `provider_id` and `provider_format_version`
describe whatever the Worker registered under that name, so a mismatch there is a deployment
problem, with the Workflow unchanged and the backend undamaged.

### A wait bound after the header

`subscribe()` may run at any activation of a retained Workflow Task, so a wait can be created after
the header frame has already gone to Core. Core appends observation deltas and never rewrites what
it already holds, so a header cannot be extended in place — that wait's binding rides its own
**bindings frame** instead (ADR-027), emitted with the delta of the activation that registered the
wait and therefore ahead of both the segment that first records a run for it and the terminal.
Without it the wait reaches the marker as runs and a terminal entry with no stream key, no backend,
and no start cursor, and replay of *unchanged* code fails as a wait "the Workflow did not create".

Decoding merges every bindings frame into the table the header opens, so what replay reads is one
flat `wait_id -> binding` table and it never has to ask when a wait joined.

A wait is bound **exactly once** per annotation. A second binding for the same `wait_id` is a decode
failure rather than a value replay picks between: whichever of the two stream keys it chose could be
the one the records were not written to.

A late wait carries **its own** start cursor, not wherever the waits already in the header have
reached. Recording another wait's position for it names a boundary this subscription never stood at
— and for a wait that receives nothing, the start cursor is the whole of what replay knows about it.

### Runs

- A `run` is a maximal set of **consecutive deliveries from one stream**, in observed global order.
  Alternating streams produce one run per delivery; a single-stream batch of 100,000 records
  produces one run. This is why the schedule is encoded as runs rather than as individual
  deliveries.
- **Both endpoints are recorded, not a start plus a count** (ADR-006). Backend offsets are ordered
  but not dense, so `(first_offset, count)` does not determine where a run ends.
- The sequence of runs across all segments *is* the global cross-stream schedule. Per-stream ranges
  alone are insufficient, because concurrent Workflow coroutines could otherwise observe a different
  order.
- Control records occupy offsets and advance the cursor but are never yielded to Workflow code.
  `control_positions` is a **sparse** list of the relative indices within the run at which a control
  record occurred — not one kind tag per record. Fences are rare by construction, so this keeps a
  run's control encoding proportional to the number of fences.
- **No field in a run is per-record.** A run costs two offsets, a count, and a sparse control list
  whether it covers ten records or a hundred thousand.

### Segments and the empty cases

`segment*` and `run*`, not `+`, in both cases deliberately:

- **An empty segment is meaningful**: an activation that drained and found nothing still ran one
  `_run_once`, and replay must reproduce that drain or `wait_condition` predicates fire a different
  number of times.
- **An annotation with no segments at all is meaningful too**: it is the first subscription to an
  empty stream, where the header and terminal carry everything replay needs.

### Positions

- Every position in the annotation is a **cursor boundary**, `BEGINNING` or `AFTER(offset)`. Run
  endpoints are record offsets; blocked snapshots are boundaries. The two are not interchangeable
  and the encoding keeps them distinct types.
- `start_cursor` in a wait's binding makes its initial position explicit rather than re-derived.
- `ParkReason` is **not** in the terminal. It lives once, in the Core-readable
  `ExternalStreamMarkerData.terminal_boundary` (ADR-008).

## Replay validation

Replay reads the inclusive range `[first_offset, last_offset]` for each run and verifies:

1. both endpoints are present,
2. the range contains exactly `count` records,
3. ordering is strictly increasing under the provider's comparator,
4. `control_positions` match.

A first, middle, or last deletion each fails a different one of those checks, and all four are
cheap. **These four checks are the whole of replay validation.** They are sufficient precisely
because every provider guarantees the bytes cannot change (ADR-003), so the only damage replay has
to detect is a record that is no longer present.

A provider that cannot support this verification over a compact range must encode the exact offset
sequence in place of a compact run.

Replay then follows the marker's recorded availability/blocking decisions rather than consulting
current stream timing.

### What replay checks about the Workflow's own code

Those four are about the bytes. Three more are about the code that is replaying, and all three
report row four of `failure-taxonomy.md` — nondeterminism — rather than integrity loss, because the
recorded ranges are exactly where they were written and it is the Workflow that moved.

1. **Each recorded wait's binding**, when that wait is registered, and again when a marker is
   replayed onto subscriptions that already exist. The comparison is the stream *name* and the
   backend name, which are the two halves of a binding that Workflow code chose.
2. **Every recorded delivery was taken.** A replay that reaches the end of the last segment still
   holding one is running code that consumed less than History says it consumed.
3. **Every bound wait was recreated**, checked once after the last segment as `bound ⊆ registered`
   and reported naming the waits that are missing.

The third does not follow from the other two, and what it catches is the ordinary shape rather than
an exotic one. A binding is written for a subscription whether or not anything was ever delivered
through it — the first observation has to carry provider identity, stream key, and start cursor even
for a stream that stayed quiet for the whole Workflow Task — so a `subscribe()` on a quiet stream
produces exactly a binding with no runs behind it, which every check that reasons from deliveries is
blind to. Two removals escape without it. Removing the **last** `subscribe()` on a quiet stream
renumbers nothing and leaves nothing undelivered, so the replay is accepted although the Workflow
now holds one subscription fewer than History says it did. Removing a **middle** `subscribe()` where
the later waits name the same stream and backend is worse: every survivor renumbers down by one, the
binding comparison therefore compares equal for all of them, and the records recorded for wait *k*
are consumed by what was subscription *k+1* — a different cursor, a different consumer, and nothing
left over for the delivery check to find.

**The check is one-directional deliberately** (ADR-033). `bound ⊆ registered` is the invariant;
`registered ⊆ bound` is not, because replay runs the Workflow forward past the Workflow Task the
marker covers and the subscriptions it makes there belong to the *next* marker's header. For the
same reason it is checked once at the end of replay rather than before each segment: a wait bound by
a later frame legitimately does not exist yet while an earlier segment is being delivered.

## Byte budget

The encoder enforces a hard budget, `MAX_ANNOTATION_BYTES`, chosen well below the server's event
size limit and declared as a constant rather than a guideline. It is enforced at encode time, not
checked afterwards (ADR-007):

1. When the accumulated annotation for the current Workflow Task passes a high-water fraction of
   the budget, Python sets `request_rollover` on its next `WorkflowStreamProgress`.
2. Core rolls the task over as it does on deadline expiry, **minus the finalization round trip**:
   the triggering `WorkflowStreamProgress` came from Python and already carries the terminal, so
   Core writes the marker and completes with `force_new_wft = true` without issuing
   `FinalizeExternalStreams`. This is the one Core-initiated completion that needs no finalization
   job, and it needs none because Python decided the boundary.
3. The next Workflow Task starts a fresh annotation whose header records the current cursors, so
   consumption continues uninterrupted across the split.
4. The segment that triggered it ends with `BUDGET_ROLLOVER`, which is how replay knows the batch
   continues in the following marker.

An annotation can therefore never exceed the budget: the runtime rolls the Workflow Task over
instead of growing the marker. Replay reassembles multi-marker batches in Workflow Task order,
matching each marker to its Workflow Task rather than concatenating blindly.

### The high-water mark alone is not a bound

A high-water mark is a *fraction* of the budget, and it becomes true only once a frame that crossed it
has been emitted. Frames are indivisible, and three of the four have no length this side chooses: a
binding carries the namespace, Workflow ID, first-execution Run ID, stream name, backend name, and
provider id; a run carries two provider-supplied offset strings. Any of them can be larger than the
slack a fractional mark leaves, and the batch a frame records cannot be moved to the next annotation —
its deliveries happened in *this* Workflow Task, and that task's marker is where replay has to find
them. So "roll over at 75%" is a policy, not a guarantee, and three further rules are what make the
guarantee hold for a frame of any size.

**The closing frames are reserved, not checked.** The terminal, and a bindings frame for any wait
registered since the header went out, are priced in advance and only they may spend that reservation.
Segments are refused before it is gone. Neither may ever be refused, because both record something
that already happened — and an annotation Core writes with no terminal is durable and cannot be
decoded past the frame after it.

**Delivery stops before a segment it could not record.** The annotation budget is a second delivery
budget alongside the per-activation record cap, and the smaller one wins. It is spent *before* a
record is handed to Workflow code — the only point at which stopping is still an option — priced per
record by the largest run this annotation has actually encoded, floored so that the first record of an
annotation is costed pessimistically rather than optimistically. The measurement belongs to the
**annotation, not the segment**: the open segment is emptied at the end of every activation, so a
per-segment maximum re-applies the bare floor to the first record of *every* activation, and a real
run costs two provider-chosen offset strings. Delivering on that price hands over a record the closing
segment then cannot record. The activation then ends with
`BUDGET_ROLLOVER` and the same completion asks Core to roll over. Records the annotation budget left
buffered have their readiness re-reported exactly as the record cap's do.

**Stopping and asking are one mechanism, and either alone wedges the Workflow.** Stopping bounds the
marker; the rollover is what gives the next Workflow Task a fresh annotation to deliver into. Stop
without asking and the next activation begins against the same full annotation, is handed a delivery
budget of zero, delivers nothing — and therefore *observes* nothing. So the condition may not be
phrased in terms of what this activation observed: "the annotation can afford no more" is a property
of the annotation, and one written as a property of the activation stops being true the moment the
completion path takes the delta, which is what clears the observed flag.

**The rollover request rides the completion that crossed the line.** It is read *after* the
activation's segment is closed, not before. Read before, the flag describes the annotation as it stood
one activation ago: the segment that crossed the high-water mark went out with
`request_rollover = false`, and the following activation was free to add another frame before Core had
ever been asked to close anything. One activation of delay is the entire margin the mark exists to
provide.

**A subscription set whose header cannot fit is refused at `subscribe()`.** A rollover writes a
*fresh* header, so an oversized header is not something rollover can fix — every annotation would be
the same size, and the Workflow Task would fail identically on every retry with no marker ever
written. The capacity question is therefore asked where the answer is still "do not make this
subscription": inside the Workflow's own `subscribe()` call, priced against an empty annotation
(header plus terminal, no segment), deterministic and so reproduced under replay.

A price is not a proof, though: nothing has seen the offsets of the record it is pricing. Two things
close that gap.

**A margin absorbs a misprice and turns it into a rollover.** Behind the closing reserve sits a spill
margin that a segment frame — and only a segment frame — may overrun into. Affordability is measured
against the line *before* the margin, so a mispriced record lands in it rather than past the budget,
the same completion asks Core to end the Workflow Task, and the terminal is still reserved behind it.
The margin also fixes the floor `subscribe()` checks: a fresh annotation is guaranteed room for its
header, its terminal, one segment frame, and the margin, which is what makes at least one record
affordable in every annotation. Without that guarantee a Workflow can deliver nothing, observe
nothing, and therefore never even ask for the rollover that was supposed to save it.

**A run larger than the margin is a capacity limit, reported as one.** A provider whose offsets are
far longer than anything measured can make a single run cost more than the whole budget has left, and
no rollover helps — the next annotation has to carry the same run. That boundary is refused where the
record has been drained but not yet handed to Workflow code, as the same non-retryable
`ExternalStreamCapacityError` that `subscribe()` raises, naming the provider's offsets. It fails the
Workflow, not the Workflow Task, so the encoding that cannot fit is not retried forever.

`AnnotationBudgetExceeded` remains behind all of that as the encoder's own last line, and it is a
**non-retryable application failure** for the same reason: the server retries Workflow Task failures
regardless of cause, so a plain exception there is the permanent retry loop ADR-007 exists to rule
out.

### What actually drives marker growth

One honest worst case remains: an alternating multi-stream workload, `A,B,A,B,…`, has a schedule
transition per record and cannot be compressed by range encoding. It produces one run per delivery,
hits the high-water mark sooner, and rolls over more often. Bounded marker size is bought with
additional Workflow Tasks.

That is the *only* driver of marker growth, so the budget is not expected to fire at all in the
single-stream scope of Milestone 1. It is still enforced at encode time rather than assumed, and
tests assert **encoded byte size** rather than run count — a run-count assertion tells you nothing
about what a future per-record field would cost.

## Delta accumulation

`WorkflowStreamProgress.observation_delta` carries the frames produced since the previous progress
report for the same Workflow Task — the header on the first of them, then whichever bindings and
segments that activation produced. Core appends each delta to `ExternalWaitSet.replay_annotation`
and writes the accumulated result as the marker's `replay_annotation`. Core never parses either.

## Replay read path

Replay distinguishes three kinds of waiting:

| Kind | On replay |
|---|---|
| Waiting for *new* records — watchers, idle timer, park handshake | Never occurs. Core starts no timers and Python starts no watchers. |
| Reading *recorded* offsets | Occurs. Ordinary blocking backend I/O, bounded by the provider's read timeout. |
| Blocking on backend latency or unavailability | Not a determinism failure — a transient backend failure. |

Python reads the full recorded offset range for each stream up front, in as few provider calls as
the range allows, then delivers from memory in recorded order. Replay I/O cost is therefore a
function of the consumed range, not of segment count.

**A replay drain takes from the front of the segment, and only while the front belongs to the wait
that is asking.** The segment is the global order, so a drain that searched past another wait's
record would hand its asker a record that came *after* it live: a segment recorded as (wait 2,
wait 1) replays as (wait 1, wait 2) for any reader that asks in `wait_id` order, and a wait
appearing twice with another between them collapses two of the segment's drains into one. A drain
that meets another wait's record answers empty, which is exactly what the live drain answered —
the record was not in that wait's buffer yet — and it leaves the record where the drain that
recorded it will find it.

## Cursor origin

A cursor is never derived from mutable backend state, on any Run.

- **First execution of a chain:** the subscription starts at the `BEGINNING` boundary, recorded as
  the `start_cursor` of its binding in the subscription's **first** observation delta — which is
  emitted whether or not a record was ever delivered. That is the header frame for a wait the
  annotation opened with, and a bindings frame for a subscription created after the header went
  out.
- **Subsequent Runs:** the committed continuation state arrives in the reserved Continue-As-New
  header as an `AFTER(offset)` boundary, persisted in `WorkflowExecutionStarted` and restored before
  any subscription is established (ADR-022).

Both paths populate the same binding field, so replay reads an explicit starting boundary in every
case, including the case where the stream was empty for the subscription's entire life.

### The continuation header carries the whole binding

Keyed by `wait_id`, and carrying per wait what a binding above carries: the cursor, the stream name,
the backend name, and the provider identity — `provider_id` and `provider_format_version`. The cursor
alone cannot say *what it is a position in*, and an offset means nothing outside the store that
produced it, so a successor that resumed on the wait number alone would hand a predecessor's offset
to a store that never held those records — which the backend accepts, and which skips everything
before that boundary silently.

Marker replay makes the same comparison for every recorded range it reads. **A successor Run's first
live read happens before it has written a marker that could make it**, which is the whole reason the
continuation carries the binding rather than leaving it to replay (ADR-039).

Which half disagrees decides how it is reported, and it is the same split the binding table uses:

| Disagreement | Reported as |
|---|---|
| Stream name, or backend name | Row four of `failure-taxonomy.md` — Workflow code moved |
| `provider_id`, or `provider_format_version` | Row one — a deployment mapped the name elsewhere |

Both are raised where the subscription is registered, before the cursor reaches the manager, so no
backend ever reads at a boundary that was not produced against it.

**A recorded format version is compared by presence, not by truthiness.** Nothing in the provider
contract reserves zero, so it is a version a provider may declare and the encoding represents it
exactly; treating it as a "nothing was recorded" sentinel skipped the comparison for that one value
and made Continue-As-New less safe than replay for the identical binding. A header that recorded no
version has no entry at all, and that is what skips the check.

### The continuation is snapshotted where no Workflow code can still run

Creating the Continue-As-New command does not end the activation: the Workflow's event loop keeps
draining ready tasks after a terminal command is added, so a stream consumer scheduled or unblocked
after that point still advances the consumption cursor. Those consumptions do reach the
predecessor's final marker, which is closed on the way out, so a header serialised at command
creation names an earlier boundary than History does and the successor is handed a record a second
time.

Every Continue-As-New command on the completion is therefore re-serialised at the point the
activation emits its stream commands — the same place the observation delta that commits this
boundary is emitted, and the first point at which the boundary has stopped moving. Every command
rather than the last, because two top-level coroutines can each raise one and only Core decides
which terminal survives.

Neither this nor anything about the header's content needs an SDK internal flag. Core matches a
Continue-As-New command to its `WorkflowExecutionContinuedAsNew` event by command type alone and
never compares the command's headers against the recorded ones, so a replay that regenerates the
header at the later boundary cannot disagree with a History written at the earlier one. What the
successor resumes from is the copy in its own `WorkflowExecutionStarted`, which is durable and which
replaying the predecessor does not rewrite.

### The binding is must-understand, and the schema version is what enforces it

The header's reader is the **successor's** Worker, which is what makes its wire compatibility a
different problem from the annotation's. An annotation is written and read by one deploy's Workers;
this header is written by one Run and read by the next Run's Worker, and on an unversioned task queue
that can be an *older* one. It is read while the successor's runtime is built, before any Workflow
code runs.

**A Worker that cannot validate the binding must not start the Run**, and a schema version it
refuses is the only thing that makes it one. Nothing about the byte layout can help here: syntactic
compatibility is not semantic compatibility, and an old Worker that *parses* the header does not
thereby acquire the check — it runs the restoration it shipped with, compares the stream name, and
accepts a cursor it cannot vouch for. If its registry resolves that backend name to another store,
every record below the boundary is skipped in silence, which is the exact failure the binding exists
to prevent. So the binding is encoded **inline in each entry** rather than appended after the entries
an older reader consumes: a trailing block is what an unknowing reader steps over, and stepping over
this is the one thing no reader may do (ADR-039).

What that costs is stated rather than designed away. An old Worker fails the successor's first
Workflow Task, and every identical retry, until a binding-aware Worker picks it up; a rollback to
only old Workers blocks that Run until it is rolled forward. That is a blocked Run rather than a
wrong one, which is the direction ADR-014 and `failure-taxonomy.md` take throughout — an explicit,
retryable incompatibility in place of possible silent data loss.

Two things follow for anyone changing this:

- **Moving the emitted version is a staged deployment step.** Every Worker must *decode* the new
  version before any Worker *writes* it, and the version carried on the value rather than fixed in
  the encoder is what lets a writer be pinned behind its readers while that is arranged. A
  deployment that cannot tolerate the stall at all needs Worker Versioning or build routing to keep
  the successor on a binding-aware build — a deployment mechanism, not something the SDK provides.
- **An older version stays decodable**, and accepting it restores a cursor with the binding checks
  skipped. That is the same outcome an old Worker reaches and not the same fault: a check that was
  never recorded cannot be made, while a check that *was* recorded must never be discarded.

## Subscription numbering is a nondeterminism hazard

`wait_id` is allocated from a per-Run counter in `subscribe()` call order, which puts it in the same
hazard class as timers and activities: inserting, removing, or reordering a `subscribe()` call
renumbers every later wait in that Run.

- Such a change is nondeterministic for Workflows already running against deployed code and must be
  gated behind `workflow.patched()`, exactly as an inserted timer would be.
- Adding a subscription on a code path a running Workflow has not yet reached is safe.
- **Detection is by binding, and it is not complete.** A wait whose stream or backend changed is
  reported where it is registered, and a wait the marker bound that the Workflow no longer creates is
  reported at the end of replay — both as row four of the failure table (`failure-taxonomy.md`)
  rather than as a silently different stream result. What no check can see is a change among waits
  whose bindings are *identical*: inserting a `subscribe()` in the middle of several subscriptions to
  the same stream on the same backend renumbers them without changing any comparison, and an
  addition at the end is a supported change, so the two are indistinguishable from the annotation.
  That residue is why this is stated as a rule about Workflow code rather than left to a check.
