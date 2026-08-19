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
