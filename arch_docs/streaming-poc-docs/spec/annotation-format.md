# Replay annotation format

The opaque bytes Python encodes, Core stores in a marker, and Python decodes on replay. Core never
parses it.

Owned by P5 (codec), P10b (emission), P13 (replay read path), C14a/C14b (accumulation and marker
emission).

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

This is safe with respect to Workflow time: all segments of a marker belong to one Workflow Task,
so `workflow.now()` is constant across them in both the live run and replay. Commands produced
during a retained Workflow Task are buffered until the task completes, in both directions.

## Schema

The annotation is a versioned binary encoding. `schema_version` leads, so markers written by older
SDK versions stay readable.

```text
annotation := header, segment*, terminal

header  := schema_version
         , provider_id, provider_format_version
         , streams[]                      // wait_id -> (stream_key, start_cursor)

segment := run*, segment_end_reason
run     := (wait_id, first_offset, last_offset, count, control_positions)
segment_end_reason := NO_DATA_AVAILABLE | BATCH_LIMIT | FENCE_REACHED | BUDGET_ROLLOVER

terminal := blocked_snapshot[]            // wait_id -> cursor boundary: BEGINNING | AFTER(offset)
```

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
- `start_cursor` in the header makes the initial position explicit rather than re-derived.
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

`WorkflowStreamProgress.observation_delta` carries the segments produced since the previous progress
report for the same Workflow Task. Core appends each delta to `ExternalWaitSet.replay_annotation`
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

## Cursor origin

A cursor is never derived from mutable backend state, on any Run.

- **First execution of a chain:** the subscription starts at the `BEGINNING` boundary, recorded in
  the header's `start_cursor` in the subscription's **first** observation delta — which is emitted
  whether or not a record was ever delivered.
- **Subsequent Runs:** the committed continuation state arrives in the reserved Continue-As-New
  header as an `AFTER(offset)` boundary, persisted in `WorkflowExecutionStarted` and restored before
  any subscription is established (ADR-022).

Both paths populate the same header field, so replay reads an explicit starting boundary in every
case, including the case where the stream was empty for the subscription's entire life.

## Subscription numbering is a nondeterminism hazard

`wait_id` is allocated from a per-Run counter in `subscribe()` call order, which puts it in the same
hazard class as timers and activities: inserting, removing, or reordering a `subscribe()` call
renumbers every later wait in that Run.

- Such a change is nondeterministic for Workflows already running against deployed code and must be
  gated behind `workflow.patched()`, exactly as an inserted timer would be.
- Adding a subscription on a code path a running Workflow has not yet reached is safe.
- Detection is not best-effort: a renumbered wait produces the annotation mismatch in row four of
  the failure table (`failure-taxonomy.md`) rather than a silently different stream result.
