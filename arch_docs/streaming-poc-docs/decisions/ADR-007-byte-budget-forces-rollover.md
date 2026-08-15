# ADR-007 — A hard byte budget forces rollover rather than growing a marker

**Status:** Accepted · **Affects:** P5, C12b, C14a · **Spec:** `spec/annotation-format.md`

## Context

Markers are emitted once per Workflow Task consumption batch, not once per item. But marker *count*
being bounded is not the same as marker *size* being bounded, and only the first follows from
batching. A single Workflow Task can consume unboundedly many records, and an alternating
multi-stream workload produces one run per delivery.

## Options

**A. Trust range encoding.** Runs compress well; assume the marker stays small.

**B. Check the size after encoding** and fail the Workflow Task if it exceeds the event limit.

**C. Enforce a hard budget at encode time** and roll the Workflow Task over before it is exceeded.

## Decision

**C.** `MAX_ANNOTATION_BYTES` is declared as a constant, chosen well below the server's event size
limit, and enforced at encode time:

1. When the accumulated annotation passes a high-water fraction of the budget, Python sets
   `request_rollover` on its next `WorkflowStreamProgress`.
2. Core rolls the task over as it does on deadline expiry, **minus the finalization round trip** —
   the triggering progress command came from Python and already carries the terminal, so Core writes
   the marker and completes with `force_new_wft = true` without issuing `FinalizeExternalStreams`.
   This is the one Core-initiated completion that needs no finalization job, because Python decided
   the boundary.
3. The next Workflow Task starts a fresh annotation whose header records the current cursors.
4. The triggering segment ends with `BUDGET_ROLLOVER`, which is how replay knows the batch continues
   in the following marker.

A is not a bound. B turns a capacity problem into a stuck Workflow: the encoding that overflowed will
overflow again on retry.

## Consequences

- **An annotation can never exceed the budget.** Approaching it is a runtime event, not an error.
- Replay reassembles multi-marker batches in Workflow Task order, matching each marker to its
  Workflow Task rather than concatenating blindly.
- **Bounded marker size is bought with additional Workflow Tasks.** The alternating multi-stream
  workload `A,B,A,B,…` has a schedule transition per record, cannot be compressed by range encoding,
  hits the high-water mark sooner, and rolls over more often. That is the *only* driver of marker
  growth once ADR-003 removes per-record fields.
- In the single-stream scope of Milestone 1 the budget is not expected to fire at all. It is still
  enforced, because "not expected" is not a bound.
- Tests assert **encoded byte size**, not run count — a run-count assertion tells you nothing about
  what a future per-record field would cost.
