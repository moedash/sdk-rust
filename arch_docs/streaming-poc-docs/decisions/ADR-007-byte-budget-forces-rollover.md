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

**The high-water mark is not by itself the whole of C.** It is a *fraction* of the budget and it turns
true only once a frame that crossed it has been emitted, while the frames are indivisible and three of
the four carry strings whose length this side does not choose. A frame can therefore be larger than the
slack a fractional mark leaves, and the batch it records cannot be deferred to the next annotation --
its deliveries happened in this Workflow Task and that task's marker is where replay must find them.
Enforcement at encode time is completed by three further rules, spelled out in
`spec/annotation-format.md`:

- the terminal and any late bindings frame are **reserved** rather than checked, so closing an
  annotation can never be the thing refused;
- **delivery stops** before a segment the annotation could not record, which is the last point at which
  stopping is an option, and that activation ends with `BUDGET_ROLLOVER`. A record is priced by the
  largest run *the annotation* has encoded -- not the largest in the open segment, which is emptied
  every activation and so prices every activation's first record at the bare floor;
- a **spill margin** sits behind the closing reserve for a segment frame to overrun into, which turns a
  misprice into a rollover instead of a refusal, and guarantees that at least one record is affordable
  in every annotation -- without which a Workflow can deliver nothing, observe nothing, and therefore
  never ask for the rollover that was supposed to save it;
- a run larger than that margin is a **capacity limit**, refused where the record has been drained but
  not yet handed to Workflow code, as a non-retryable `ExternalStreamCapacityError` naming the
  provider's offsets. No rollover helps there: the next annotation carries the same run;
- the rollover request is read **after** the activation's segment is closed, so the completion that
  crossed the line is the one that carries it -- read before, it described the annotation as it stood
  an activation earlier and gave the next activation a free frame;
- a subscription set is refused at `subscribe()` when an **empty annotation** could not carry it --
  header, terminal, one segment frame, and the margin -- because a rollover writes a fresh header of
  the same size and so cannot help. Pricing only the header and terminal there accepts a set that
  clears the check and then cannot encode its first completion.

The encoder still refuses an oversized frame if all of that is circumvented, but as a **non-retryable
application failure** -- failing the Workflow, not the Workflow Task. The server retries Workflow Task
failures regardless of cause, so a plain exception there is option B by another route.

## Consequences

- **An annotation can never exceed the budget.** Approaching it is a runtime event, not an error.
- The annotation budget is a **second delivery budget**, sitting alongside the per-activation record
  cap; the smaller of the two wins. Records it leaves buffered have their readiness re-reported exactly
  as the record cap's do.
- `subscribe()` can fail on capacity grounds. It is deterministic -- the same subscriptions in the same
  order give the same answer -- so replay reproduces the refusal rather than diverging on it.
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
