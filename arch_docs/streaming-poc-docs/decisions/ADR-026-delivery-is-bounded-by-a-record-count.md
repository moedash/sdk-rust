# ADR-026 — Delivery within one activation is bounded by a record count

**Status:** Accepted · **Affects:** P11, P8, P10b, P13 · **Spec:** `spec/python-runtime.md`

## Context

An activation runs on a thread-pool executor under a **2-second** deadlock timeout. ADR-011 keeps
I/O off that thread, which bounds how long any *single* step of a drain takes — and says nothing
about how many steps there are.

Delivery volume is the unbounded direction. The Workflow thread drains the subscription's buffer
while the watcher refills it concurrently from the Worker's own asyncio loop, so a producer that
never lets the buffer run dry produces an iterator that never blocks and an activation that never
ends. Measured against the real iterator: 316,086 records consumed in 2 seconds, blocking zero
times. The Workflow Task then fails on the deadlock timeout, and every retry does the same, so the
Workflow is stuck permanently — a healthy producer and a healthy backend, and a Workflow that never
progresses.

Workflow Task rollover does not cover this. Rollover bounds the Workflow *Task*, is decided by Core
between activations, and cannot interrupt an activation in progress.

## Options

**A. Bound by elapsed time.** Stop delivering when the activation has spent some fraction of the
deadlock budget.

**B. Bound by the buffer size alone.** Deliver until the buffer is empty; rely on the buffer bound
to make that finite.

**C. Bound by a fixed record count per activation**, and re-arm readiness for whatever is still
buffered when the budget is exhausted.

## Decision

**C.** `MAX_RECORDS_PER_ACTIVATION` is 256 records handed to Workflow code per activation. When the
budget is exhausted the subscription blocks **even though records are still buffered**, which is
what ends the activation, and readiness is re-armed for those buffered records so the next
activation continues from them.

A is nondeterministic. How many records fit in a time slice depends on machine speed, record size,
converter cost, and load, so two runs of the same input segment differently. Segment boundaries are
recorded in the annotation (ADR-018), so replay would drain a different number of times than the
live run did, and `wait_condition` predicates would fire a different number of times — a
nondeterminism failure attributed to Workflow code that did nothing wrong.

B does not bound an activation at all. The buffer bounds memory held ahead of delivery, not the
number of records that pass through it: the watcher refills on the Worker loop while the Workflow
thread drains, and a producer that keeps pace makes "drain until empty" a condition that is never
reached.

Re-arming readiness is not an optimization but part of the decision. The watcher signals readiness
on arrival and has already advanced `prefetch_cursor` past the buffered records, so no further
notification is coming for them. A budget that blocked without re-arming would leave the Workflow
waiting forever on data already in front of it — trading a deadlock timeout for a silent stall,
which is worse, because the stall reports nothing.

**The count is charged at delivery, and an activation begins already charged for what Workflow code
is holding.** Both follow from the budget being a *reservation* rather than a check, and each closes a
schedule that a plainer reading of "256 records per activation" let through.

Charging at consumption — one record at a time, as each is handed over — reads the budget at the drain
and takes nothing, so the next subscription's drain sees the same room and takes it as well. Two
subscriptions consumed by two independent coroutines then take a whole budget each, and `n` of them
take `n`; `merge()` was immune only because it fills one record at a time, which charges and checks in
the same place. Charging at delivery also puts the count on the same quantity the annotation records,
so the cap bounds the segment replay will divide by rather than a different number.

Resetting the count to zero per activation leaves the other half open. A batch is delivered whole and
consumed one record at a time, so a Workflow that stops iterating part-way through carries the
remainder in its ready list — where the next activation takes it with no drain and no check. Free, and
it accumulates: one subscription per activation can leave a nearly full list behind, so `n` of them
arrive holding `n` budgets between them. Starting the count at what the ready lists already hold makes
carried-over-plus-newly-delivered exactly one budget.

## Consequences

- **One activation is bounded by construction**, independently of producer rate, backend speed, and
  buffer size. No workload can make an activation run to the deadlock timeout by volume.
- **"The budget stopped delivery" is now a conservative signal**, and the two things that read it —
  re-arming readiness, and withholding immediate parkability — are safe in that direction. An
  activation pre-charged for a full ready list reports the budget spent without having tried to
  deliver; the re-arm is then skipped by the manager for an empty buffer, and a wait withheld from
  parking is retained for its idle timeout rather than parked. Under-reporting is what loses records,
  since nothing else announces what a budget left behind.
- **A record drained but never consumed still costs its slot.** It has been recorded in the
  annotation, so it is delivery whatever the Workflow does with it, and the Run keeps it in hand.
  Closing a subscription drops its ready list and gives the slots back, since those records can no
  longer be consumed by anyone.
- The segment that exhausts the budget ends with `BATCH_LIMIT`, which is what that
  `segment_end_reason` means and how replay reproduces the same split.
- **The budget does not apply during replay.** Delivery comes from the recorded segments, which
  already fix how many records each activation received; re-imposing a live budget on top could only
  disagree with the recording.
- A fast producer costs more activations rather than one long one. Activations within a retained
  Workflow Task are cheap — they produce no History events and accumulate into one marker — so this
  is paid in segments, not in Workflow Tasks.
- Changing the constant changes recorded segment boundaries for runs written after the change.
  Existing markers replay from their own recorded segments, so in-flight Runs are unaffected, and
  the value is a constant rather than a Worker option so that one deployment cannot split segments
  two ways.
- A producer that never lets the buffer run dry must still let every activation complete, the
  Workflow Task park or roll over, and the marker be written. That is the behavior to hold the
  implementation to.
