# ADR-018 — Replay reproduces activation segmentation rather than collapsing it

**Status:** Accepted · **Affects:** P5, P13, C10 · **Spec:** `spec/annotation-format.md`

## Context

A retained Workflow Task may span many `ResolveExternalStreamWaits` activations but produces exactly
one marker. On replay, Core issues one `ReplayExternalStreams` job for that marker. The question is
whether the *k* original activations must be reproduced as *k* drains.

## Options

**A. Collapse.** Deliver all recorded records in one drain; record order is what matters.

**B. Reproduce, by issuing *k* activations from Core.** Requires Core to understand the annotation.

**C. Reproduce inside Python.** The annotation carries one segment per original activation; the replay
driver performs one event-loop drain per segment.

## Decision

**C.**

A changes behavior. Under `_single_batch_activation`, each stream activation drives exactly one
`_run_once(check_conditions=True)` drain, and stream jobs land in the non-query, non-signal job set,
so `workflow.wait_condition` predicates are evaluated **once per activation**. Reproducing only the
record order while changing how many drains occurred changes when conditions fire.

B would require Core to parse the annotation, which it is designed not to do.

Under C the live run's *k* activations become *k* drains inside one replay activation, so coroutine
scheduling and condition evaluation match.

*k*, and not *k + 1*. The replay driver is not the only thing that drains: the job lands in the
non-query job set, so the activation runs one `_run_once` of its own for that set whatever the driver
did. The driver therefore drains the first *k - 1* segments and arms the last, leaving that one drain
to the activation -- which is also what the live run did, where each activation's single trailing drain
served the records it had just been handed. Closing the replay moves to after that drain, in the
activation's own `finally`.

## Consequences

- The annotation grammar carries `segment*`, one per original activation, each with its own
  `segment_end_reason`.
- **An empty segment is meaningful** and must round-trip: an activation that drained and found nothing
  still ran one `_run_once`.
- Segmentation reproduces **how many** drains happened; the global order of runs within a segment
  reproduces **which wait** each drain served. A replay that kept the segment boundaries but let a
  drain search the segment for its own wait would reproduce the count and reorder the deliveries,
  which is the half of the live schedule concurrent coroutines can observe.
- This is safe with respect to Workflow time: all segments of a marker belong to one Workflow Task, so
  `workflow.now()` is constant across them in both the live run and replay. Commands produced during
  a retained Workflow Task are buffered until the task completes, in both directions.
- Core stays annotation-blind.
- The test list requires a `wait_condition` registered mid-stream to fire on the same delivery under
  replay as it did live, for a marker spanning several activations.
- A test that drives only the replay driver cannot see the count: the drain it misses is the
  activation's. The drain count is asserted across the whole activation, driver plus trailing
  `_run_once`, or it is not asserted at all.
- **A zero-segment marker is the exception and closes before that drain**, because with no segment to
  serve the drain is a live one and a reposition after it would retract records it had just delivered.
  See `spec/annotation-format.md`.
