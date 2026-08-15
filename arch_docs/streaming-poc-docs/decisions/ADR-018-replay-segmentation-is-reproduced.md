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

## Consequences

- The annotation grammar carries `segment*`, one per original activation, each with its own
  `segment_end_reason`.
- **An empty segment is meaningful** and must round-trip: an activation that drained and found nothing
  still ran one `_run_once`.
- This is safe with respect to Workflow time: all segments of a marker belong to one Workflow Task, so
  `workflow.now()` is constant across them in both the live run and replay. Commands produced during
  a retained Workflow Task are buffered until the task completes, in both directions.
- Core stays annotation-blind.
- The test list requires a `wait_condition` registered mid-stream to fire on the same delivery under
  replay as it did live, for a marker spanning several activations.
