# ADR-016 — The idle timeout is a Workflow-Task policy reduced by `min`

**Status:** Accepted · **Affects:** C6, P10a, P21 · **Spec:** `spec/wft-lifecycle.md`

## Context

`idle_timeout` is configured per subscription through `with_options`. Core runs **one** timer for the
whole quiescent set, so several configured values must reduce to one before the completion is sent.

## Options

**A. Per-subscription timers.** Each subscription parks on its own schedule.

**B. One timer, reduced by `max`.** The most patient subscription wins.

**C. One timer, reduced by `min`.** The shortest configured timeout wins.

## Decision

**C**, applied over the quiescent set in `wait_id` order, then clamped below the rollover deadline.

A defeats the point: the idle timeout exists to decide whether the *Workflow Task* can be released,
and one idle stream must not park the task while another is still delivering records. It is a property
of the set, not of a member.

B breaks the option's meaning for whoever set the shortest one — a subscription configured to stop
waiting after 200 ms must not be held for 5 s because another member of the set is more patient.

`min` also degrades safely: a shorter idle timeout costs an extra Workflow Task, never a lost record.

## Consequences

- The inputs are the configured values of the quiescent set and nothing else — no wall-clock input —
  so the result is **deterministic and reproduces on replay**.
- Default is one second.
- **Validation rejects a non-positive or missing value at `with_options` time rather than coercing
  it**, and Core independently rejects a `WorkflowStreamQuiescent` whose `idle_timeout` is
  non-positive as a malformed completion.
- Core clamps the effective value below the Workflow Task rollover deadline so rollover stays
  authoritative (ADR-017).
- Core never runs per-stream timers.
- Milestone 2 tests the reduction directly, including that the same reduction is observed on replay.
