# ADR-045 — Output latency and input parking share one terminal race

**Status:** Accepted · **Affects:** retained Workflow Tasks, output visibility · **Spec:**
`spec/wft-lifecycle.md`

## Context

A Workflow Task retained on external input can also hold Workflow-originated output. Letting input
parking and output visibility use independent timers and handshakes can write two terminals, confirm
a park generation after output already forced rollover, or leave installed intents for a park Core
discarded.

## Decision

Core serializes readiness, idle/all-fenced parking, the output deadline, ordinary rollover, and
server-bound completion as one terminal race. The first accepted output in a batch arms a distinct
deadline; the default and first-release quantum is 100 milliseconds, reduced by `min` as shorter
topic policies join the batch and clamped below ordinary rollover.

- A confirmed park stages the output and reports `WorkflowOutputStreamCommit` before
  `ExternalStreamParkResult(confirmed)`. One shared marker completes the task without forcing a
  replacement.
- A recheck that became ready reports `WorkflowOutputStreamBuffered` before
  `ExternalStreamParkResult(became_ready)`. The task remains open and the earliest output deadline
  remains armed.
- If readiness wins after Python already staged for a confirmation, Core discards the stale park
  terminal, resolves the abandoned park, then finalizes with `TASK_COMPLETED`, records the carried
  output commit once, and forces a replacement.
- If the output deadline wins during park preparation, Core invalidates that quiescence generation,
  resolves the abandoned park so Python removes or records every owed intent removal, then issues
  `FinalizeExternalStreams(OUTPUT_LATENCY)`. A commit already returned by the losing prepare is
  carried forward; otherwise Python stages during finalization. The output marker completes with
  `force_new_wft = true` so input consumption continues on a replacement task.

Late results are stale and cannot produce another terminal or marker.

## Consequences

- Healthy visibility is bounded by the configured quantum plus staging and Workflow Task completion
  latency; it is not an availability SLA during backend or server outage.
- Parking may flush earlier than the output deadline and avoids an extra Workflow Task lifecycle.
- Replay follows the recorded terminal and never arms the wall-clock output timer.
- The output winner never confirms the losing park generation.
