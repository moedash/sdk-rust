# ADR-009 — Shutdown and eviction are two transitions, split by whether a WFT is open

**Status:** Accepted · **Affects:** C15b, P20 · **Spec:** `spec/wft-lifecycle.md`

## Context

When a Worker shuts down with subscriptions still active, something must create server-visible work
so another Worker can pick the Run up. `force_new_wft` is the obvious mechanism.

## Options

**A. Set `force_new_wft` on the way out**, uniformly, for every Run with active subscriptions.

**B. Let the language SDK request the forced task** on its completion.

**C. Two transitions**, chosen by whether the Run holds a Workflow Task.

## Decision

**C.**

A is not implementable in the state that needs it most. `force_new_wft` rides on
`ActivationCompleteOutcome::ReportWFTSuccess`, which is built from `data.task_token` — the token of
the Workflow Task the Run currently holds. **With no open Workflow Task there is no task token, no
completion, and therefore nothing to set `force_new_wft` on.** The window after a command-producing
or rolled-over completion is exactly that state: subscriptions active, no park generation installed,
no Workflow Task.

B is also unavailable: `force_new_wft` is a local variable computed inside
`ManagedRun::prepare_complete_resp`, not a field in the completion protocol, and Worker shutdown
routinely begins while no Python activation is outstanding, so there may be no completion to ask on.

| Run state | Marker | Server-visible replacement |
|---|---|---|
| Workflow Task open | Core issues `FinalizeExternalStreams{SHUTDOWN}` and writes the marker from the finalized annotation | Same completion sets `force_new_wft = true` |
| No open Workflow Task | **None** — nothing is accumulated (ADR-008) | Python's manager sends an acknowledged unparked wake Signal per active subscription, then tears the watcher down |
| No open Workflow Task, wait set parked | None | None needed — a confirmed park generation exists, so the producer's wake path applies unchanged |

## Consequences

- Both transitions are **runtime-owned**, not something the consuming Workflow requests.
- Both are offers to the task queue, not promises by the shutting-down Worker. Any eligible Worker may
  pick the task up and reconstruct the subscription from the marker; if none is available, the task
  times out and the server retries it — ordinary Worker-shutdown behavior.
- The no-open-WFT case needs the read-only `external_stream_run_status` probe (C4) to tell the states
  apart, and it must not use the readiness call, which would falsely claim a record is buffered.
- The sweep cannot ride on the eviction path: an idle cached Run with no Workflow Task has no pending
  work, so `shutdown_done` drops it with no eviction activation at all.
- An unacknowledged shutdown wake is retried within the grace period under the same request ID, then
  reported through `temporal_external_stream_shutdown_wake_failed`. **A shutting-down Worker can promise a
  hand-off attempt; it cannot promise the hand-off.**
- C15b and P20 are declared as separate deliverables because they cover mutually exclusive Run states;
  testing them together would hide the case where the wrong mechanism is applied to the wrong state.
