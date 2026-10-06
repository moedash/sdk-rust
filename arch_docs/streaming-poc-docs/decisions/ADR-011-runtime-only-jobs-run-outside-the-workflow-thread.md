# ADR-011 — Runtime-only jobs are handled in `_handle_activation`, not `_apply`

**Status:** Accepted · **Affects:** P19, P8, P11, P13 · **Spec:** `spec/python-runtime.md`

## Context

Four activation jobs are added. Two of them are *themselves* backend operations:
`ReplayExternalStreams` needs inclusive range reads plus integrity validation over the whole recorded
range, and `PrepareExternalStreamPark` needs a backend transaction. The natural place to handle an
activation job is the `job.HasField(...)` dispatch chain in `_apply`.

## Options

**A. Handle all four in the `_apply` dispatch chain**, like every other job.

**B. Partition them in `_handle_activation`**, the async layer above `activate()`.

## Decision

**B.**

A is wrong by construction. `_WorkflowInstanceImpl.activate()` is **synchronous**, runs on a
thread-pool executor under `asyncio.wait_for` with a **2-second** deadlock timeout, and the Workflow
event loop is a custom deterministic loop that cannot await real network I/O. Putting a multi-second
backend transaction there fails the Workflow Task for a *healthy* backend, and the failure mode gets
worse the more records replay must validate.

`_handle_activation` is already `async` and already performs pre-activation await work —
`decode_activation` is awaited there before `workflow.activate` is handed to the executor. Stream jobs
are partitioned in the same place:

| Job | Handling |
|---|---|
| `PrepareExternalStreamPark` | Handled entirely there; the worker synthesizes `ExternalStreamParkResult` without calling `activate()` at all |
| `FinalizeExternalStreams` | Same place; its input terminal is manager-only, while `OUTPUT_LATENCY` may additionally stage output in the outer async layer — see ADR-010 and ADR-045 |
| `ReplayExternalStreams` | *Prepared* there: buffers filled and validated, then the job passes through to `activate()` for in-memory delivery |
| `ResolveExternalStreamWaits` | Passes straight through — readiness already means "buffered" |

## Consequences

- **`_apply` performs no I/O, ever**, and completes in bounded time. It pops from the buffer and
  converts what it popped, and conversion is the half of decoding that awaits nothing (ADR-028).
- Replay is indistinguishable from live delivery to the Workflow thread, because both drain the same
  buffer — one filled by the replay reader, one by a live watcher.
- Failures propagate through the defined activation-failure path — transient backend error →
  `WORKFLOW_TASK_FAILED_CAUSE_EXTERNAL_STORAGE_FAILURE`, integrity violation → `StreamIntegrityError`
  — rather than surfacing as a deadlock timeout, which would misattribute a storage problem to the
  Workflow's own code.
- This is a **named deliverable (P19)**, not an implementation detail of the manager: it is the only
  thing standing between a slow backend and a spurious `_DeadlockError`.
- Tests drive a replay job and a park job that each take longer than the deadlock timeout and require
  both to complete.
