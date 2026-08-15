# ADR-014 — Integrity loss blocks the Workflow; there is no terminal-failure opt-in

**Status:** Accepted · **Affects:** P18, P13 · **Spec:** `spec/failure-taxonomy.md`

## Context

When replay finds a recorded record missing — deleted, trimmed, or expired — the Workflow cannot make
correct progress. What should the externally visible outcome be?

## Options

**A. Fail the Workflow** with a terminal `FailWorkflowExecution`.

**B. Block the Workflow**: fail the Workflow Task repeatedly with a distinct error type and metric,
making no further progress until an operator intervenes.

**C. Block by default, with an opt-in to fail terminally** for deployments that prefer it.

## Decision

**B**, with no opt-in.

A destroys recoverable state. Retention loss is usually an operational error and usually repairable;
**a blocked Workflow can be resumed after repair, a failed one cannot.** A Workflow Task completion is
also a success/failure `oneof`, so failing the task and emitting a terminal command are mutually
exclusive.

C is not implementable. Integrity loss is usually discovered *while replaying a historical Workflow
Task*, and that task's commands are already durable in History. Emitting `FailWorkflowExecution` at
that point contradicts the recorded commands and produces a nondeterminism error instead of the
intended clean failure — and replay cannot reach the current Workflow Task without first reading the
records it cannot read. A deployment that wants the Run gone terminates it, which is already the
documented operator action.

> Integrity loss **blocks the Workflow**: the Workflow Task fails repeatedly with a distinct failure
> cause and a non-retryable error message, and the Workflow makes no further progress until an
> operator repairs the backend or terminates the Run.

## Consequences

- **"Non-retryable" describes the error surfaced to the user and to metrics, not the Workflow Task**,
  which the server retries regardless. There is no protocol-level non-retryable Workflow Task failure.
- The distinction from a transient backend outage is in the error type and the metric, not in whether
  the task is retried. An outage clears on its own; integrity loss does not.
- **Operators must alert on the `StreamIntegrityError` metric specifically**, not on Workflow Task
  failure counts, which transient outages also increment.
- P18 registers **no** `workflow_failure_exception_types` entry. (The Python options are
  `workflow_failure_exception_types` on the Worker and `failure_exception_types` on `@workflow.defn`;
  there is no `WorkflowFailureErrorTypes`.)
- Should a Core mechanism to defer the failure until replay reaches a writable current Workflow Task
  ever be designed, the opt-in can be reconsidered.
- **Integrity loss must never resolve to an alternate stream result.**
