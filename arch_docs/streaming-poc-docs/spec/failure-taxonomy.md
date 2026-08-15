# Failure taxonomy

Four distinct outcomes, deliberately not collapsed — each has a different operator response.

Owned by P18 (error types and metrics), P13 (classification at the replay read path).

## One channel, four meanings

All four outcomes are expressed through the same channel, because a Workflow activation completion
is a `oneof status { Success | Failure }` — a completion cannot both fail the Workflow Task and emit
a terminal command, and `Failure` carries only a `failure` and a `force_cause`.

**There is no protocol-level "non-retryable Workflow Task failure": the server retries Workflow Task
failures regardless of cause.** What differs between the rows is the error surfaced to the user and
to metrics, and the operator response — not whether the task is retried.

| Condition | Completion | Server behavior | Operator response |
|---|---|---|---|
| Backend unreachable, timing out, or erroring on replay | `Failure`, `force_cause = WORKFLOW_TASK_FAILED_CAUSE_EXTERNAL_STORAGE_FAILURE`, transient error type | Retried | None — clears when the backend recovers |
| Recorded offset missing, expired, reordered, or count mismatch — i.e. deletion, trimming, or retention expiry | `Failure`, same `force_cause`, **`StreamIntegrityError`** error type, distinct metric | Retried, and keeps failing | Repair or restore the backend, or terminate the Run |
| Record bytes present and intact, but the DataConverter or codec cannot decode them | `Failure`, same `force_cause`, **`StreamDecodeError`** error type, its own metric | Retried, and keeps failing | Align the consumer's converter/codec with the producer's — the stream is fine |
| Recorded annotation does not match the subscriptions Workflow code creates | `Failure` as a nondeterminism error, same class as a command mismatch | Retried, and keeps failing | Fix or version the Workflow code |

Rows two through four are distinguished from row one by **error type and metric, never by the retry
behavior of the Workflow Task**. Alerting must key on those metrics rather than on Workflow Task
failure counts, which row one also increments.

## The classification rule

Mechanical and, since structural immutability is required of every provider, unconditional:

> **If the range validated, the bytes are the bytes that were written**, so any subsequent decode
> failure is row three. Only a missing offset or a range that fails validation is row two.

Row three is separated from row two because a converter mismatch is a configuration error on the
*consumer*, and reporting it as stream integrity loss sends an operator to restore a backend that was
never damaged (ADR-015).

Row four covers both directions: a marker referencing a `wait_id` Python never created, and Workflow
code subscribing where the annotation has no corresponding wait. Core-side, a marker found by
lookahead with no matching state machine is handled as local activities already handle that case.

## Integrity loss blocks the Workflow

The externally visible result of integrity loss is a **blocked Workflow**:

> The Workflow Task fails repeatedly with a distinct failure cause and a non-retryable error message,
> and the Workflow makes no further progress until an operator repairs the backend or terminates the
> Run.

It does **not** terminate the Workflow (ADR-014). Retention loss is usually an operational error and
usually repairable; a blocked Workflow can be resumed after repair, a failed one cannot.

"Non-retryable" therefore describes the error surfaced to the user and to metrics, not the Workflow
Task, which the server retries regardless. The distinction from a transient backend outage is in the
error type and the metric, not in whether the task is retried; an outage clears on its own and
integrity loss does not. **Operators are expected to alert on the integrity metric specifically.**

**There is no terminal-failure opt-in.** See ADR-014 for why one is not implementable.

**Integrity loss must never resolve to an alternate stream result.**

## Additional failure rules

- Backend failure during a live activation fails the Workflow Task with the external-storage failure
  cause; an uncommitted cursor is retried from the preceding marker.
- Watcher failures are retried within provider policy. If readiness can no longer be monitored
  safely, Python reports the failure to Core rather than allowing the WFT to time out silently.
- A worker crash discards uncommitted reads. Replay re-reads from the last committed marker.
- Stale readiness notifications are ignored by wait and quiescence generation.
- No cursor advances unless the marker commits the complete cross-stream annotation.
- An unacknowledged shutdown wake is reported through the `external_stream_shutdown_wake_failed`
  metric rather than retried indefinitely or assumed delivered.

## Metrics summary

| Metric | Fires on |
|---|---|
| local wakeup | readiness `Accepted` |
| stale notification | readiness `Stale` |
| signal wakeup, parked | readiness `Parked` |
| signal wakeup, unparked | readiness `NoOpenWorkflowTask` |
| signal wakeup, evicted | readiness `RunNotFound` |
| `StreamIntegrityError` | row two |
| `StreamDecodeError` | row three |
| `external_stream_shutdown_wake_failed` | a shutdown wake unacknowledged when the grace period expires |
