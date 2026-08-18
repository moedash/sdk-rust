# Failure taxonomy

Four distinct outcomes, deliberately not collapsed — each has a different operator response.

## One channel, four meanings

All four outcomes are expressed through the same channel, because a Workflow activation completion
is a `oneof status { Success | Failure }` — a completion cannot both fail the Workflow Task and emit
a terminal command, and `Failure` carries only a `failure` and a `force_cause`.

**There is no protocol-level "non-retryable Workflow Task failure": the server retries Workflow Task
failures regardless of cause.** What differs between the rows is the error surfaced to the user and
to metrics, and the operator response — not whether the task is retried.

| Condition | Completion | Server behavior | Operator response |
|---|---|---|---|
| Backend unreachable, timing out, or erroring on replay | `Failure`, `force_cause = WORKFLOW_TASK_FAILED_CAUSE_EXTERNAL_STORAGE_FAILURE`, **`StreamStorageError`** error type, its own metric | Retried | None — clears when the backend recovers |
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

## What connects a row to a completion

A row is only operator-distinct if something sets the cause and increments the counter, and the
three failing rows arrive at a completion by two different routes. A runtime-only job — a replay
read, a park — raises on the Worker's loop, where an exception becomes a failed completion. A
delivery raises on the **Workflow thread**, where `activate()` converts it into a failed
completion before the async layer ever sees it; out there no exception exists at all.

So **reporting inspects the failed completion, never an exception.** Every failed completion is
matched by the application failure's type name against the taxonomy, walking the cause chain
because the raising frame may have wrapped it. A match sets
`force_cause = WORKFLOW_TASK_FAILED_CAUSE_EXTERNAL_STORAGE_FAILURE` and increments **that row's
counter and no other**; no match leaves both untouched, which is what keeps row four and ordinary
Workflow bugs out of these series. Keying on an exception instead would report the runtime-only
route and silently drop every delivery failure — most of rows two and three.

The type *name* is therefore the seam, so the mapping from name to counter lives with the error
classes rather than at the reporting site: split apart, the two drift and a renamed class stops
being counted without anything failing.

Classification itself happens at **both halves of the decode split** (`python-runtime.md`), because
that is where a record can fail:

- A **preparation** failure is classified on the Worker's loop, carried on the record, and raised by
  the delivery that would have yielded its value.
- A **conversion** failure is classified on the Workflow thread, where the record would have become
  a value.

Both resolve to row three rather than row two, and for the same reason in either delivery path:
replay validated the recorded range before preparing it, and live delivery read the record out of a
backend that guarantees a record's bytes cannot change once written (ADR-003). Either way the range
validated, so the rule above resolves to a decode failure. A cause that is *already* one of these
error types keeps its own row rather than being relabelled — a storage failure surfacing out of
external payload retrieval is row one, not the consumer's converter mismatch.

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
- An unacknowledged shutdown wake is reported through the
  `temporal_external_stream_shutdown_wake_failed` metric rather than retried indefinitely or
  assumed delivered.

## Metrics summary

| Metric | Fires on |
|---|---|
| local wakeup | readiness `Accepted` |
| stale notification | readiness `Stale` |
| signal wakeup, parked | readiness `Parked` |
| signal wakeup, unparked | readiness `NoOpenWorkflowTask` |
| signal wakeup, evicted | readiness `RunNotFound` |
| `temporal_external_stream_storage_failure` | row one |
| `temporal_external_stream_integrity_failure` | row two |
| `temporal_external_stream_decode_failure` | row three |
| `temporal_external_stream_shutdown_wake_failed` | a shutdown wake unacknowledged when the grace period expires |

Row four has no counter, by construction: it is ordinary nondeterminism, and giving it one would put
a Workflow bug in a series an operator alerts on to decide whether to touch a backend.
