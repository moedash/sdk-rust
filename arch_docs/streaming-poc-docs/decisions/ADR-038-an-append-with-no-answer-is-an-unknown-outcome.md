# ADR-038 — An append that reports no outcome is unknown, not failed

**Status:** Accepted · **Affects:** P6, P6b · **Spec:** `spec/wake-signal.md`

## Context

ADR-036 gave the post-append window an outcome that names the offset and one recovery, cancellation
included. It left one assumption in place: that cancellation or a failure *before* `append()` returns
means nothing landed. For a local backend that is true. For a remote one it is not.

A backend commits on its own side and only then answers. The Redis provider runs an atomic append
script server-side and receives its result in a separate client-side step, so a cancellation or a
dropped connection between those two steps leaves a durable record whose offset nobody holds. What
reached the caller there was a bare `CancelledError` or a bare `ConnectionError`: no offset, no
record, nothing that identifies the write whose fate is in question.

That is unrecoverable in both directions, and each direction breaks a different half of the producer's
contract:

- **Retrying `publish()`** draws a new sequence number, and therefore a new idempotency key, so a
  record that did land is appended a second time under a key the backend has no way to relate to the
  first. For `finish_writing()` the duplicate is a second write fence, which reads back as a producer
  session that ended twice.
- **Not retrying** can leave a durable record that no wake was ever sent for, stranding a parked
  Workflow for its whole idle timeout — the exact state the acknowledged-wake contract exists to
  refuse.

The caller cannot choose between them, because nothing it was handed says which history happened.

## Options

**A. Keep reading "did not return" as "did not happen".** The status quo after ADR-036.

**B. Probe on the interrupted path** — read the stream back, shielded, and decide whether the record
is there before reporting anything.

**C. Auto-resolve** — re-append the exact record from inside the interrupted call, so the caller only
ever sees a settled outcome.

**D. Fold it into `WakeNotAcknowledgedError`**, the way ADR-036 folded cancellation, with the offset
left unset.

**E. Give the window its own outcome**, carrying the exact record, with a recovery operation that
re-appends *that* record — and refuse further appends on the stream until it is settled.

## Decision

**E.** Everything out of `backend.append()` that is neither a return nor a contractual refusal raises
`AppendNotAcknowledgedError`, carrying `.record` — byte-identical, still holding its
`(session_id, sequence)`, offset unset — plus `.wake`, `.lease` and `cancelled=True` when cancellation
ended the attempt. `ExternalStreamProducerTopic.resolve_append(record)` re-appends that record and
then runs the wake, so a failure in the wake stage raises `WakeNotAcknowledgedError` with the offset
exactly as `publish()` does.

**The recovery is correct for both histories, which is what makes an unknown outcome actionable
rather than merely honest.** ADR-020 already requires that a repeat append of byte-identical content
under a used key write nothing and return the original offset, and a key the backend never saw is
simply appended now. One call, one record, one offset, either way.

**Until it is settled, the stream refuses further appends from that producer with the same error.**
This is the substantive half. Reporting the state and leaving the caller free to `publish()` again
still permits the duplicate — the failure is not that the caller lacked information, it is that the
obvious next call is the wrong one. The refusal is checked at entry, so a concurrent publish already
past that point completes; concurrent publishes have no defined order between them to preserve.

**`AppendConflictError` is exempt**, and is the only exemption the contract can support: it says the
key was used with *different* bytes, so this record did not land and re-appending it would raise the
identical error. Treating it as unknown would send the caller to settle an append that has no
settlement and would wedge the stream behind it. `KeyboardInterrupt` and `SystemExit` are not
converted either, for ADR-036's reason — the interpreter is going away and there is no caller left to
recover.

A is what the review found, and its cost is a silently duplicated record.

B cannot be relied on, for the reason ADR-036 rejects deferring or shielding the wake: the probe runs
on a cancelled task, a second delivery cuts it short, and the ambiguity is back. It is also not free —
without an index from idempotency key to offset, the probe is a scan — and it does not remove the need
for E, because the failure that loses the append's answer is usually the one that loses the probe's
too. Two mechanisms where one is sufficient.

C hides the choice the caller has to make. Cancellation is refused rather than propagated already
(ADR-036), but auto-appending goes further: it publishes a value on a task that was asked to stop, in
the history where the first attempt never landed. Whether to complete the write or honour the
cancellation belongs to the caller; what the SDK owes is that both remain reachable.

D repeats ADR-036's argument in a case where it does not hold. Folding cancellation into the wake
error was right because the *state* was identical — durable record, unsent wake, one recovery. This
state is not that one: the offset is unknown, the recovery is an append rather than a Signal, and
`retry_wake` on it would announce a record that may not exist. A caller's existing
`WakeNotAcknowledgedError` handler is wrong here rather than merely incomplete, which is precisely the
case a second type is for.

## Consequences

- **`publish()` has three outcomes, not two**: appended and announced, appended but unannounced, and
  outcome unknown. The third is the only one whose recovery touches the backend's append path.
- **A cancellation is "before the append" only if the backend was never called.** The producer draws
  its sequence and encodes the payload first, and an interruption there still raises `CancelledError`
  with nothing appended and nothing owed. Once the call is in flight, the window is ambiguous by
  construction and is reported as such.
- **An unsettled append blocks its stream.** That is deliberate: the alternative is a duplicate. A
  caller that cannot settle it — a dying Activity — drops the producer, and the retried attempt reuses
  the session id and re-derives the same key, so its own re-append is the same no-op.
- Nothing changes for a backend that never loses an answer; the new outcome is unreachable when
  `append()` always returns or raises `AppendConflictError`.
- A test must commit inside `append()` and *then* lose the answer, settle it, and find exactly one
  record — one fence, for `finish_writing()`. A test that blocks before the commit exercises the same
  code path and proves nothing about the duplicate, because the producer cannot tell the two apart.
