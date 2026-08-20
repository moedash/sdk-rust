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

**The recovery is bound to the operation, the stream, and the producer instance**, and each binding
answers a distinct way the record could be duplicated or misrouted:

- **The operation, not just the record.** What the interrupted call owed includes whether a wake was
  due, under which lease, and whether cancellation is still to be honoured. The producer holds all of
  it, and every raise about that append reports the producer's current canonical state for the
  operation. A refusal that described the *refused* call instead would make its own instructions
  wrong: a caller following them settles with `wake=False` a record that owed a Signal, and the parked
  Workflow stays parked on a durable record nobody announced. If `resolve_append()` itself is
  interrupted, its effective wake and lease replace the earlier attempt's: it is now the call whose
  outcome is unknown. Cancellation is cumulative rather than replaceable, because once delivered it
  remains for the caller to honour after settlement. The retained state and the newly raised error
  are therefore produced from that same updated operation.
- **The stream.** A `StreamRecord` names its producer session and sequence but not its stream, and
  idempotency is scoped per stream. On any other topic that key has never been used, so settling there
  appends a second copy of the value onto a stream no consumer of it is watching, and leaves the real
  stream blocked. `resolve_append` refuses a record that is not *this* topic's outstanding append, and
  refuses one whose bytes differ from it, before touching the backend.
- **The producer instance.** A replacement producer built with the same session id has its sequence
  and wake counters back at zero, so settling there leaves both invalid: its next publish reuses a
  sequence number the recovered record already holds, and its next unparked wake re-derives a request
  ID an earlier and *different* wake already used — which the server deduplicates away, leaving a
  durable record unannounced. The recovery for a producer that is gone is the one the Activity retry
  already performs: rebuild with the same session id and re-run the same calls in the same order. That
  re-derives the same sequences, so the appends deduplicate, and the counters advance correctly
  because the calls actually ran. `resolve_append` is the in-process recovery for the instance that
  still owes the append, and says so when refusing.

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
- **Recovery is in-process only.** `resolve_append` names the producer, the topic and the exact
  record, and refuses anything else with a `ValueError` that says which of the three did not match.
  Cross-process and cross-attempt recovery is the Activity retry, which needs no new mechanism.
- **Repeated recovery updates one operation rather than creating a parallel version of it.** A
  re-interrupted `resolve_append()` makes its effective wake and lease the defaults for the next
  attempt, and cancellation stays set if any attempt received it. Later refusals and recovery errors
  report exactly that retained state.
- The `.wake` and `.lease` of a recovery default to what the interrupted call was doing, rather than
  to the method's own defaults. A recovery that chose its own policy would give a `wake=False` fence a
  Signal and take one away from a `wake=True` publish.
- Nothing changes for a backend that never loses an answer; the new outcome is unreachable when
  `append()` always returns or raises `AppendConflictError`.
- A test must commit inside `append()` and *then* lose the answer, settle it, and find exactly one
  record — one fence, for `finish_writing()`. A test that blocks before the commit exercises the same
  code path and proves nothing about the duplicate, because the producer cannot tell the two apart.
