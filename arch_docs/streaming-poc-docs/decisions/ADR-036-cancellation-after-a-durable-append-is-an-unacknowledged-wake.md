# ADR-036 — Cancellation after a durable append is reported as an unacknowledged wake

**Status:** Accepted · **Affects:** P6b, P14 · **Spec:** `spec/wake-signal.md`

## Context

This record is about what happens *after* an acknowledged append; the append's own acknowledgement
window is ADR-038.

Once the append is acknowledged, `publish()` distinguishes two outcomes: the append succeeded and the
wake did not, or both did. Every failure after the append carries the durable offset and names one
recovery, which
is what stops a caller re-publishing a record that already landed — a second `publish()` draws a new
sequence number, and therefore a new idempotency key, so it appends the value twice.

Cancellation was outside all of that. `CancelledError` derives from `BaseException`, so it passed
through the coordination handler (which re-raised it ahead of the conversion, deliberately), through
the Signal loop (which caught `Exception`), and through `publish()` itself (which catches only the
durable-but-unacknowledged error). What reached the caller was a bare `CancelledError` with no
offset, no `pending` and no `restart` — indistinguishable from cancellation *before* the append, and
so unrecoverable in both directions at once: the caller could not wake a record it had not been told
about, and could not safely re-publish the value either.

Cancellation here is ordinary, not exotic. An Activity being cancelled, a Worker shutting down, and a
`asyncio.timeout` around a publish all produce it, and all of them can land after `append()` has
returned.

## Options

**A. Re-raise the bare `CancelledError`.** The status quo.

**B. Defer cancellation through the post-append section**, completing the wake and then re-raising,
so the state is clean and the cancellation is honoured.

**C. Shield the wake**, letting it finish detached from the cancelled task.

**D. Raise a cancellation-specific exception** carrying the offset and the recovery — a new type the
caller has to learn.

**E. Report it as the existing durable-but-unacknowledged outcome**, with a flag saying cancellation
is what ended the attempt.

## Decision

**E.** Any `Exception` *or* `asyncio.CancelledError` raised after a durable append leaves
`publish()`, `finish_writing()` and `wake()` as `WakeNotAcknowledgedError` carrying `.offset`, one of
`pending`/`restart`, and `cancelled=True` when cancellation was the cause. `KeyboardInterrupt` and
`SystemExit` are not converted: they are the interpreter going away, and there is no caller left to
recover.

A is what the review found. The two states it fuses — nothing appended, and appended but
unannounced — have opposite recoveries, and the caller cannot tell them apart.

B and C both try to make the problem disappear by finishing the wake, and neither can promise it. The
handler would run on a cancelled task, so a second delivery cuts it short and the ambiguous state is
back; ADR-032 already establishes that nothing on a cancelled path may block for long, and the wake's
third step is a Signal RPC. C additionally detaches: a shielded wake outlives the call that owns it,
so `publish()` returns or raises while a Signal it can no longer report on is still in flight — the
same "someone else may be sending" that the acknowledged-wake contract exists to refuse.

D is E with a second name for one state. The state *is* the durable-but-unacknowledged one — durable
record, unsent wake, one recovery — and the caller's existing handler already knows what to do with
it. A separate type would mean every caller writing the same recovery twice, and a caller that
forgot the second one would be back to the ambiguity this record is about.

The cost of E is real and is accepted: cancellation is refused rather than propagated, so a
`Task.cancel()` ends the task with this error instead of as cancelled, and an `asyncio.timeout`
wrapped around a publish will not convert to `TimeoutError`. `cancelled` is what makes that
recoverable — a caller that wants to honour the cancellation re-raises after recovering the wake,
which is the only order that leaves nothing owed.

## Consequences

- **The post-append section has no exit that says nothing.** Every way out of it names the offset and
  one recovery, cancellation included.
- Cancellation delivered *before the backend is called* still raises `CancelledError`. Nothing was
  sent, so there is no offset to carry and nothing to recover; reporting an unacknowledged wake there
  would send the caller to wake a record that does not exist. This record originally said "before the
  append", which assumed an append that did not return had not happened — true of a local backend and
  false of a remote one. **ADR-038 corrects that half**: cancellation delivered while `append()` is in
  flight is its own outcome, `AppendNotAcknowledgedError`, because the record may already be durable.
- `wake()` converts cancellation even though it can be called without an append of its own, because
  it is only ever reached after one — `wake=False` followed by one `wake()` is a batch whose records
  are already durable.
- A test must cancel a publish *after* the append and assert the error carries the offset and exactly
  one recovery, perform that recovery, and find one record in the stream. Asserting on the exception
  type alone passes against a conversion that dropped the offset.
