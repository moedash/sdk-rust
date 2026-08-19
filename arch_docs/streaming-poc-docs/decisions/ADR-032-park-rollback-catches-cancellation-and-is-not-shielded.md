# ADR-032 — Park rollback catches cancellation, and is not shielded

**Status:** Accepted · **Affects:** P8, P2b · **Spec:** `spec/wft-lifecycle.md`

## Context

The park handshake installs an intent per subscription and then rechecks every stream in the set. An
attempt that does not reach a confirmed result must take every intent it installed back out:
producers read those intents, so a park visible for a generation Core never confirmed is one they
send wakes against and Core discards as stale, and the eviction that follows takes with it the only
record that the intents exist.

Failure is not the only way an attempt ends early. Cancellation is a normal termination path here —
Core withdrawing the activation, the Worker shutting down, the task being torn down — and in Python
`CancelledError` derives from `BaseException`, so it does not pass through a handler written for
`Exception`. Neither do `KeyboardInterrupt` and `SystemExit`.

## Options

**A. Catch `Exception`,** re-raising cancellation ahead of the handler so it is visibly not rolled
back.

**B. Catch `BaseException`** and roll back inline, unshielded.

**C. Catch `BaseException` and shield the rollback**, so it completes even if cancellation is
delivered again.

**D. Leave a cancelled park to the resolve, the reconciliation, and the owed-removal ledger.**

## Decision

**B.**

A abandons exactly the case worth catching. The likeliest way a half-installed park is abandoned is
a Worker on its way down, which is also the case with the least recoverable aftermath: the mirror of
what was installed leaves with the process, so the next Worker can only reach those intents through
registration-time reconciliation of a wait that may never be registered again. A handler that
re-raises cancellation ahead of the rollback reads as deliberate care and does nothing whatsoever,
which is worse than no handler, because it answers the question for the next reader.

C is the tempting refinement and is wrong in a specific way. A shielded rollback detaches from the
cancelled task and can complete *after* `prepare_park` has released the Run's park lock. That lock
is what orders a rollback against the next park, so a rollback outliving it can remove an intent a
newer and entirely legitimate park has just installed — an unparked Run produced by the cleanup
path, which is the same hazard the owed-removal ledger's generation check exists for (ADR-031),
arrived at from the other side. Against that, the shield buys little: a single `Task.cancel()` still
lets an already-running handler's `await` complete, so it only protects against repeated
cancellation, and what repeated cancellation leaves behind is owed rather than lost.

D has nothing to work from. An intent installed by an attempt nobody rolled back is recorded only on
the subscription that attempt was about to abandon; the ledger holds what a removal *decided on*
leaves behind, not what was never attempted.

## Consequences

- **The rollback is total and its failures are swallowed.** Every subscription the attempt installed
  is attempted regardless of what the others do, and the error that reaches the caller is the one
  that ended the attempt — a removal that fails there stays owed for the Run's next drain.
- `KeyboardInterrupt` and `SystemExit` take the same path, which is wanted: they are the interpreter
  going away, which is when a leaked intent is least recoverable.
- **Nothing in the handler may block for long.** It runs on a cancelled task, so a second delivery
  cuts it short, and whatever is left over is owed rather than retried in place.
- A test must cancel `prepare_park` between two installs, await the `CancelledError`, and assert
  that no intent for the attempted generation survives — asserting on the exception alone passes
  against the shape this decision rejects.
