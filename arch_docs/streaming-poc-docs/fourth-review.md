# Workflow streaming implementation review — fourth round

**This is a review artifact, not part of the design.** Like `review-guide.md`, `follow-up-review.md`
and `third-review.md`, it records what was found and what was done about it. Everything else in this
directory states current truth and carries no revision narrative; the specs and decision records named
below were updated in place, and those are the authority on what the code now does.

Date of review: 2026-08-19 · Reviewed revisions: `sdk-python` `e3c0571f`, `sdk-rust` `620ef949`

The review found five defects — four P1 and one P2 — with the third round's fixes as its baseline, and
reproduced each one against the current implementation before reporting it. It found no further
Core-side defect at that confidence bar, and every finding is Python-side.

**Each fix is covered by a test that was confirmed to fail against the pre-fix code**, by reverting the
fix and re-running it — step 4 of "before reporting a defect found by a test" in
`verification-hazards.md`. Cases 72-76 of `required-tests/tests-m1.md` are those tests.

Reviewing the fixes then found one more P1 in this round's own code, and validating *that* fix found
three more in its recovery path. Validating those fixes found one final P1 in repeated recovery. All
three rounds are recorded below under "Defects the fixes themselves introduced"; cases 77-79 are
their test sets.

| Severity | Finding | Status |
|---|---|---|
| P1 | Independent subscriptions can jointly exceed the activation delivery budget | Fixed |
| P1 | Concurrent encodes make Activity-retry idempotency depend on completion order | Fixed |
| P1 | Cancellation after a durable append is an outcome with no recovery state | Fixed |
| P1 | A second consumer of one subscription strands the first permanently | Fixed |
| P2 | A `Stale` retry that discovers `RunNotFound` keeps the dead Run's watcher | Fixed |

Four of the five share the shape the third round ended on, and it is worth naming again: a guarantee
written for the path it was aimed at and not for the adjacent one. A budget checked where it was not
reserved. An idempotency key drawn after an `await` that can reorder it. A handler for `Exception` on a
path that cancellation also takes. A retry whose answer was thrown away by the caller that asked for
it.

## P1 — Independent subscriptions can jointly exceed the delivery budget

**Confirmed, at 511 records against a cap of 256.** `_fill` read the remaining budget and charged
nothing; `record_consumption` charged one record at a time afterwards. So the check was not a
reservation: the first subscription drained all 256 slots into its private ready list and yielded one,
the second saw 255 still available and drained those, and both then consumed lists nothing would check
again. `merge()` was immune only incidentally — it fills one record at a time, so it charges and checks
in the same place — which is why the existing coverage did not see it.

Reproducing it also showed the cap was measuring the wrong quantity. `record_delivery` accumulates into
the annotation at drain time, so a count charged at consumption bounds something the recorded segment
does not: the segment could hold more records than the number replay divides activations by.

**Fixed by charging the budget at delivery**, where the records move, and by **starting each
activation's count at what the ready lists already hold** rather than at zero. The second half closes
the same hole from the other side, and the review did not raise it: a batch is delivered whole and
consumed one record at a time, so a Workflow that stops iterating part-way through carries the
remainder into the next activation, where it is consumed with no drain and therefore no check at all.
That remainder was free, and it accumulates — one subscription per activation can leave a nearly full
list behind, so *n* of them arrive holding *n* budgets between them and hand all of it over in one
`activate()` call, which is the same unbounded `n ×` the review reported by a slower route. With both
halves, what an activation may hand over — carried over plus newly delivered — is exactly one budget
under any schedule.

A ready list is deliberately *not* refused when the budget is spent: a subscription holding records is
never blocked, so no readiness has to be re-armed for it, and refusing would have needed a new
"blocked with records in hand" state that the re-arm path (which re-reports from the manager's buffers)
cannot see.

Spec: `spec/python-runtime.md`, "Delivery within one activation is bounded by a record count". Decision:
ADR-026, extended with where the count is charged.

## P1 — Concurrent encodes make idempotency depend on completion order

**Confirmed.** `publish()` drew its sequence number *after* awaiting the payload encode, so identities
were handed out in encode-completion order rather than invocation order. A codec may do real I/O — an
external payload store, a KMS round trip — and that order is not stable across attempts, so two
concurrent publishes exchange keys whenever the store answers the other way round. Reproduced with a
gated codec: attempt one released `"a"` first and appended at sequences 0 and 1; the retry, same session
id and same call order, released `"b"` first and raised `AppendConflictError` on both calls.

The cross-topic case is the quieter half and was reproduced too. Deduplication is scoped by stream key,
so swapped keys on two different topics do not collide — they land under keys the other stream has never
seen, and the retry appends duplicates while raising nothing.

**Fixed by drawing the sequence at the call, before the encode is awaited**, in `publish()` and
`finish_writing()` alike. An encode that then fails leaves its number unused, which costs nothing: a gap
is unobservable, since offsets come from the provider, and the retry reuses the same number for the same
call.

Spec: `spec/backend-contract.md`, producer binding. Decision: ADR-020, which now states that the
producer side owes the key its stability.

## P1 — Cancellation after a durable append had no recovery state

**Confirmed.** `CancelledError` derives from `BaseException`, so it passed through the coordination
handler (which re-raised it ahead of the conversion, deliberately), the Signal loop (which caught
`Exception`), and `publish()` itself (which catches only the durable-but-unacknowledged error). What
escaped was a bare `CancelledError` carrying no offset, no `pending` and no `restart` —
indistinguishable from cancellation *before* the append, and so unrecoverable in both directions: the
caller could not wake a record it had not been told about, and retrying `publish()` drew a new sequence
number and appended the value twice. Reproduced at every stage after the append, including the Signal
send, and for `finish_writing()`, whose duplicate reads back as a producer session that ended twice.

**Fixed by reporting it as the outcome it is** — durable record, unsent wake — with `cancelled` set so a
caller that wants to honour the cancellation can re-raise after recovering the wake. Cancellation
delivered *before* the append still propagates as cancellation, and that half is asserted too: nothing
landed, so there is no offset to carry and nothing to recover. Where "before the append" ends turned out
to be the next finding, below.

Deferring the wake through the cancelled section, or shielding it, were both considered and rejected —
neither can promise the wake completes, and shielding detaches a Signal from the call that owns it.
ADR-036 has the full comparison.

Spec: `spec/wake-signal.md`, "All three steps after the append are inside the guarantee". Decision:
ADR-036.

## P1 — A second consumer of one subscription strands the first

**Confirmed.** `__aiter__` returns a new generator per call, but the cursor, the blocked flag and the
readiness future belong to the subscription — and the future is a single slot held in two places, on the
subscription where `close()` finds it and in the runtime's pending map where the readiness activation
does. A second waiter replaced the first in both: readiness resolved only the newer future, its cleanup
removed the map entry, and the older one became unreachable by the activation and by `close()` alike.
Reproduced as a coroutine still pending after a record had been buffered, readiness resolved, and the
subscription closed.

**Fixed by refusing a second waiter**, with a non-retryable `ConcurrentStreamConsumerError`, on the
single-subscription path and inside `merge()` — which checks every member before registering any, so a
refused merge leaves no wait registered and blocked with no coroutine behind it.

The refusal is on the *waiter* rather than on the iterator, and that is the substantive part of the fix.
An iterator-level claim would also refuse working code: breaking out of an `async for` leaves the
generator suspended at its `yield` rather than closed, so it cannot be told apart from a live second
consumer. Refusing at the wait catches every genuinely concurrent case anyway, because two coroutines
can only be inside `_iterate` at once while one is suspended, and the only suspension point there is the
wait. Both halves are asserted: the refusal, and that break-then-iterate-again still works.

Spec: `spec/python-runtime.md`, "A subscription has one consumer". Decision: ADR-037.

## P2 — A `Stale` retry that discovers `RunNotFound` keeps the watcher

**Confirmed.** `_retry_stale` returned only whether some attempt was accepted and discarded every other
answer, so control returned to `_report_ready` with the original `Stale`: the owed wake went out, and
the cleanup branch guarded by `RunNotFound` was skipped. Reproduced with a notifier answering `Stale`
once and `RunNotFound` thereafter — one wake sent, and the subscription still in `_runs`.

**Fixed by returning the result the retries ended on** and acting on that. `RunNotFound` also ends the
retries rather than using them up: the Run cannot come back, and each further attempt only delays the
wake the buffered record still needs.

Spec: `spec/core-lang-protocol.md`, the readiness-result table.

## Defects the fixes themselves introduced

The cancellation fix above was reviewed in turn, and left the boundary immediately before the one it
closed still ambiguous. Case 77 is its test set. Validating *that* fix then found three more, all in
the recovery it added rather than in the outcome; they are the section after it, and case 78.
Validating those corrections followed the operation through one more interrupted recovery and found
the final defect below; case 79 is its test.

### P1 — An append that reports no outcome was read as an append that failed

**Confirmed, as two DATA records.** The fix handled cancellation delivered *after* `append()` returned.
It kept the assumption underneath the old behaviour: that an `append()` which did not return had not
happened. That is true of an in-process backend and false of a remote one — a backend commits on its
own side and only then answers, and the Redis provider runs its atomic script server-side and receives
the result in a separate client-side step. Cancelled or disconnected between the two, the producer
raised a bare `CancelledError` or a bare `ConnectionError` carrying no offset and no record, and the
caller's only moves were both wrong: retrying `publish()` drew a fresh sequence number and appended the
value a second time, while not retrying could leave a durable record no wake was ever sent for.
Reproduced with a backend that stores through `MemoryStreamBackend.append()`, signals that it has
committed, and then blocks: the cancelled publish escaped bare, and the retry left
`[(0, DATA), (1, DATA)]`.

**Fixed by giving the window its own outcome.** `AppendNotAcknowledgedError` carries the exact record —
byte-identical, still holding its `(session_id, sequence)` — and `resolve_append()` re-appends *that*
record, which ADR-020 already makes a no-op returning the original offset if it landed and an ordinary
append if it did not. One call is therefore right without knowing which history happened. Until it is
settled the stream refuses further appends from that producer, because reporting the state and leaving
`publish()` available still permits the duplicate: the caller's problem was never a shortage of
information, it was that the obvious next call was the wrong one.

`AppendConflictError` stays a refusal — it says the key was used with different bytes, so the record
did not land and re-appending it would raise identically — and that is the only exemption the backend
contract can support.

The existing "cancellation before the append is still a cancellation" test moved with the boundary. It
now blocks in the payload codec rather than inside `append()`, because that is where cancellation is
*knowably* pre-append: a backend that blocks before storing and one that blocks after storing are the
same thing from the producer's side, and a test that asserted a bare cancellation for the first would
have been asserting the defect.

Spec: `spec/wake-signal.md`, "The append itself has an acknowledgement window". Decision: ADR-038,
which also records why probing the backend and auto-resolving were rejected.

### P1 ×3 — the recovery for that outcome was under-bound

Validating the fix above found three more, all in the recovery rather than in the outcome. The
diagnosis they share: the producer kept the unsettled *record* when what it owed was the unsettled
*operation*, and `resolve_append()` checked the record's session id when the thing that makes a
re-append a no-op is the whole of `(stream, session, sequence, bytes)`. Case 78 is their test set.

**The refusal described the wrong call.** With an append unsettled, a later `publish("b", wake=False)`
was refused with an error naming the older record but carrying the *refused* call's `wake` and
`lease`, and `cancelled` defaulted back to `False`. A caller following that error's own instructions
settled a record that owed a Signal with no wake at all: `records [(0, DATA)] signals 0`. Fixed by
storing the operation — stream, record, wake, lease, cancelled — so the first raise and every later
refusal report the same canonical recovery. `resolve_append()`'s `wake` and `lease` now default to
what the interrupted call was doing rather than to the method's own defaults, which is the same
defect one level down: a `wake=False` fence must not acquire a Signal from its recovery.

**The recovery was not bound to its stream.** A `StreamRecord` names its session and sequence but not
its stream, and idempotency is scoped per stream, so `topic_b.resolve_append(error_from_a.record)`
appended a second copy onto B — `tokens [(0, DATA)] events [(0, DATA)]` — while leaving A unsettled
and therefore still blocked. Fixed by carrying `stream_key` on the error and resolving against *this*
topic's outstanding entry, matched on the full record rather than on the idempotency key, so different
bytes under that key are refused too and do not clear the entry.

**The recovery was not bound to its producer.** The session check passed on a *replacement* producer
built with the same stable session id, whose sequence and wake counters are back at zero. Its next
publish reused sequence 0 (`AppendConflictError`), and its recovery wake re-derived the request ID an
earlier, different unparked wake had already used — `request_ids_collide True` — so the server would
deduplicate it away and leave the record unannounced. The contract is now inverted explicitly:
recovery is in-process, for the instance that still owes the append. A producer that is gone recovers
the way an Activity retry already does, re-running the same calls in the same order, which re-derives
the same keys and advances the counters because the calls actually ran. The refusal says so.

All three checks run before the backend is touched, and each names which binding failed.

### P1 — a re-interrupted recovery kept two contradictory operations

**Confirmed, as one durable DATA record and no Signal.** The first `publish()` used `wake=False`,
committed and lost its response. Its `resolve_append()` deliberately changed the recovery to
`wake=True` with a new lease, then also committed and was cancelled before answering. The second
error correctly reported the recovery attempt's wake, lease and cancellation, but the producer's
retained entry was still the original publish. A later refusal therefore described the newer state
while a defaulted `resolve_append()` silently read the older one and returned success without the
Signal it owed: `records [(0, DATA)] signals 0`.

**Fixed by making replacement return the canonical operation used for both storage and the raised
error.** A repeated unknown outcome replaces the matching entry with the newest attempt's effective
wake and lease. Cancellation is combined with the earlier state rather than replaced, because once
delivered it remains for the caller to honour after settlement. Every later refusal and defaulted
recovery now observes that same object. Case 79 commits the initial append and two recovery attempts,
cancels the first recovery, verifies the next one's connection failure cannot erase that
cancellation, checks the intervening refusal's recovery fields, resolves from that refusal with no
overrides, and observes one record and exactly one Signal.

Spec: `spec/wake-signal.md`, "The append itself has an acknowledgement window". Decision: ADR-038.

## What this round did not change

- **No Core (Rust) code.** All five findings were Python-side, and the review traced the Core state
  machine, marker, wake interception, retention, park, finalization, rollover and replay paths without
  finding anything at the same confidence bar.
- **No new taxonomy row.** `ConcurrentStreamConsumerError` sits outside the four for the same reason
  `ExternalStreamCapacityError` does: it describes Workflow code asking for something the design does
  not offer, not a stream or a converter misbehaving at read time.
- **No new branch for cancellation.** It was folded into the existing durable-but-unacknowledged
  outcome rather than added beside it, so a caller already handling that outcome needs nothing more for
  the cancelled form of it. `publish()` did gain a third outcome, but from the finding below rather
  than from this one, and for a state whose recovery is genuinely different.

## A note on the suite

`test_shutdown_with_no_open_task_hands_the_run_to_another_worker` fails intermittently when the
end-to-end files run together, with a `KeyError` raised by CPython's import lock inside the sandbox
importer. It was confirmed to fail the same way **with this round's changes stashed**, so it is
pre-existing and load-dependent rather than a regression — the same check `third-review.md` had to make
about the empty-stream replay flake, and for the same reason: a full-suite failure that passes in
isolation says nothing until it has been run at HEAD as well.
