# ADR-037 — A subscription has one consumer, and the refusal is on the waiter

**Status:** Accepted · **Affects:** P9, P21 · **Spec:** `spec/python-runtime.md`

## Context

`__aiter__` returns a new async generator on every call, but the generators are not independent views:
the cursor, the readiness future, and the blocked flag all belong to the subscription. In particular
the readiness future is a single slot in two places at once — on the subscription, where `close()`
finds it, and in the runtime's pending map keyed by wait id, where the readiness activation finds it.

A second coroutine blocking on the same subscription therefore did not queue behind the first, it
replaced it in both places. Readiness resolved only the newer future; its `finally` removed the map
entry and cleared the field; and the older future became unreachable by the readiness activation and
by `close()` alike. That coroutine is stuck for the life of the Run — and because the blocked flag is
shared too, the runtime can report the wait as no longer blocked while a coroutine is still sitting on
it, so Core may not even be retaining a Workflow Task on its behalf.

Two consumers of the same *stream* is a supported shape and already has an answer: subscribe twice.
Delivery is a broadcast (ADR-021), so each wait gets every record and keeps its own cursor.

## Options

**A. Support several waiters per subscription**, with a list of futures resolved together.

**B. Refuse a second *iterator*** — claim the subscription when `_iterate` starts, release it when the
generator finishes.

**C. Refuse a second *waiter*** — refuse at the point a coroutine tries to block on a wait that
already has one blocked on it.

**D. Leave it.** Document single-consumer and rely on callers.

## Decision

**C.** `ConcurrentStreamConsumerError` — non-retryable, for the reason
`ExternalStreamCapacityError` is — is raised when a coroutine tries to block on a subscription whose
readiness future is already held, on both waiting paths: the single-subscription one and `merge()`.
`merge()` checks every member before registering any, so a refused merge leaves no wait registered and
blocked with no coroutine behind it.

A makes the *stranding* go away and leaves the rest. Two coroutines would still share one cursor and
one blocked flag, so records would interleave between them in an order nothing records, and the
`wait_id`-ordered interleaving that makes `merge()` replay would have no equivalent here. It is a
feature nobody asked for, standing in for one that already exists.

B is the tempting shape and refuses correct code. Breaking out of an `async for` leaves the generator
*suspended at its yield*, not closed — CPython finalizes it later, at collection — so an
iterator-level claim cannot tell "took a few records and came back" from "two live consumers", and the
first is ordinary Workflow code that works today.

C catches every case B would, because two coroutines can only be inside `_iterate` at once while one
of them is suspended, and the only suspension point in there is the wait. A generator suspended at its
`yield` is not inside `_iterate` at all — it is between `__anext__` calls, with the caller's own code
running. Where records are already buffered, two `__anext__` calls run to completion one after the
other with no interleaving and no shared future, which is harmless and is left alone.

D was the state the review found, and the failure it produces is the worst shape available: a Workflow
permanently stuck on a future nothing can resolve, with no error, no metric, and a blocked-set snapshot
that says the wait is not blocked.

## Consequences

- **Iterating the same subscription again, after the previous consumer has stopped, is allowed** and
  resumes where it left off. That is the whole reason the refusal is not on the iterator.
- The error is deterministic and raised on the Workflow thread, so it reproduces under replay and
  fails the Workflow rather than looping a Workflow Task retry that meets the same code every time.
- The refusal is *not* a general concurrency check: two `__anext__` calls that both find records ready
  are served. What is refused is a second *blocked* waiter, which is the only state that loses one.
- A test must block one consumer, start a second, and assert the second is refused while the first
  still receives the next record and is resumed by `close()`. It must also assert that
  break-then-iterate-again on one subscription still works, or the guard passes by refusing everything.
