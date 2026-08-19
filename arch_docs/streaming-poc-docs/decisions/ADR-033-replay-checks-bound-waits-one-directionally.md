# ADR-033 — Replay checks that every bound wait was recreated, in one direction only

**Status:** Accepted · **Affects:** P5, P10b, P13 · **Spec:** `spec/annotation-format.md`

## Context

Replay joins a recorded delivery to a subscription by `wait_id` and nothing else, and `wait_id` is a
counter over `subscribe()` call order. Two checks reason from what the code did: each registered
wait is compared against its recorded binding, and a replay that ends holding a recorded delivery
nobody took fails. Neither says anything about a recorded wait that the code no longer creates,
because both start from the subscriptions that exist.

A binding is written for a subscription whether or not anything was delivered through it: the first
observation must carry provider identity, stream key, and start cursor even for a stream that stayed
quiet for the whole Workflow Task. A binding with no runs behind it is therefore the ordinary shape
of a quiet `subscribe()`, not an edge case — and it is invisible to any check that reasons from
deliveries.

## Options

**A. Nothing further.** The binding comparison and the unconsumed-delivery check are the whole of
it.

**B. `bound ⊆ registered`,** checked once after the last segment of the marker.

**C. Set equality** — every bound wait registered *and* every registered wait bound.

**D. B, but checked before each segment**, alongside the unconsumed-delivery check.

## Decision

**B.**

A leaves two removals accepted, and the second is the dangerous one. Removing the **last**
`subscribe()` on a quiet stream renumbers nothing and leaves nothing undelivered, so replay
completes against a Workflow that now holds one subscription fewer than History says it did, with
that stream's live reads never made. Removing a **middle** `subscribe()` where the later waits name
the same stream and backend renumbers every survivor down by one, so the binding comparison compares
equal for all of them and the records recorded for wait *k* are consumed by what was subscription
*k+1* — a different cursor, a different consumer, and nothing left over for the delivery check to
notice. That is the silently different stream result the failure taxonomy says must never happen.

C forbids a supported change. Replay runs the Workflow **forward past** the Workflow Task the marker
covers, and the subscriptions it makes there belong to the next marker's header, so a registered
wait with no binding is the ordinary case rather than a disagreement. Under C, adding a `subscribe()`
at the end — the one subscription change the determinism rule explicitly permits — would fail
against every marker written before it.

D is C's timing mistake without C's semantics. A wait bound by a later frame legitimately does not
exist while an earlier segment is being delivered, so the same check run per segment reports
nondeterminism against unchanged code whenever a `subscribe()` ran after the first activation of a
retained Workflow Task (ADR-027).

## Consequences

- The failure is row four of `spec/failure-taxonomy.md` — nondeterminism, naming the missing waits —
  and not integrity loss. The recorded ranges are exactly where they were written; it is the
  Workflow code that moved, and sending an operator to repair the backend would be the wrong
  instruction.
- **A closed subscription must stay in the registered set** (ADR-029). It was created, so the marker
  binds it; a close that forgot it would fail this check against a Workflow whose only unusual act
  was to finish reading a stream early.
- **One residue stays undetected**, and it is the price of permitting an addition at the end:
  inserting a `subscribe()` in the *middle* of several subscriptions to the same stream on the same
  backend renumbers them without changing any binding comparison and without changing the bound set's
  membership in the registered one. The determinism rule covers it; no check can.
- A test must replay a marker that binds a wait with **no runs at all** against Workflow code that
  creates no subscription, and require the error. A test built on a wait with recorded deliveries
  passes against option A.
