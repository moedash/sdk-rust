# ADR-034 — `merge()` rotates its start position across passes

**Status:** Accepted · **Affects:** P11, P13 · **Spec:** `spec/python-runtime.md`

## Context

`merge()` iterates several subscriptions as one wait, taking at most one record from each per pass,
in `wait_id` order. The `wait_id` order is what makes the interleaving reproduce on replay; the
one-record limit is what makes it a merge rather than a priority order.

The delivery budget, however, is charged per record handed to Workflow code and covers the merged
set as a whole (ADR-026). A pass can therefore be cut anywhere inside it, and where it is cut is
decided by where it began.

## Options

**A. Begin every pass at the lowest `wait_id`.** One record each, no cursor.

**B. A rotation cursor local to the generator**, resuming after the subscription that last took a
record.

**C. Cap the number of subscriptions a merge accepts** below the delivery budget, making A's claim
true.

**D. Record the rotation in the annotation** so replay reproduces it, or give each subscription its
own budget.

## Decision

**B.**

A fails twice, and the second failure is the general one. With more continuously ready subscriptions
than the budget has records — 257 against 256 — the pass is cut in the same place on every
activation and the last subscription is never reached at all. It is not merely served nothing:
filling returns on the spent budget before it consults the manager, so the Worker is never asked
whether that stream holds anything, for the life of the Run. And starvation is only the extreme of a
defect that needs no cap to be exceeded: any count that does not divide the budget leaves the pass
cut mid-way, so 100 always-ready waits against 256 records give the first 56 one record per
activation more than the other 44, forever, with the gap growing by one per activation and nothing
bounding it. The single-record skew A is credited with is a property *within* a pass; only resuming
where the last pass stopped makes it a property across them.

C is a limit on user code imposed to make an implementation's fairness claim true, and it does not
even make it true: a count comfortably under the budget still fails to divide it, which is the
unbounded case above. It would also have to be enforced somewhere, turning a merge over many streams
into an error rather than a slower merge.

D pays for something already free. A per-subscription budget multiplies an activation's length by
the number of streams, which is ADR-026's deadlock with a larger constant in front of it. Recording
the cursor promotes a fairness heuristic into replay state that every later change to the heuristic
would have to version — and it is unnecessary, because ask order provably cannot change what a
replayed segment yields.

## Consequences

- **The cursor is not replay state and is not reconstructed.** Under replay the budget does not
  apply, so passes are not cut where they were cut live and the rotation reaches positions the live
  run never started from. That is safe because a replay drain serves from the front of the recorded
  segment and only while the front belongs to the asking wait: a wait asked out of turn is told
  nothing and the record stays for whoever asks next. Every active wait is still asked exactly once
  per pass, so the front's owner is reached in every pass and the recorded global order comes out
  whole whatever position the pass started at.
- **"Asked exactly once per pass" is load-bearing** and is pinned by a test that replays a segment
  interleaving three waits and repeating one. Any later variant that lets a pass skip a wait — an
  early return on a spent budget, a per-subscription budget — would let a replayed segment be
  yielded in an order the live run never delivered.
- Post-replay the cursor generally sits somewhere other than where the live run left it. It steers
  only future **live** fairness, and no live schedule was ever recorded for History to contradict.
- **A control record advances the rotation**; a subscription that had nothing does not. The former
  spends its turn and the budget, because it occupies an offset inside a run. Advancing past the
  latter would cost a wait the turn it never got, which is the starvation this decision ends,
  reintroduced from the other side.
- A closed subscription leaves the set, and a cursor naming it wraps to the front rather than
  pinning the pass.
