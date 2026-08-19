# ADR-031 — An owed park-intent removal belongs to the Run, not to the subscription

**Status:** Accepted · **Affects:** P8, P2b, P9 · **Spec:** `spec/wft-lifecycle.md`

## Context

A park intent is durable backend state. Everything that knows one exists is not: the
`installed_park_generation` a Worker keeps is a mirror of a park *that Worker* confirmed, and an
intent inherited from an evicted Run or a departed Worker is mirrored nowhere at all. A removal
keyed on the mirror alone therefore does nothing — silently, and by returning early — for exactly
the class of intent that no other mechanism can reach.

Every path that removes an intent reaches it through a `Subscription`: the resolve iterates the ones
the manager still holds, the rollback walks the ones the attempt just installed, the cancellation
works on the one it is dropping. So a removal that fails has nowhere to be recorded that survives
the next step. The close drops the subscription, the eviction drops the Run's map, and a failure
written onto either is a failure written onto the object about to be discarded.

The two of those are one problem: cleanup responsibility is being stored on the thing whose
disappearance is what made cleanup necessary.

## Options

**A. Record the failure on the `Subscription`** and retry the next time that wait is registered.

**B. A per-Run ledger of owed removals**, keyed `(stream key, wait_id)`, drained by that Run's next
park, resolve, registration, or eviction.

**C. A background retry task** owned by the manager, retrying on its own schedule.

**D. B, but drain unconditionally** — delete the key rather than re-reading it first.

## Decision

**B.**

A is the shape that produced the defect. "The next registration" is a coincidence of eviction rather
than a mechanism: registration happens once for the life of a cached Run, so a Run that stays cached
never provides another one, and the Run that most needs the removal — cached and blocked on
something other than this stream — is precisely the one that never registers the wait again. For a
cancelled wait it is not even a coincidence; a closed wait is never registered again by definition.

D is unsafe in a way that is worse than the leak it would be closing. `wait_id` restarts at 1 in a
Continue-As-New successor while the stream key does not change, so an entry a predecessor Run left
behind can name the key of an intent a **successor** installed and is currently parked on. Deleting
that unparks a Run whose park is real, whose producers therefore send nothing, and whose Workflow
Task no wake will ever create. A leaked intent costs wakeups for one stream; this costs the Run. So
a drain re-reads the intent and removes it only when the `park_generation` and Run ID it carries
both still match what was recorded, and forgets the entry otherwise.

C is deferred rather than rejected, and it is the honest completion of this decision: it is the only
option that retries with no Workflow or Core event behind it, which is what a backend that is
unavailable for the whole bounded retry window and recovers into an idle cached Run needs. It is not
taken here for two reasons. It needs an owner that teardown awaits, or a Worker exits with a retry
in flight and the "last chance on this Worker" property below stops being true. And an autonomous
retry is the retry with the widest window between its read and its delete, which is the window the
generation check narrows but does not close — so it wants a compare-and-delete in the provider
contract first, not after.

## Consequences

- **A ledger entry means "a removal was decided on and has not been confirmed", not "an intent
  exists".** That is what makes draining safe from any holder of the Run's park lock rather than
  only from the path that recorded it, and it leans on removal being idempotent
  (`spec/backend-contract.md`): a drain that repeats a removal that in fact succeeded costs a round
  trip. The entry also retains nothing a teardown could invalidate — no watcher, no buffer, no
  connection beyond the backend the removal must go through.
- **The entry is made before the backend call**, not when one fails. A call that never returns —
  the backend raised, the task was cancelled mid-await — owes the removal exactly as an error does,
  and the subscription it was reached through is frequently dropped in the same breath.
- **Reading an inherited intent is what records it.** It is the only mirror such an intent ever
  gets; without it, the resolve, the rollback and the cancellation all go on short-circuiting on an
  empty `installed_park_generation` for it.
- **A fresh install at the same key supersedes what is owed there**, or a removal recorded against a
  generation that has since been overwritten would be retried against the park now sitting behind
  it.
- **Eviction is the last chance a Run gets on this Worker.** The ledger and the park lock both go
  with the Run, so what is not retired then is left to the next Worker's registration-time
  reconciliation — which is why that reconciliation must record what it reads.
- **The read/delete window is narrowed, not closed, across Runs.** Two Runs of a chain hold
  different park locks and may be held by different Workers, so nothing process-local orders a
  predecessor's drain against a successor's install. Closing it requires a conditional delete —
  remove only if the generation and Run ID still match — as a provider obligation.
- A test must assert that a removal survives the close *and* the eviction of the subscription that
  owed it, and separately that a drain meeting a replaced intent forgets the entry rather than
  removing what it found.
