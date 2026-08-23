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

**B. A per-Run ledger of owed removals**, keyed `(stream key, wait_id)`, with lifecycle-triggered
drains.

**C. A background retry task** owned by the manager, retrying on its own schedule.

**D. B, but drain unconditionally** — delete the key rather than re-reading it first.

## Decision

**B and C, using provider-atomic compare-and-delete.**

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
the provider atomically removes an intent only when the `park_generation` and Run ID both still
match what was recorded, and the manager forgets the entry on either a removal or a mismatch.

B alone was the original decision, with C deferred until cleanup had an owner that teardown awaits
and the provider contract could close the read/delete race. Both prerequisites now exist. The
manager strongly holds one retry task per Run, retries with bounded exponential backoff, and cancels
and awaits those tasks during Worker shutdown. The conditional provider operation makes the
autonomous retry safe across a Continue-As-New successor replacing the same key. Lifecycle drains
remain eager fast paths, but another Workflow or Core event is no longer required: a backend that
recovers while the Run stays cached and idle is enough to retire the stale intent.

## Consequences

- **A ledger entry means "a removal was decided on and has not been confirmed", not "an intent
  exists".** That is what makes draining safe from any holder of the Run's park lock rather than
  only from the path that recorded it. The entry also retains nothing a teardown could invalidate —
  no watcher, no buffer, no connection beyond the backend the removal must go through.
- **The entry is made before the backend call**, not when one fails. A call that never returns —
  the backend raised, the task was cancelled mid-await — owes the removal exactly as an error does,
  and the subscription it was reached through is frequently dropped in the same breath.
- **Reading an inherited intent is what records it.** It is the only mirror such an intent ever
  gets; without it, the resolve, the rollback and the cancellation all go on short-circuiting on an
  empty `installed_park_generation` for it.
- **A fresh install at the same key supersedes what is owed there**, or a removal recorded against a
  generation that has since been overwritten would be retried against the park now sitting behind
  it.
- **Eviction ends cache ownership, not cleanup ownership.** The manager retains the ledger, its
  park lock and its retry task until the debt is retired. Shutdown cancels and awaits retry tasks
  within its grace period, and then makes one last bounded pass at what is still owed — a loop
  cancelled during its backoff has an attempt left in it, and eviction stands aside while shutting
  down rather than waiting on a lock a stuck loop may hold.
- **An autonomous retry has to be bounded per call, not per attempt.** It is the only thing left
  holding a removal once the inline attempts are spent, so a backend call that hangs rather than
  raises does not cost one attempt — it ends the mechanism, silently, and holds the Run's park lock
  while it does. Every park-lock hold outside an activation is therefore time-bounded, and a hold
  that runs out of time is a failed attempt. Holds an activation takes are not, because that wait is
  the backend exposure the park handshake already owns.
- **Cleanup announces what the stale intent silenced, and that obligation is not the
  subscription's either.** Wakes sent while the intent was installed named a generation Core
  discards, so removing it stops the suppression without delivering the record it suppressed. With
  the wait still registered, local readiness announces it — the reconciliation's first-time success
  included, which is the *ordinary* path for an inherited intent and the one that never touches the
  ledger. With the wait gone, nothing about the record is knowable from here: the buffer went with
  the subscription that the close or the eviction dropped, and a remote producer's append into the
  suppression window was never visible from this Worker at all. So a confirmed removal with no
  subscription behind it sends one *unconditional* unparked wake. Conditioning it on a registered
  subscription is the same mistake this ADR is about, one level up: it keeps the leak fixed and puts
  the record loss straight back, for exactly the intents whose owner is gone. The cost is an empty
  Workflow Task when nothing had arrived; the alternative is silence when something had.
- **No wake this Worker sends names an intent it has already decided to remove.** A ledger entry
  says that generation is one Core has discarded, so a wake composed from it is a Signal the service
  accepts and Core ignores — a success the sender reports and the Workflow never sees. The manager
  therefore decides what a wake names, rather than the sender reading `current_park_generation`
  itself, and answers "unparked" both for an entry in the ledger and for a handoff whose intent was
  just retired. That is what makes the shutdown sweep's accounting true: the sweep wakes the
  subscriptions of a Run whose stale intents the final removal pass has not reached, and a counter
  that reported a clean shutdown on the strength of an obsolete-generation Signal was reporting a
  handoff nobody received.
- **The sweep's wake and the removal's handoff are one obligation.** Both are the unparked wake for
  the same wait, and the sweep runs first, so a successful sweep wake is recorded on the ledger entry
  and the final pass does not send it again. Recorded only on success — a wake the sweep could not
  get acknowledged leaves the handoff still owed.
- **A cleanup handoff is work of its own, and shutdown waits for it.** Every drain reaches it holding
  the Run's park lock, under a budget that bounds one single-key backend call rather than a Signal
  with retries behind it. The last of these is created by the final owed-removal pass, which nothing
  runs after, so shutdown awaits them within what is left of its grace period and counts what the
  bound cuts off. A wake merely scheduled as the process exits is a wake never sent, and worse than
  one reported lost.
- **The provider reports three outcomes, and two of them mean "gone".** A delete that commits and
  loses its reply leaves a retry meeting a key it cleared itself, so *absent* and *mismatch* cannot
  be one answer: the first ends the suppression and may owe an announcement, the second is a park to
  leave alone. Collapsing them costs a record, not an intent.
- **Discovery is retried like the removal, and bounded by the subscription.** The ledger holds an
  identity, so a read that never succeeds records nothing at all — no entry, no task, no owner. The
  reconciliation therefore keeps retrying its read with backoff while this Worker holds the
  subscription, and hands over to the ledger as soon as one succeeds. It stops at cancellation
  because the justification for removing what is at that key is holding the Run; without it, the
  intent found there could be a park another Worker is in.
- **The provider closes the cross-Run race.** Two Runs of a chain hold different park locks and may
  be held by different Workers, so nothing process-local orders a predecessor's drain against a
  successor's install. `remove_park_intent_if_matches` compares and deletes atomically.
- Tests assert autonomous recovery after both resolve-time and inherited-intent failures, cleanup
  continuing after eviction, shutdown owning an in-flight retry, and a conditional removal leaving
  a replacement intent intact. Three more cover the mechanism itself: a removal call that hangs
  rather than raises does not end the retry loop or keep the park lock, shutdown makes an attempt a
  cancelled backoff would have skipped, and a confirmed removal re-announces the record its intent
  had silenced. Four cover the handoff once the subscription is gone: a removal confirmed after
  eviction still sends the unparked wake, the sweep does not compose one from an intent it has
  decided to remove, a removal the final pass confirms carries its own handoff and shutdown does not
  exit before the Signal lands, and the sweep's wake is not sent a second time by that pass.
