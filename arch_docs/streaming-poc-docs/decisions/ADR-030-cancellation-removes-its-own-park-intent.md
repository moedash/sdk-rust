# ADR-030 — Cancelling a subscription removes its park intent itself

**Status:** Accepted · **Affects:** P8, P2b, P9 · **Spec:** `spec/wft-lifecycle.md`

## Context

A park intent is durable backend state; the record of which Worker installed it is not. Two
enforcement points already remove one (`spec/wft-lifecycle.md`): the resolve that ends a park this
Run is sitting in, and the reconciliation a Worker performs when it registers a wait it finds an
intent for. Both are driven by a registered subscription — the resolve iterates the ones the manager
holds, the reconciliation fires as one is registered.

Cancelling a subscription is the one event that removes what both of them read. Whatever it does not
do about the intent, nothing else is going to do later.

## Options

**A. Leave it to the two existing points.** Cancellation stops the watcher and drops the buffer only.

**B. Remove the intent in the cancel path**, before the watcher is stopped.

**C. Remove it at Run teardown**, along with the buffers and connections.

## Decision

**B.**

A has nothing left to work from. A resolve removes the intents recorded for subscriptions the manager
still holds, and the cancelled one is no longer among them; the reconciliation fires when a wait is
registered, and a closed wait is not registered again for the life of the Run. Reconciliation does
reach the intent eventually *if* the Run is evicted and rebuilt by a replay that recreates and
re-registers the subscription — but that is a coincidence of eviction, not a mechanism, and a Run
that stays cached to completion never provides one.

C narrows the window to the length of the Run instead of closing it, which is the same defect
measured differently: a Workflow that closes a subscription in its first activation and then runs for
a week advertises a park for a wait that ended a week earlier.

What the window costs is not a leak but a lost wake, and it is exactly the failure the intent
invariant exists to prevent. `current_park_generation` keeps answering a generation Core has
discarded; a producer that reads it names that generation in its wake Signal, and Core drops the
Signal as stale. The record is appended, the Signal is sent, the producer's `publish()` returns
successfully, and the Workflow is never woken — with the retry behaviour and the request-ID
deduplication that make this permanent described under the same invariant in `spec/wft-lifecycle.md`.

The asymmetry with the other two points is the whole argument. They can afford to log a failed
removal and be reached again later, because the subscription they work from is still registered and
both will run again against it. Cancellation is the removal of that registration, so a failure there
has no successor.

## Consequences

- **The intent goes before the watcher.** A cancellation interrupted between its two steps — by a
  crash, an eviction, a Worker exiting — leaves either a stopped watcher and a permanent intent, or a
  removed intent and a watcher that the Run's own teardown will stop regardless. Only one of those
  orders has a recoverable failure in the middle of it.
- **Removal is serialized against this Run's parking and resolving**, on the same terms as
  reconciliation, so a cancellation racing a park in flight is ordered before or after it and cannot
  remove an intent that park is in the middle of installing.
- **A removal that fails is reported and the cancellation proceeds.** Holding the cancellation open
  on the backend would keep a watcher, a buffer, and a connection alive for a wait the Workflow has
  already ended, and would not make the intent go away. Nothing downstream will observe the
  leftover — that is what this decision is about — so the log line is the only notice there is.
- A test that closes a subscription with an intent installed must assert the intent is **gone from
  the backend**, not merely that the watcher stopped; the two are separable and only one of them is
  durable.
