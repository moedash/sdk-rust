# ADR-021 — Delivery to multiple subscriptions is broadcast, not work-sharing

**Status:** Accepted · **Affects:** P9, P21 · **Spec:** `spec/wft-lifecycle.md`

## Context

One Workflow may hold two subscriptions to the same stream name. Two readings are possible: each sees
every record, or the two split the records between them.

## Options

**A. Work-sharing.** The two subscriptions compete for records; each record goes to one of them.

**B. Broadcast.** Each subscription sees every record from its own cursor.

## Decision

**B.**

Work-sharing requires a shared cursor that both subscriptions advance, which means a single mutable
position raced between two coroutines inside one Run. That is not reproducible on replay without
recording the arbitration of every record, which defeats the run encoding (ADR-006) — and the
arbitration itself has no deterministic rule to reproduce.

Competing consumers means separate Workflows or separate Runs, each with its own subscription and
cursor, not two subscriptions racing for one cursor inside a single Run.

## Consequences

- Cursors are per **subscription**, not per stream. Two subscriptions to one stream name have
  independent wait IDs, independent cursors, independent park intents keyed by
  `(stream key, wait_id)` (ADR-012), independent `wait_generation`s, and independent entries in the
  annotation header and the Continue-As-New continuation state.
- Cancelling a subscription commits its cursor at the next marker and removes it from the wait set.
- Re-subscribing to the same stream name creates a **new** subscription with a new wait ID; it does
  not resume the cancelled one.
- Resumption across Continue-As-New is by wait ID, which is why re-ordering `subscribe()` calls is a
  nondeterministic change.
- Work-sharing between subscriptions inside one Workflow is an explicit **non-goal**.
