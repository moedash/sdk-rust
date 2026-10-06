# ADR-012 — Park intents are keyed per subscription, not per stream

**Status:** Accepted · **Affects:** P2b, P3b, P21 · **Spec:** `spec/backend-contract.md`

## Context

Parking installs an intent in the backend so a producer can discover that a consumer is waiting and
send a wake Signal. The intent must carry a cursor boundary and a park generation. Two subscriptions
in one Workflow may target the same stream.

## Options

**A. Key by stream key.** One intent per stream; simplest.

**B. Key by `(stream key, wait_id)`.** One intent per subscription.

**C. Key by `(stream key, wait_id, run_id)`.**

## Decision

**B.**

A breaks whenever one Workflow holds two subscriptions to one stream. They have independent cursors
and re-block independently, so a stream-keyed intent lets one overwrite the other's cursor, and a wake
for one silently consumes the other's generation.

C is unnecessary and harmful. `wait_id` is stable across a Continue-As-New chain and the stream key
already contains the first execution Run ID, so `(stream key, wait_id)` is unique within a chain, and
only one Run of a chain is live at a time. Including the Run ID in the *key* would let a new Run's
intent accumulate alongside its predecessor's instead of replacing it.

The current Run ID is carried as the intent's **value**, so a new Run's intent deterministically
replaces its predecessor's for the same key.

## Consequences

- Every coordination structure this design places in the backend or on the wire is keyed to include
  `wait_id`.
- A wake Signal carries `(wait_id, park_generation)` — the only generation state that reaches the
  backend or the wire. `wait_generation` never leaves the Core/lang boundary.
- The parking conformance suite must fail a stub that keys intents by stream alone, using the
  two-subscription case.
- Claims must be **leased and renewable**, so a claim abandoned by a crashed producer becomes
  takeable rather than standing for the life of the store. The lease is not what keeps such a crash
  from stranding the generation — a producer that loses the claim signals anyway
  (`spec/wake-signal.md`). A provider that cannot lease must expose observe-only semantics and let
  every producer signal idempotently.
