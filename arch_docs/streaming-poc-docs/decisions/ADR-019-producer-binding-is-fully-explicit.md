# ADR-019 — Producer binding is fully explicit, and the stream name appears once

**Status:** Accepted · **Affects:** P6a, P6 · **Spec:** `spec/backend-contract.md`

## Context

A producer — an Activity, or a plain external process — must address the right stream on the right
Workflow chain, encode payloads the consumer can decode, and append idempotently under retry. Some of
that could plausibly be inferred from ambient context.

## Options

**A. Infer what we can** from `temporalio.activity.Info` and Worker configuration; require the rest.

**B. Require every binding input explicitly**, and have the producer verify the key before its first
append.

For the stream name specifically:

**B1.** Name inside the key passed to `connect()` *and* in `topic()`, validated for equality.
**B2.** Name only in `topic(name)`; `connect()` takes the Workflow *chain* key.

## Decision

**B** with **B2**.

A cannot work for the most important field: `temporalio.activity.Info` exposes `workflow_run_id` but
**not** the first execution Run ID, so an Activity cannot derive the chain key. The Workflow passes it
to the producer explicitly — as an Activity argument, or through whatever channel a non-Temporal
producer already uses.

B1 creates two sources of truth for one value. B2 removes the possibility of disagreement instead of
validating against it, and lets one connection serve several topics.

A producer needs five things, none of which it can infer:

- **The Workflow chain key**, including `first_execution_run_id`, verified by describing the Workflow
  before the first append. Publishing under an unverified key is a configuration error, not a silent
  no-op.
- **A backend connection.** A Worker has one configured backend; a plain process constructs a
  provider directly.
- **A Temporal client**, for the wake Signal.
- **The same `DataConverter`** the consuming Workflow uses, including any codec (ADR-015).
- **A stable producer session ID and sequence**, which is what makes append idempotent under Activity
  retry.

## Consequences

- Consumer-side and producer-side handles are **distinct types**: `ExternalStreamTopic` has
  `subscribe()` and no `publish()`; `ExternalStreamProducerTopic` has `publish()` / `finish_writing()`
  and no `subscribe()`. They are not the same object passed across a process boundary.
- Activities derive a default session ID from their own identity so a retried attempt reuses it; plain
  processes **must** supply one — the API requires it rather than defaulting to a fresh random value.
- P6a's criteria include a wrong first-execution Run ID failing loudly.
- P6 stops at the append; the acknowledged-wake contract is P6b.
