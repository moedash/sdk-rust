# ADR-047 — Pending output is a topic ordering barrier

**Status:** Accepted · **Affects:** output providers and clients · **Spec:**
`spec/backend-contract.md`

## Context

A Workflow batch is durable in the provider before Temporal decides whether its producing task
commits. Direct Activity or external-process output can append while that decision is pending.
Skipping unresolved positions would expose later data ahead of output that may still commit.

## Decision

A pending Workflow stage occupies provider order but is not readable. `read_output_after()` returns
only the committed prefix before the first unresolved stage and identifies that pending barrier.
Readers never pass it. `ExternalOutputStreamClient` resolves the barrier with ADR-044's built-in
History predicate, then retries the provider read.

Direct `ExternalOutputStreamProducer` records are immediately committed singleton appends and may
share a topic with Workflow batches. Their retry identity remains `(session_id, sequence)`.

## Consequences

- If the pending stage commits, its records precede direct records already placed behind it. If it
  aborts, it yields nothing and a retried Workflow Task may append after those direct records.
- The guarantee is per-topic provider order, not a deterministic total order between independently
  running Workflow and Activity producers.
- There is no atomic snapshot or order across topics. Applications needing one order publish
  envelopes to one topic.
- `tail()` returns the readable committed boundary and cannot cross a pending head.
