# ADR-044 — Output stage tokens and exact History floors prove commit

**Status:** Accepted · **Affects:** output staging, reconciliation · **Spec:**
`spec/backend-contract.md`

## Context

Workflow output must be staged before its Workflow Task is reported, but it may become visible only
after that task commits. A Worker can die on either side of the server acknowledgement. Speculative
Workflow Tasks make Scheduled and Started event IDs unsuitable identities: discarded attempts may
leave neither event durable, and later attempts may reuse the IDs.

## Options

**A. Use Workflow Task event IDs as the stage identity.** Reuse collides across speculative
attempts.

**B. Resolve an old pending stage after a timeout.** Absence of a marker is not proof while the
task outcome is still undecidable.

**C. Mint a unique token per attempt and reconcile from the exact durable History predecessor.**

## Decision

**C.** The Worker mints one opaque `stage_token` per Workflow Task staging attempt and reuses it only
for retries of that exact stage. Core supplies `history_floor_event_id`, the event immediately before
the producing task's Scheduled event in the ordered History view; staging is refused if that exact
floor is unavailable.

A resolver scans strictly above the floor. The exact token in an external-stream marker commits the
stage. Otherwise the first durable Workflow Task completed, failed, or timed-out boundary, or
Workflow closure, aborts it. With neither fact the stage remains pending. History loss is an
integrity failure, never an inferred abort.

## Consequences

- Byte-identical retries and reused speculative event IDs cannot alias another attempt.
- Workers reconcile opportunistically; `ExternalOutputStreamClient` performs the same History
  predicate lazily at a pending head.
- Repeated stage, commit, abort, and reconciliation operations are idempotent. Reusing a token with
  another immutable manifest is a conflict.
- There is no timeout-based escape from an undecidable stage. Operators restore the needed History
  or provider metadata, or terminate the affected execution.
