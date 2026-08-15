# ADR-006 — A run records both endpoints, not a start plus a count

**Status:** Accepted · **Affects:** P5, P13 · **Spec:** `spec/annotation-format.md`

## Context

The annotation encodes consecutive deliveries from one stream as a compact run. The encoding has to
let replay re-read exactly the records that were delivered, and detect it when one of them is gone.

## Options

**A. `(wait_id, first_offset, count, control_positions)`** — a start plus a length.

**B. `(wait_id, first_offset, last_offset, count, control_positions)`** — both endpoints and the
count.

## Decision

**B.**

A cannot detect a deletion inside a range. Backend offsets are ordered but **not dense**, so
`(first_offset, count)` does not determine where a run ends. A count-limited read from `first_offset`
over an open-ended range returns *some* `count` records — and if a record inside the range was
deleted, the last one it returns is a later record standing in for the missing one. That validates
cleanly and delivers the wrong data.

Recording `last_offset` closes it: replay reads the inclusive range `[first_offset, last_offset]` and
verifies

1. both endpoints are present,
2. the range contains exactly `count` records,
3. ordering is strictly increasing under the provider's comparator,
4. `control_positions` match.

A first, middle, or last deletion each fails a **different** one of those checks, and all four are
cheap.

## Consequences

- One extra offset per run — not per record — so this costs nothing asymptotically.
- These four checks are the whole of replay validation, sufficient because ADR-003 guarantees the
  bytes themselves cannot change.
- A provider that cannot support this verification over a compact range must encode the exact offset
  sequence in place of a compact run.
- The test list requires deleting the first, the middle, and the last record of a recorded range as
  three separate cases; the middle one is what a start-plus-count encoding would have passed.
