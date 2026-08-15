# ADR-002 — A cursor is a position boundary, not a record identity

**Status:** Accepted · **Affects:** P2, P3, P5 · **Spec:** `spec/backend-contract.md`

## Context

A subscription must persist "where I got to" so a later Run, or a replay, resumes correctly. The
representation has to work for a consumer sitting at the current tail, where the next record does
not exist yet.

## Options

**A. The cursor names the next record to deliver.** Intuitive; resume is "read from the cursor".

**B. The cursor is a position boundary** — `BEGINNING | AFTER(last_consumed_offset)`.

## Decision

**B.**

A is not implementable. After a consumer reads the current tail, the next record does not exist and
therefore has no offset. Redis offers no way to name the ID the next `XADD` will produce, so a
consumer parked at the tail has nothing to persist as a next-to-deliver cursor.

```text
cursor := BEGINNING
        | AFTER(last_consumed_offset)
```

`BEGINNING` is the provider's beginning-of-stream boundary and need not be the offset of any real
record. `AFTER(x)` names the boundary immediately following the record at offset `x`, whether or not
a record after `x` exists yet.

A provider may represent the boundary as `(offset, inclusive | exclusive)` instead. What it may not
do is require the cursor token to be the offset of a record that does not exist yet.

## Consequences

- Two distinct provider primitives, not one: an **exclusive** watch strictly after a boundary
  (Redis `XREAD BLOCK`, or from sentinel `0-0` for `BEGINNING`), and an **inclusive** range read over
  an explicit `[first, last]` pair (Redis `XRANGE`).
- **Replay never reads "from the cursor".** It reads explicit recorded ranges; the answer is already
  in the marker.
- Offsets are compared by the provider's ordering rule, not lexically. Redis IDs compare as numeric
  `(ms, seq)` tuples — string comparison breaks as soon as the millisecond component changes width.
- The conformance suite must contain a case that parks a consumer at the tail, appends a record whose
  ID could not have been predicted, and resumes; a backend requiring a nameable next ID fails it.
