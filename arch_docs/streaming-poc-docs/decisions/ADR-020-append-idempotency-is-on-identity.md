# ADR-020 — Append is idempotent on identity, not on key alone

**Status:** Accepted · **Affects:** P2, P3, P6 · **Spec:** `spec/backend-contract.md`

## Context

Producer append must be safe under Activity retry, so it is keyed by `(session_id, sequence)`. A
retried attempt reusing the key must not create a second record. But a nondeterministic Activity may
retry with *different* bytes under the same key.

## Options

**A. Idempotent on key.** Any reuse of `(session_id, sequence)` is a no-op returning the original
offset.

**B. Idempotent on key and identity.** Reuse with byte-identical content is a no-op returning the
original offset; reuse with different bytes is an **error**.

## Decision

**B.**

Under A, a nondeterministically retried Activity silently aliases two different writes to one offset.
Replay cannot detect that — the offset is present, the range validates, the count matches — so the
Workflow observes bytes that no longer correspond to what the producer last intended. If it is ever
noticed, it is misdiagnosed as backend corruption.

Making the mismatch an error surfaces the real problem, which is a producer that is not deterministic
across retries.

## Consequences

- Providers must store enough to compare content, not just the key.
- **The producer side owes the key its stability**, and that is a constraint on when the sequence is
  drawn, not only on where the session ID comes from: at the call, before the payload's encode is
  awaited. Drawn afterwards, the number follows the codec's completion order, which is external I/O
  and not reproducible across attempts — so under B two concurrent publishes that complete the other
  way round on a retry hit exactly the "same key, different bytes" error this record makes an error,
  and the producer that earns it was deterministic. `spec/backend-contract.md` carries the rule.
- The conformance suite must include both directions: reusing a `(session_id, sequence)` pair with
  byte-identical content is a no-op returning the original offset; reusing it with different bytes is
  rejected as an error.
- Only successfully appended records may trigger wakeup.
- Exactly-once producer execution remains a **non-goal**; the backend adapter provides idempotent
  append semantics instead.
