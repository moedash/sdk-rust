# ADR-003 — Structural immutability is required of every provider

**Status:** Accepted · **Affects:** P2, P17, P5, P13 · **Spec:** `spec/backend-contract.md`

## Context

Replay re-reads records from the backend rather than from History. It must detect damage to those
records — otherwise a Workflow silently observes different data on replay than it did live. The
question is what class of damage the design must detect, and what that detection costs per record.

## Options

**A. Require every provider to guarantee stored bytes cannot change.** Replay then validates
presence, count, order, and control positions only.

**B. Make it optional, with a per-record content-hash mode** for providers that cannot make the
guarantee. Replay validates hashes for those providers.

**C. Detect nothing**; trust the backend entirely.

## Decision

**A.** Structural immutability is a **registration-time requirement**, checked when a backend is
registered on the Worker (P17), not a per-provider runtime mode. A backend that cannot make the
guarantee does not satisfy the contract and is rejected before any Workflow can name it.

Redis Streams is the worked example and satisfies it: an entry can be deleted by `XDEL` or removed by
trimming, but its fields cannot be rewritten in place.

B costs one hash per record in every marker — the single encoding in the whole design that would grow
with item count — to defend against a failure mode no supported backend exhibits. Requiring the
guarantee up front is both cheaper and stricter than making it optional and paying to detect its
absence. C gives up the feature's determinism claim.

## Consequences

- Given the guarantee, replay needs to detect exactly one class of damage — **a record that is no
  longer there** — and offsets suffice. Deletion, trimming, and retention expiry are the realistic
  operational failure modes, and all are caught by the four range checks in
  `spec/annotation-format.md`.
- **Nothing in the annotation encoding is per-record.** A run costs two offsets, a count, and a sparse
  control list whether it covers ten records or a hundred thousand. This is what makes the
  single-stream marker-size claim unqualified.
- **Accepted cost, stated plainly:** if a provider silently violates immutability — a buggy custom
  backend, or out-of-band surgery on the stream — replay delivers the altered bytes as though they
  were original, and no error is raised. The risk is bounded by the registration requirement and the
  conformance suite.
- `schema_version` leads the annotation encoding, so a content-hash mode can be introduced later
  without a format break if a provider ever needs one.
- A decode failure is therefore never an integrity failure — see ADR-015.
