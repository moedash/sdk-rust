---
doc_id: EWS-SPEC-INDEX
status: normative-pre-production
audience: [implementers, coding-agents, reviewers]
---

# External Workflow Streams — normative reference

This directory is the canonical implementation contract for accepted External Workflow Streams
behavior. It contains exact lifecycle rules, protocol messages, provider operations, annotation
formats, runtime ownership, and failure classification. The human-facing [`guide/`](../guide/README.md)
summarizes these documents but does not override them.

## Structured lookup index

| Document ID | File | Canonical for | Related ADRs |
|---|---|---|---|
| `EWS-SPEC-PUBLIC` | [`public-surface.md`](public-surface.md) | Public names, handle roles, Worker configuration, stream identity, coexistence boundary | 001, 019, 021, 037, 040, 048 |
| `EWS-SPEC-BACKEND` | [`backend-contract.md`](backend-contract.md) | Provider capabilities, cursor and key semantics, immutability, staging, barriers, park operations, producers, fences, retention | 002, 003, 012, 019, 020, 040, 044, 047 |
| `EWS-SPEC-WFT` | [`wft-lifecycle.md`](wft-lifecycle.md) | Workflow Task admission, retention, timers, parking, output/park arbitration, shutdown, eviction, wake durability | 004, 009, 016, 017, 021, 030–032, 043, 045 |
| `EWS-SPEC-ANNOTATION` | [`annotation-format.md`](annotation-format.md) | Input replay grammar, output manifests, segmentation, byte budgets, replay checks, continuation state | 005–007, 018, 022, 027, 033, 039, 046, 048 |
| `EWS-SPEC-PROTOCOL` | [`core-lang-protocol.md`](core-lang-protocol.md) | Completion commands, activation jobs, marker envelope, readiness calls, Core state | 008, 013, 041 |
| `EWS-SPEC-PYTHON` | [`python-runtime.md`](python-runtime.md) | Sandbox boundary, decoding, output staging, cursor positions, delivery budgets, merge, close, job dispatch | 010, 011, 026, 028, 029, 034, 035, 037 |
| `EWS-SPEC-WAKE` | [`wake-signal.md`](wake-signal.md) | Signal envelope, request IDs, interception, wake cycles, producer send and recovery | 023, 025, 036, 038, 042 |
| `EWS-SPEC-FAILURE` | [`failure-taxonomy.md`](failure-taxonomy.md) | Failure classes, completion behavior, error types, metrics, operator response | 014, 015 |

The corresponding decision records are indexed in [`../decisions/README.md`](../decisions/README.md).
Required observable behavior is indexed in [`../required-tests/`](../required-tests/).

## Retrieval procedure for implementers and coding agents

1. Select the document that owns the behavior from the table above. Do not infer a cross-component
   rule from one implementation file.
2. Search that document for the exact protocol symbol, state name, provider operation, error type, or
   lifecycle boundary involved.
3. Follow the linked ADR before changing the rule. The specification states current truth; the ADR
   records why competing designs were rejected.
4. Check the required-test lists for the behavioral obligation and its mapped test before changing
   either implementation or documentation.
5. Consult [`../verification-hazards.md`](../verification-hazards.md) before treating a failing or
   passing integration result as evidence.

## Precision conventions

- `status: normative-pre-production` means the document is authoritative for the implemented design,
  while the feature's production-readiness gate remains open.
- `doc_id` is a stable lookup key. File headings are descriptive and may become more readable without
  changing that identity.
- Protocol names, state names, field names, operation names, and error types are exact identifiers.
- Tables describing outcomes or legal combinations are exhaustive unless explicitly labelled as an
  example.
- A failure consequence explains why a rule is load-bearing; it is part of the contract rather than
  background narrative.
- Candidate behavior belongs under [`../proposals/`](../proposals/) and is never silently mixed into
  these specifications.

Some ADR headers retain `P…` and `C…` labels from the implementation plan that preceded document
consolidation. They are historical annotations, not active requirement identifiers or links. Use the
ADR number, stable specification `doc_id`, canonical section, and required-test mapping for current
traceability.

## Source-of-truth rule

Each normative fact has one home. Other specifications link to that home rather than restating it.
Guide diagrams intentionally simplify the system and must always link back here. If a summary,
proposal, ADR rationale, test description, and specification differ, resolve them in this order:

1. accepted specification;
2. accepted ADR for design intent;
3. required test for observable coverage;
4. human guide or proposal summary.

An inconsistency is a documentation defect; it is not permission to choose the most convenient
interpretation.
