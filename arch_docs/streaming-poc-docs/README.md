---
doc_id: EWS-DOCS-INDEX
status: pre-production
audience: [all]
---

# External Workflow Streams — documentation map

External Workflow Streams keep high-volume input and output payloads in a pluggable backend rather
than Temporal History while compact markers preserve deterministic replay. No Temporal Server
changes are required.

The documentation is deliberately split by reading mode:

| You need | Start here | Authority |
|---|---|---|
| A mental model, diagrams, guarantees, and operational tradeoffs | [`guide/`](guide/README.md) | Explanatory; intentionally simplified |
| Exact implementation behavior and every edge case | [`spec/`](spec/README.md) | Normative for accepted behavior |
| Why a non-obvious design was chosen | [`decisions/`](decisions/README.md) | Accepted rationale |
| Candidate or not-yet-accepted behavior | [`proposals/`](proposals/README.md) | Non-normative until promoted |
| Required observable coverage | [`required-tests/`](required-tests/) | Executable validation contract |

The old [`overview.md`](overview.md) URL remains as a short compatibility landing page.

## Human guide

Read these in order when learning or reviewing the feature:

| Guide | Answers |
|---|---|
| [`guide/architecture.md`](guide/architecture.md) | Which components own data, coordination, History, serialization, and provider state? |
| [`guide/input-lifecycle.md`](guide/input-lifecycle.md) | How does a Workflow Task move through active, quiescent, parking, parked, and no-open-task states? |
| [`guide/output-commit.md`](guide/output-commit.md) | How does output move from logical buffering through staging and History proof to visibility? |
| [`guide/replay-and-recovery.md`](guide/replay-and-recovery.md) | What is recorded, how exact-range replay works, and what survives retry, eviction, and Continue-As-New? |
| [`guide/operations.md`](guide/operations.md) | Which failures retry, which require intervention, and what must a production deployment operate? |

Diagrams explain the shape of the system but never define an edge case by themselves. Each guide
links to the specification sections that own its exact behavior.

## Normative implementation reference

Use the structured [`spec/` index](spec/README.md) to route by subsystem or stable document ID.

| If you need to… | Read |
|---|---|
| Change public names, handle roles, Worker configuration, or feature boundaries | [`spec/public-surface.md`](spec/public-surface.md) |
| Implement or evaluate a backend provider | [`spec/backend-contract.md`](spec/backend-contract.md) |
| Change Workflow Task retention, parking, rollover, shutdown, or output arbitration | [`spec/wft-lifecycle.md`](spec/wft-lifecycle.md) |
| Change replay markers, segmentation, budgets, or continuation state | [`spec/annotation-format.md`](spec/annotation-format.md) |
| Change proto messages, activation jobs, readiness calls, or Core state | [`spec/core-lang-protocol.md`](spec/core-lang-protocol.md) |
| Change Worker-side delivery, conversion, merge, close, or staging | [`spec/python-runtime.md`](spec/python-runtime.md) |
| Change wake encoding, deduplication, acknowledgement, or producer recovery | [`spec/wake-signal.md`](spec/wake-signal.md) |
| Classify errors, metrics, retries, or operator response | [`spec/failure-taxonomy.md`](spec/failure-taxonomy.md) |

Every normative spec carries machine-readable front matter with a stable `doc_id`, status, audience,
canonical scope, and related ADRs.

## Proposals and promotion records

Each proposal has two entry points:

- a short overview for deciding whether the direction is desirable; and
- a detailed design for API, protocol, failure, compatibility, and validation review.

The implemented output direction has been promoted into `spec/`; its detailed proposal remains a
non-normative rationale record. Workflow-to-Workflow subscriptions remain a future enhancement and
must not be inferred as available. See [`proposals/README.md`](proposals/README.md) for current status.

## Supporting sets

- [`decisions/`](decisions/README.md) contains 47 current ADRs. A specification says what is true; an
  ADR says why the rejected alternatives were not chosen.
- [`required-tests/`](required-tests/) contains the parsed Milestone 1 and Milestone 2 case lists.
  Editing their counted headings or bullets changes what the validation gate demands.
- [`verification-hazards.md`](verification-hazards.md) records seven constraints on trustworthy test
  evidence, including native-extension freshness, repository alignment, sandbox isolation, and
  controlled fault injection.

Status reports, review rounds, implementation plans, and investigation handoffs do not belong in the
design set. Their durable conclusions must be promoted into a specification, ADR, required test, or
proposal.

## Documentation rules

- **One normative fact, one home.** Other specifications link to the owning document. Human guides
  may summarize it but are explicitly non-normative.
- **Exact details stay textual.** Every diagram has a nearby explanation and specification link, so
  no transition exists only as an image.
- **No line-number references or restatement of local code.** Specifications name stable symbols and
  files; repository search resolves their current locations.
- **Every rule explains its failure consequence.** That consequence is what distinguishes a
  load-bearing invariant from an implementation preference.
- **Rejected alternatives live in ADRs.** Candidate behavior lives in proposals. Accepted current
  truth lives in specifications.
