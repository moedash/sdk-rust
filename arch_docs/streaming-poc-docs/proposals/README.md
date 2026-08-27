---
doc_id: EWS-PROPOSAL-INDEX
status: mixed
audience: [design-reviewers, implementers, coding-agents]
---

# Proposals and promotion records

Candidate extensions and promotion records live here. A candidate is not part of the accepted
External Workflow Streams specification. A promotion record may retain the rationale for an
implemented design, but current normative behavior must be cited from `spec/` and
`decisions/`, not from this directory.

| Proposal | Status | Short overview | Detailed design |
|---|---|---|---|
| Workflow-originated external output streams | Implemented; feature-wide pre-production validation continues | [Overview](workflow-originated-external-streams-overview.md) | [Promotion design and rationale](workflow-originated-external-streams.md) |
| Workflow-to-Workflow external stream subscriptions | Future enhancement; not implemented | [Overview](workflow-to-workflow-external-streams-overview.md) | [Full candidate design](workflow-to-workflow-external-streams.md) |

Promotion moves normative behavior into `spec/`, records each non-obvious choice in `decisions/`,
and adds required-test cases. Open cases in the executable mapping are the validation backlog for
the pre-production feature as a whole; they are not a separate maturity classification for the
output direction.
