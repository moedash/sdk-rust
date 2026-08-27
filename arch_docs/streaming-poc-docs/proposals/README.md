# Proposals and promotion records

Candidate extensions and promotion records live here. A candidate is not part of the accepted
External Workflow Streams specification. A promotion record may retain the rationale for an
implemented design, but current normative behavior must be cited from `spec/` and
`decisions/`, not from this directory.

| Proposal | Status | Adds |
|---|---|---|
| [Workflow-originated external output streams](workflow-originated-external-streams.md) | Implemented; feature-wide pre-production validation continues | Workflow/Activity publishers and an external client subscriber over externally stored output |
| [Workflow-to-Workflow external stream subscriptions](workflow-to-workflow-external-streams.md) | Future enhancement; not implemented | Direct, replay-safe Workflow consumption of another Workflow's committed external output |

Promotion moves normative behavior into `spec/`, records each non-obvious choice in `decisions/`,
and adds required-test cases. Open cases in the executable mapping are the validation backlog for
the pre-production feature as a whole; they are not a separate maturity classification for the
output direction.
