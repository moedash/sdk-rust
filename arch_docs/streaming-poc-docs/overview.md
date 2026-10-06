---
doc_id: EWS-OVERVIEW-COMPAT
status: compatibility-landing-page
audience: [all]
normative: false
---

# External Workflow Streams — overview

External Workflow Streams move high-volume stream payloads out of Temporal History and into a
pluggable backend while compact marker events preserve deterministic replay. Workflows can consume
external input and publish externally readable output. Signals provide wakeups when local readiness
cannot reach an open Workflow Task; they never carry payload records. No Temporal Server change is
required.

The documentation now separates conceptual explanation from exact implementation contracts:

- Start with the [`human guide`](guide/README.md) for architecture, state machines, replay, output
  visibility, and operational boundaries.
- Use the [`normative reference`](spec/README.md) for protocol messages, state and transition rules,
  provider operations, algorithms, errors, and edge cases.
- Read [`decisions/`](decisions/README.md) for rejected alternatives and
  [`proposals/`](proposals/README.md) for candidate behavior.

## Essential model

```text
external producer -> external backend -> Workflow       (input)
Workflow/Activity -> external backend -> external client (output)
                         |
                         +-> payload bytes stay outside History

Temporal History <- compact replay and output-commit markers
Temporal Signal  <- wake metadata only, never stream data
```

The core lifecycle and ownership diagram is in
[`guide/architecture.md`](guide/architecture.md). Workflow Task and wake states are in
[`guide/input-lifecycle.md`](guide/input-lifecycle.md), and the output stage state machine is in
[`guide/output-commit.md`](guide/output-commit.md).

> This page is a compatibility landing page and is not normative. Exact behavior belongs to the
> specifications indexed by stable document IDs in [`spec/README.md`](spec/README.md).
