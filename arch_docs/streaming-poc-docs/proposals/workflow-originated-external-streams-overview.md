---
doc_id: EWS-PROPOSAL-OUTPUT-OVERVIEW
status: implemented-pre-production
audience: [design-reviewers, product-reviewers]
normative: false
---

# Workflow-originated external output streams — overview

Workflow-originated output is implemented as the output half of External Workflow Streams. Workflow
code or its Activities publish payloads to an external backend, and external clients subscribe there
without making Temporal History the data store.

> **Current authority:** this is a non-normative promotion summary. Accepted behavior lives in
> [`../spec/`](../spec/README.md) and ADR-044 through ADR-048. The feature remains pre-production
> while the required validation gate is incomplete.

## The problem

`temporalio.contrib.workflow_streams` stores its append-only log in Workflow state, so payload size
and replay work grow with History. That is unsuitable for agent token deltas, progress events, traces,
and other high-volume client-facing output.

The external output direction is:

```text
Workflow or Activity -> external backend -> external client
```

## Central guarantee

An external client may observe a Workflow-produced batch only after the Workflow Task that produced
it commits in Temporal History.

The Worker stages an unreadable batch before reporting the task, writes a compact stage proof in the
task's external-stream marker, and promotes the stage after acceptance. If that Worker disappears, a
reader or reconciler proves commit or abort from History. Direct Activity or process output is
already committed but cannot overtake an unresolved Workflow stage on the same topic.

See the human explanation and state machine in
[`../guide/output-commit.md`](../guide/output-commit.md).

## User-visible shape

- Workflow topics expose deterministic `publish()` and explicit `finish()` operations.
- Activities and external processes use explicitly connected direct-output producer handles.
- External clients subscribe with opaque resumable cursor boundaries.
- Input and output handles, records, coordination metadata, and physical keys remain distinct.
- Workflow-originated batches flush on commands, visibility deadlines, capacity, parking, rollover,
  or shutdown boundaries.

Fine-grained, high-rate output should normally originate in Activities: each Workflow output flush
adds a marker and Workflow Task lifecycle to History.

## Scope decisions retained by promotion

- Payload bytes never enter History.
- Staged data is invisible until its exact marker proves commit.
- Pending data is a per-topic ordering barrier.
- Output capacity is measured on deterministic logical frames.
- `FINISH` is explicit and survives Continue-As-New through a must-understand header.
- No global ordering is promised across topics or between concurrent Workflow and direct producers.

For the original end-to-end rationale, API sketch, rejected alternatives, and promotion record, read
the [`detailed design`](workflow-originated-external-streams.md).
