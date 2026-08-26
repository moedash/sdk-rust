# ADR-001 — Coexist with `contrib.workflow_streams` rather than replacing it

**Status:** Accepted · **Affects:** P9, P17 · **Spec:** `overview.md`

## Context

The Python SDK already ships an experimental `temporalio.contrib.workflow_streams` package
(`WorkflowStream`, `WorkflowStreamClient`, `TopicHandle`, `WorkflowTopicHandle`), reserving the
Signal/Update/Query names `__temporal_workflow_stream_publish`, `__temporal_workflow_stream_poll`,
and `__temporal_workflow_stream_offset`. Shipping a second streaming feature alongside it risks
two overlapping APIs users must choose between.

## Options

**A. Replace it behind a compatible API.** One public surface; the external-backend
implementation slots underneath.

**B. Coexist as a separate module.** Two features, two module names, users choose.

**C. Add an external-backend storage mode to the existing package** without changing its public
surface.

## Decision

**B.** The two features are mirror images, not two implementations of one idea:

| | `contrib.workflow_streams` | External Workflow Streams |
|---|---|---|
| Direction | Workflow **produces**; external clients consume | External producers publish; Workflow **consumes** |
| Workflow-side API | `topic().publish()`; deliberately no workflow-side `subscribe()` | `topic().subscribe()`; no workflow-side produce path |
| Storage | Append-only log in Workflow state, i.e. in History | Pluggable external backend; payloads never enter History |
| Offsets | Dense `int`, exposed in `subscribe(from_offset: int)` and the offset Query | Opaque, provider-defined, totally ordered tokens (Redis IDs are `<ms>-<seq>`) |
| Transport | Signals, long-poll Updates, SSE bridge | Direct backend reads; Signals carry no payload and supply only wakeups |

A is not feasible: there is no consumption API in the contrib feature to be compatible *with*, and
its `int` offset surface is incompatible with opaque provider offsets even for the directions that
overlap.

C stays open as future work but is explicitly not a dependency of this design — it requires solving
the dense-integer-offset mapping. A distinct extension of External Workflow Streams with
Workflow-originated output and opaque client cursors is described in the
[output-stream proposal](../proposals/workflow-originated-external-streams.md); it does not alter
this decision until accepted and incorporated into the specification.

## Consequences

- Both features ship; neither is deprecated by this work.
- This feature takes distinct names: module `temporalio.contrib.external_workflow_streams`, entry
  point `external_stream`, reserved Signal `__temporal_external_stream_wake`.
- **No name in this feature may begin with `__temporal_workflow_stream`.** P9's completion criteria
  assert this.
