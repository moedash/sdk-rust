---
doc_id: EWS-GUIDE-INDEX
status: explanatory-pre-production
audience: [readers, design-reviewers, operators]
normative: false
---

# External Workflow Streams — human guide

This guide explains the feature as a system: what crosses each boundary, why Workflow Tasks are
sometimes retained, how wakeups work, how output becomes visible, and what replay depends on. It is
the place to build a mental model before reading protocol messages or backend operations.

> **Audience and authority:** this guide is explanatory and intentionally omits edge cases. The
> documents in [`../spec/`](../spec/README.md) are the normative implementation reference. If a
> diagram or summary here appears to disagree with a specification, the specification wins.

## Reading path

1. [`architecture.md`](architecture.md) — components, data paths, control paths, and ownership.
2. [`input-lifecycle.md`](input-lifecycle.md) — the Workflow Task and wakeup state machines.
3. [`output-commit.md`](output-commit.md) — staging, commit proof, visibility, and reconciliation.
4. [`replay-and-recovery.md`](replay-and-recovery.md) — markers, exact-range replay, Worker loss, and
   Continue-As-New.
5. [`operations.md`](operations.md) — failures, retention, durability boundaries, and validation.

## The feature in one paragraph

External Workflow Streams keep high-volume stream payloads in a pluggable external backend rather
than Temporal History. Workflows can consume external input and publish external output while compact
markers preserve deterministic replay. Signals carry wakeups, never records. Core owns Workflow Task
lifecycle and marker coordination; the language runtime owns backend I/O, payload conversion, and
replay validation. No Temporal Server protocol or persistence change is required.

## The four invariants worth remembering

- Stream payload bytes never enter Temporal History or a wake Signal.
- Reading or staging a record is not a commit; the corresponding marker is the durable boundary.
- Core never writes a stream marker without a terminal boundary supplied by the language runtime.
- Replay follows recorded ranges and activation segmentation, not current backend timing.

For exact language, exceptions, and failure consequences, start at the
[`spec/` index](../spec/README.md).
