# ADR-024 — Milestone 0 is a throwaway spike that exports no public API

**Status:** Accepted · **Affects:** all Milestone 0 members, P9 · **Spec:** —

## Context

The first vertical slice is meant to de-risk the Core/Python round trip and the `WaitingOnLAs`
refactor (C5) before durable machinery is built on top. The question is whether that slice is a
preview of the product or a scaffold.

## Options

**A. First slice is a preview.** Ship live consumption with a public API, add markers and replay
later.

**B. First slice is a throwaway spike.** Private, unexported, never merged to a release; the first
*mergeable* slice is Milestone 1.

## Decision

**B.** Quality bar: prototype now, harden later — but the prototype is **throwaway, not a preview**.

A cannot survive its own Workflow Task retry. A slice that consumes external data without markers or
replay has no committed cursor, so any eviction, Worker restart, or Workflow Task failure re-delivers
or loses records. That is not a preview of the product; it is a different product with no durability.

Exporting the API in the spike would also freeze a shape that cannot replay.

## Consequences

- **No public API surface — module, class, or reserved name — is exported before Milestone 1 is
  complete.** P9 builds the Workflow-facing API but does not export it.
- Milestone 0 runs on **disposable histories with no replay guarantee**. A spike run evicted
  mid-stream is expected to lose its position, and that is acceptable precisely because nothing is
  merged.
- The spike gets rollover *transport* (C12a) but not marker-integrated rollover (C12b), because it
  emits no annotation to write.
- P5, P10b, C14a, C14b, C15a, and C15b are deliberately excluded: emitting observation deltas requires
  a codec to encode them and a Core handler to accept them.
- The spike is deleted or rewritten afterwards. Its value is the answer to "does the Core/Python round
  trip work", not the code.
