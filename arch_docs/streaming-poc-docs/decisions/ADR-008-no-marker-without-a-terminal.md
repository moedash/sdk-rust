# ADR-008 — Core never writes a marker without a terminal from Python

**Status:** Accepted · **Affects:** C14b, C15a, C15b, C12b, C8 · **Spec:** `spec/core-lang-protocol.md`

## Context

The annotation grammar is `annotation := header, segment*, terminal`, and the terminal is the blocked
cursor snapshot. **Only Python can encode it** — Core is annotation-blind by design. But several
completion paths are decided *inside Core*, with no Python activation outstanding at the moment of
decision: idle-timeout park, all-fenced park, rollover-deadline expiry, and shutdown or eviction with
a Workflow Task open.

## Options

**A. Core manufactures a terminal** from what it knows.

**B. Core writes a best-effort marker** from the last accumulated delta when it cannot obtain a
terminal.

**C. Core always obtains a terminal from Python first**, and writes nothing if it cannot.

## Decision

**C**, with no exceptions. Core obtains the terminal via
`ExternalStreamParkResult.final_observation_delta` for the park paths, and via
`FinalizeExternalStreams` → `ExternalStreamFinalized` for the rest.

A is impossible: Core does not parse the annotation and does not know stream offsets.

B produces a marker that violates its own grammar — a `header, segment*` with no `terminal` — and
replay would have to guess at the boundary it was supposed to record. **A truncated annotation is
durable and wrong.** An abandoned Workflow Task, by contrast, commits no cursor and loses no record;
the cost is one repeated Workflow Task.

If a terminal cannot be obtained, Core writes nothing, the Workflow Task fails, and the server
retries it. The replacement attempt replays from the previous marker.

## What makes C implementable

> **An accumulated, unwritten annotation exists only while a Workflow Task is open.**

Deltas arrive only on activation completions, activations exist only under a Workflow Task, and every
Workflow Task completion path writes the accumulated annotation as exactly one marker and clears it.
So a Core-decided boundary either has a Workflow Task to finalize against, or has nothing to write.
There is no third state in which Core holds a partial annotation with no way to complete it.

Core asserts the invariant where it clears `ExternalWaitSet.replay_annotation` (C15a), and a test
drives eviction in both Run states to confirm the no-open-WFT case writes no marker at all.

## Consequences

- Refusing to emit a terminal-less annotation belongs to **emission** (C14b); obtaining a terminal for
  a Core-decided boundary belongs to **finalization** (C15a). That is why the dependency runs
  C15a ⇢ C14b and not the reverse.
- `FinalizeExternalStreams` is a runtime-only activation job in the same class as
  `PrepareExternalStreamPark`: it runs no user Workflow code, cannot resolve futures, and its only
  legal responses are `ExternalStreamFinalized` or an activation failure.
- **`ParkReason` lives in exactly one place** — the Core-readable
  `ExternalStreamMarkerData.terminal_boundary`. Core knows the reason on every path, so duplicating it
  inside the opaque terminal would add a second copy that could disagree with the first.
- The marker is written by the **finalization completion**, never by the eviction completion, which
  reports nothing and may carry no commands.
