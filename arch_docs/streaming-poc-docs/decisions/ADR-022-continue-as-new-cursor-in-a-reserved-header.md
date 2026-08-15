# ADR-022 — The Continue-As-New cursor travels in a reserved internal header

**Status:** Accepted · **Affects:** P15 · **Spec:** `spec/annotation-format.md`

## Context

A stream spans a full Continue-As-New chain. A new Run must resume from the position the previous Run
committed, and must do so on replay as well as live.

## Options

**A. Read the current position from the backend** when the new Run establishes its subscription.

**B. Store it in backend coordination state** keyed by chain, written at Continue-As-New.

**C. Attach it to a reserved internal header on the Continue-As-New command**, persisted in the new
Run's `WorkflowExecutionStarted`.

## Decision

**C.**

A and B both derive a cursor from **mutable backend state**, which replay must never do: the value
read on replay is whatever the backend holds now, not what the Run originally started from, so two
replays of the same history can diverge.

C makes the starting position durable in History itself. It is restored before any subscription is
established, and populates the same annotation header `start_cursor` field that a first-execution Run
fills with `BEGINNING`.

> A cursor is never derived from mutable backend state, on any Run.

- **First execution of a chain:** `BEGINNING`, recorded in the header's `start_cursor` in the
  subscription's **first** observation delta — emitted whether or not a record was ever delivered.
- **Subsequent Runs:** the committed continuation state arrives in the reserved header as an
  `AFTER(offset)` boundary.

## Consequences

- Replay reads an explicit starting boundary in **every** case, including the case where the stream
  was empty for the subscription's entire life.
- The stream key `(namespace, workflow ID, first execution Run ID, stream name)` uses the *first
  execution* Run ID, which prevents collisions after Workflow ID reuse while remaining stable across
  the chain.
- P15 depends on C14b, not only on header propagation: a Continue-As-New that drops its final segment
  restarts the new Run at a stale cursor. The observation delta must be committed on the
  terminal-command path.
- Two same-stream subscriptions restore independently, each keyed by its own `wait_id`.
