# ADR-043 — A second Workflow Task is buffered, never substituted

**Status:** Accepted · **Spec:** `spec/wft-lifecycle.md`

## Context

`ManagedRun` owns the task token for one outstanding Workflow Task until that task is reported. A
new polled task can arrive on a different input lane before the local completion message clears the
old one. The Run may have no pending activation jobs at that instant, but it still owns the original
task and its token.

Overwriting the slot loses the original token. A debug-only assertion detects the invariant
violation during development, but in a release build the assertion logs and continues; continuing
with substitution would silently corrupt task ownership.

## Options

**A. Substitute the newer task.** Let the latest poll result win.

**B. Make the invariant an unconditional process panic.** Stop the Worker rather than continue.

**C. Guard admission and defensively buffer.** Normal admission buffers whenever a task is already
owned; the callee retains its debug panic but release builds preserve the old task and queue the new
one.

## Decision

**C.** `must_buffer_wft` includes ownership of an outstanding task even when no activation job is
pending. `_incoming_wft` keeps the debug panic so the impossible call path remains loud in debug
builds. After that diagnostic, the release path buffers the replacement and returns without changing
the original slot.

The original task is reported with its original token. Only then may the buffered replacement drain
into the Run.

## Consequences

- No scheduling race can transfer or discard ownership of a task token.
- Release recovery is local to this admission invariant; no global `dbg_panic!` policy changes.
- The regression must use a real `ManagedRun` and two permitted tasks, clear the first activation
  while leaving its task outstanding, prove the second buffers, report the first, and prove the
  second drains.
- Fault injection covers both profiles: debug panics at the invariant, while release preserves the
  original token and later admits the replacement.
