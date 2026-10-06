# ADR-004 — Progress and quiescence are separate commands

**Status:** Accepted · **Affects:** C1, C6, C14a, P10a, P10b · **Spec:** `spec/wft-lifecycle.md`

## Context

At every activation return the runtime may need to do two things: commit what the Workflow observed,
and ask Core to hold the Workflow Task open. A single command carrying both is the obvious encoding.

## Options

**A. One command** that reports quiescence and carries the consumed records.

**B. Two commands** — `WorkflowStreamProgress` (commits an observation delta) and
`WorkflowStreamQuiescent` (asks for retention).

## Decision

**B.** The two questions are independent, and conflating them loses progress on every path where the
answers differ:

1. *Did replay-visible stream state change?* → `WorkflowStreamProgress`, on **every** completion
   path.
2. *Should the Workflow Task be retained?* → `WorkflowStreamQuiescent`, which starts the idle timer.

Under A, an activation that both consumed records **and** produced a server-bound or terminal command
is not quiescent, so it emits nothing — and the consumption is never committed. See ADR-005 for why
that is silently fatal.

A completion may carry either command, both, or neither. `WorkflowStreamProgress` never implies
retention, and `WorkflowStreamQuiescent` carries no annotation data.

## Consequences

- Two command tags (23 and 24) rather than one.
- Retention applies only when external stream waits are the sole reason the activation cannot
  progress. If the completion also carries server-bound commands, Python emits progress but not
  quiescence, and the wakeup comes from a watcher instead — see `spec/wft-lifecycle.md`.
- **Command ordering is normative:** `WorkflowStreamProgress` precedes every command whose value could
  depend on consumed data, so replay validates a record before matching the command derived from it.
- Core accumulates successive deltas for one Workflow Task and writes them as exactly one marker.
