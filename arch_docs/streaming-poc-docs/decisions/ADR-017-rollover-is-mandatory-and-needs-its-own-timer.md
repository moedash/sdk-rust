# ADR-017 — Workflow Task rollover is mandatory and needs a sink-independent timer

**Status:** Accepted · **Affects:** C13, C12a, C12b · **Spec:** `spec/wft-lifecycle.md`

## Context

Retaining an open Workflow Task is how the runtime keeps consuming without writing an event per
record. But a retained task is bounded by the server's Workflow Task timeout, and Core already has a
mechanism that forces a task to roll over: the local-activity heartbeat deadline.

## Options

**A. Defer rollover** to a later milestone; ship retention first.

**B. Reuse `sink_heartbeat_timeout_start`**, the existing local-activity heartbeat timer.

**C. Add a per-Run timer facility on `ManagedRun` independent of the local-activity request sink**,
and make the local-activity heartbeat one caller of it.

## Decision

**C**, in the first slice that retains Workflow Tasks at all — including the Milestone 0 spike.

**Why A is not available.** A stream whose inter-record gaps stay under the idle timeout never reaches
the idle parking path, so the retained Workflow Task runs until the server's Workflow Task timeout
expires and the task *fails*. With a one-second idle timeout, a producer emitting 100 records at
sub-second intervals holds a task open far past a default ten-second Workflow Task timeout. Any slice
that retains Workflow Tasks needs rollover in the same slice.

**Why B is insufficient.** `sink_heartbeat_timeout_start` schedules the deadline by pushing a
`LocalActRequest::StartHeartbeatTimeout` into the local-activity request sink, inside
`if let Some(la_sink) = sink`. With no sink it **silently** returns an `AbortHandle` for a timer that
was never started. The sink is `Option`al on `ManagedRun` and is constructed only when
`config.task_types.enable_local_activities`, and Python sets
`enable_local_activities = self._activity_worker is not None`.

So **a Python Worker that registers Workflows but no Activities has no rollover timer at all** — and
external streams must work on exactly that kind of Worker.

The `force_new_wft` plumbing itself is fine and already exists; it is the *timer* that must be lifted
out.

## Consequences

- One code path for "deadline that forces a Workflow Task to roll over", avoiding two timers racing on
  the same Run.
- The rollover deadline derives from the same `wft_timeout` that drives the local-activity heartbeat,
  but is scheduled independently of the sink.
- The idle timeout is clamped below the rollover deadline so rollover stays authoritative.
- Every active subscription, cursor, annotation delta, and readiness generation survives rollover.
- Rollover also bounds a second effect: while a task is retained the server cannot start another one,
  so Signals, Updates, and non-legacy Queries queue until it completes. Retention latency for those
  inputs is bounded by the rollover deadline and nothing else; callers needing lower latency must
  lower the Workflow Task timeout, not the idle timeout.
- C13 is on the critical path for anything that retains a Workflow Task, and a test must exercise a
  Worker with `enable_local_activities = false`.
- Rollover splits into C12a (transport, no marker — usable in the spike) and C12b (finalization and
  marker emission).
