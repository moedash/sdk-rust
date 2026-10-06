# ADR-013 — The readiness result distinguishes a cached Run from a missing one

**Status:** Accepted · **Affects:** C4, P7, P8, P20 · **Spec:** `spec/core-lang-protocol.md`

## Context

A watcher that observes an append calls `notify_external_stream_ready()` and must decide what to do
next from the answer. The interesting case is a Run that is cached and has registered waits, but has
no open Workflow Task — the healthy window after a command-producing completion or a rollover.

## Options

**A. Three results** — `Accepted | Stale | Parked` — with everything else an error.

**B. Four results**, folding "cached but no open WFT" into `RunNotFound`.

**C. Five results**, with `NoOpenWorkflowTask` distinct from `RunNotFound`.

## Decision

**C.** `Accepted | Stale | Parked | NoOpenWorkflowTask | RunNotFound`.

B collapses the normal, healthy path into the cache-eviction path. Two things go wrong: the metric
cannot distinguish routine operation from eviction pressure, and the watcher tears itself down at
exactly the moment it is still needed — the Run is still cached, its Python watchers still exist, and
Core simply has no open Workflow Task.

| Result | Meaning | Watcher action |
|---|---|---|
| `Accepted` | Readiness serialized into an open WFT | Nothing further; Core will activate |
| `Stale` | The wait exists but its `wait_generation` moved on | Re-probe; do **not** signal |
| `Parked` | A confirmed `park_generation` exists | Send the wake Signal |
| `NoOpenWorkflowTask` | Run cached, waits registered, no WFT open | Send the wake Signal; **keep** the watcher |
| `RunNotFound` | Run absent from this Core worker's cache | Send the wake Signal, then tear the watcher down |

The three signal-sending results are distinguished by what the watcher does *afterwards* and by what
an operator should conclude, not by whether a Signal is sent.

## Consequences

- A separate **read-only** companion call, `external_stream_run_status(run_id) -> WftOpen | Parked |
  NoOpenWorkflowTask | RunNotFound`, exists for the shutdown sweep. It is deliberately not the
  readiness call: readiness means "a record is buffered", so probing with it would assert something
  false and manufacture a spurious activation on the way out.
- Both are answered on the same serialized local-input lane, so a status answer is as authoritative as
  a readiness acknowledgement. Core already answers a read-only question on that lane through
  `GetStateInfoMsg`, so this is not a new kind of mechanism.
- Each result gets its own metric — see `spec/failure-taxonomy.md`.
- The bridge surfaces both enums to Python (P7).
