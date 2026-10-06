# ADR-023 — `park_generation = 0` is the unparked wake

**Status:** Accepted · **Affects:** C1, C11, P14, P20 · **Spec:** `spec/wake-signal.md`

## Context

The wake Signal carries `(wait_id, park_generation)`, and Core ignores a Signal naming a generation it
does not recognize as stale. But three senders need to wake a Run when **no park generation exists at
all**: a watcher seeing `NoOpenWorkflowTask`, a watcher seeing `RunNotFound`, and the Worker's
shutdown sweep.

## Options

**A. A separate Signal or command** for the unparked wake.

**B. Send the parked envelope with an arbitrary generation** and let Core treat it as stale-but-wake.

**C. Reserve `park_generation = 0`** to mean "no park generation — this is a recheck request".

## Decision

**C.** Park generations are quiescence generations, which start at 1, so 0 is free.

A adds a wire format, a Core handler, and a Python send path to express something the existing
envelope can carry.

B is self-defeating: an unrecognized non-zero generation is *supposed* to be ignored as stale, because
there the sender is making a claim that turned out to be wrong. Overloading it removes the design's
only defense against genuinely stale Signals.

Core validates chain identity for an unparked wake and otherwise accepts it as a recheck request. The
runtime rechecks every active subscription on wakeup regardless, so an unnecessary unparked wake costs
at most one empty Workflow Task — which this design already permits.

## Consequences

- **Parked and unparked wakes use the same envelope.** One Signal name, one message, one interception
  path.
- A **non-zero** generation the current Run does not recognize is still ignored as stale.
- The derived request ID needs care for unparked wakes. It is normally derived from
  `(namespace, workflow_id, first_execution_run_id, stream_name, wait_id, park_generation)` so retries
  deduplicate server-side. With `park_generation = 0` there is no attempt identity, so the derivation
  additionally includes **the sender's identity and a per-sender monotonic wake counter**, held fixed
  across retries of that one attempt. Without that, two Workers shutting down at different times would
  derive the same request ID and the server would deduplicate the second wake away — turning a correct
  retry mechanism into silent loss.
- Tests must assert both directions: an unparked wake is accepted as a recheck, and two Workers
  shutting down at different times derive **different** request IDs with both wakes delivered.
