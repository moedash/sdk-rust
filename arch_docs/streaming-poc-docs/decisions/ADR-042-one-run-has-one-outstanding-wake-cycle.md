# ADR-042 — One Run has one outstanding wake cycle

**Status:** Accepted · **Spec:** `spec/wake-signal.md`

## Context

When Core cannot accept buffered readiness locally, the Worker sends a Signal that asks the server
for a Workflow Task. Signal acknowledgement proves that request reached History; it does not prove
that the task it created completed successfully. The task may fail, its report may be rejected, or
the Run may be evicted and replayed before the same buffered range is reconstructed.

If every reconstructed readiness report sends a new wake, each failed task can recursively create
another task. Concurrent subscriptions make the same mistake independently even though Workflow
Tasks are scoped to the Run, not to a subscription. Distinct wakes entering the execution while a
terminal command is closing it can sustain a `BUSY_WORKFLOW` / `UnhandledCommand` report-and-replay
cycle.

## Options

**A. Signal every readiness report.** Treat replay and every wait independently.

**B. Deduplicate by buffered range or wait generation.** Reuse durable-looking readiness facts as
the wake identity.

**C. Keep one Run-wide wake cycle until the task it caused completes.** Correlate that completion to
the particular successful Signal attempt, coalescing replay and concurrent waits behind it.

**D. Cap retries or treat `BUSY_WORKFLOW` as terminal.** Bound excess work by abandoning the wake.

## Decision

**C.** A Workflow Task services the Run's complete wait set, so the outstanding request is Run-wide.
The first readiness result that cannot be delivered locally opens a cycle and draws one wake counter.
All retries keep that counter and therefore the same request ID. Replay, eviction, and other waits
may reconstruct buffered readiness, but they do not open another cycle.

Each Signal attempt records the activation sequence at which it began. Only a successful completion
of a later activation can close that attempt's cycle, and it counts only after the Signal attempt is
acknowledged. A completion racing an in-flight attempt is retained until its result is known; a
failed attempt discards that correlation before retrying. This prevents a task that was already
running when the send began from being mistaken for the task the Signal caused.

D is not available. `BUSY_WORKFLOW` is transient, and abandoning the wake can strand a durable
record whose watcher has already advanced past it. A retry cap converts excess work into silent data
loss rather than restoring a bound.

## Consequences

- A nonterminal completion closes the cycle and re-reports any buffered readiness, which may open a
  genuinely later cycle with a new counter.
- A terminal completion closes the cycle without rearming; there is no future activation that could
  consume the buffer.
- Retiring a stale park intent invalidates the cycle that intent silenced before issuing the required
  unparked reannouncement.
- Failed Signal attempts remain retries of one wake, not new asks, and keep one request ID.
- Tests must cover replay reconstruction, concurrent waits, a completion racing a failed attempt,
  a genuinely later range, terminal completion, and stale-intent cleanup.
