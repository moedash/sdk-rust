# ADR-010 — The input terminal in `FinalizeExternalStreams` is manager-state-only

**Status:** Accepted · **Affects:** C15a, P19 · **Spec:** `spec/python-runtime.md`

## Context

`FinalizeExternalStreams` asks Python for the annotation's terminal — the blocked cursor snapshot —
on paths where Core decides the boundary. Whether answering it requires touching the backend
determines its failure classes, its retry policy, and where it can be dispatched.

## Options

**A. Reconcile the terminal against the backend** before encoding it, so the recorded boundary
reflects current stream state.

**B. Encode the terminal from the manager's in-memory state**, calling no provider method.

## Decision

**B.**

The boundary is not "wherever the stream is now"; it is **where this Workflow Task's deliveries
stopped**, which is fixed the moment the last activation of that task returned. The manager already
holds it exactly. Refreshing it against the backend would be actively wrong — it could name a
position replay must not reproduce.

A would additionally require specifying the transaction, its race against readiness, and a retry
policy for transient failure, all to reproduce a value already in hand.

This decision is specifically about producing `final_observation_delta`. The later output direction
reuses the job with reason `OUTPUT_LATENCY`; in that case the Worker's outer async layer also stages
buffered output and sends `WorkflowOutputStreamCommit` on the same completion (ADR-045). That output
operation does not refresh or otherwise change the input terminal.

## Consequences

- **There is no input-provider transaction to race while encoding the terminal.** Output staging,
  when required, has its own failure and retry contract.
- **Watchers keep running during finalization**, exactly as they do during park preparation. A record
  arriving mid-finalization changes nothing about the terminal: it belongs to the next Workflow Task
  and reaches Core through the normal readiness path or, if none is open by then, through the wake
  Signal.
- **The terminal encoder's only failure mode is missing state** — the Run's manager entry is gone or
  unreadable. There is no transient class in that encoder. A separate output stage may fail as
  `StreamStorageError`; either failure means Core writes no marker and the Workflow Task is retried
  (ADR-008).
- It is still dispatched outside the synchronous Workflow thread, but **for a different reason than
  the other runtime-only jobs**: it must run no user code and must be answered from out-of-sandbox
  state, not because it blocks. See ADR-011.
- A test asserts the choice rather than trusting it: for a finalization with no buffered output, a
  provider that raises on every input method is registered, the finalization is driven, and the
  terminal must still be produced — proving its encoding makes no provider call.
