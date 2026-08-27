# ADR-048 — Finished output topics use a must-understand header

**Status:** Accepted · **Affects:** Continue-As-New, topic termination · **Spec:**
`spec/annotation-format.md`

## Context

Workflow output topics span a Continue-As-New chain. A successor must reject a publish after the
Workflow explicitly finished that topic without consulting mutable provider state, and a provider
offset or marker from another implementation cannot safely answer that question.

## Decision

`finish()` in Workflow code and `finish_writing()` in a direct producer are the only operations that
append a `FINISH` record. Workflow completion, failure, cancellation, and termination synthesize no
topic terminal.

Before Continue-As-New, the Workflow runtime writes the topics it finished plus provider identity and
format version into a reserved output continuation header. The format is must-understand: the
successor decodes and validates it before Workflow code or backend I/O, restores the finished set,
and deterministically rejects another Workflow publish to a finished topic.

A direct producer's `finish_writing()` closes that producer's topic handle; it cannot mutate a
Workflow's continuation header. Applications mixing direct and Workflow publishers on one topic
must therefore choose one terminal owner. A client stops at the first `FINISH` it reads.

## Consequences

- Finished state survives across Runs without using mutable backend state as a replay fact.
- Direct producers do not silently impose Workflow state through the backend.
- An unknown schema or mismatched provider binding fails before user code runs.
- Clients that want to stop on Workflow closure also observe Temporal execution status; backend
  idleness is never an implicit finish.
- The input cursor continuation and output finished-state continuation remain separate reserved
  headers because they carry different must-understand schemas.
