# ADR-035 — Both sides of a stream bind the Workflow's serialization context

**Status:** Accepted · **Affects:** P4, P6, P18 · **Spec:** `spec/python-runtime.md`

## Context

A `DataConverter` component may implement `WithSerializationContext`, which exists so that a codec or
payload converter can behave differently per Workflow — an encryption key derived from the Workflow
ID is the reason the interface is there. Everywhere else in the SDK the Worker binds a
`WorkflowSerializationContext(namespace, workflow_id)` before converting a payload for a Workflow,
on both the asynchronous and the synchronous side.

An external stream record is a payload delivered to a Workflow, converted by the same
`DataConverter`, in the same activation as the payloads that do get the binding. Producer and
consumer are required to share that converter (ADR-019), and a stream is written by a producer and
read by a consumer that may be a different process, a different Worker, and a later Run of the chain.

Both sides were context-free. That is the fact this decision turns on: they were **internally
consistent**, so every deployment with a context-aware converter was working.

## Options

**A. Leave both sides context-free.** Consistent, and no deployment breaks.

**B. Bind the consumer only.** The defect is on the consumer's decode path, so fix it there.

**C. Bind both sides**, from the same Workflow identity.

**D. Bind with `_with_contexts`**, adding the storage driver's store context alongside.

## Decision

**C**, and the symmetry is the whole of the argument.

A is not tenable once a Workflow reads a stream record and anything else in the same activation. The
Workflow's own argument arrives bound and the record does not, so a converter that derives anything
from the Workflow sees two different worlds one line apart. The consequence is not a clean error: a
context-aware codec that refuses without a context fails during preparation, the failure is carried
on the record and raised at delivery, and the Workflow Task retries forever against a record that
will never decode — reported as row three of `spec/failure-taxonomy.md`, whose operator instruction
is to align the consumer's converter with the producer's, when they already are aligned.

**B is the trap.** It reads as the minimal fix and it breaks every currently working deployment
whose codec keys on the Workflow: the producer would encrypt with no context while the consumer
decrypts with a Workflow-derived key, so records that appended successfully become undecodable on
the far side. Two context-free sides agree; two bound sides agree; one of each is the only
combination that does not. A defect that is symmetric has to be fixed symmetrically or not at all.

D binds something untrue. The store context names the Workflow as the payload's *storer*, and a
stream record is stored by its **producer** — an Activity or a plain process — not by the consuming
Run. Nothing on the consumer's decode path reads it in any case: retrieval resolves the driver from
the claim embedded in the payload rather than from a store context.

## Consequences

- **The identity is `workflow_id`, never a Run ID**, on all three holders. A stream spans a
  Continue-As-New chain, so a successor Run decodes records its predecessor wrote; anything
  Run-scoped makes a chain's own records unreadable from its first continuation onward.
- **Who holds a bound converter follows from what that holder is**, and getting it wrong is not
  cosmetic. The runtime is per Run and is built bound — on the Worker's side, because it crosses
  into the sandbox and `with_context` runs user code. The manager is per Worker and must **not**
  hold one: it prepares records for every Run at once, so it derives the context per record from the
  record's own stream key and memoizes the clones for that one call rather than on itself, where
  they would accumulate an entry per Workflow ID the Worker ever served. The producer serves one
  chain and binds once.
- **The ordinary deployment sees no change.** `with_context` returns the converter unchanged unless
  a component implements `WithSerializationContext`, so a default converter is not cloned and not
  altered.
- **The producer's store context stays empty**, naming no target, so a driver selector that routes
  by target has nothing to route on. That is a store-context gap rather than a serialization-context
  one, it is not needed for the symmetry above, and closing it requires deciding what target an
  Activity-hosted producer should name — which would change where blobs land.
- A test that proves this must use a converter whose behaviour actually **differs** by context. One
  that merely records what it was handed passes against option A on both sides, and a round trip
  through two context-free sides passes as well; only a per-Workflow transformation separates
  "both bound to the same context" from "both bound to nothing".
