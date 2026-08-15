# ADR-005 — Progress is an observation delta, emitted on every completion path

**Status:** Accepted · **Affects:** C14a, P5, P10b · **Spec:** `spec/annotation-format.md`

## Context

`WorkflowStreamProgress` commits what replay must reproduce. The emission rule decides which
activations produce one.

## Options

**A. Emit when records were consumed.** The natural reading of "progress".

**B. Emit on quiescence only.** Fewer commands; one delta per park.

**C. Emit whenever replay-visible state changed** — an observation delta.

## Decision

**C.**

A loses the empty-stream case entirely. A Workflow that subscribes to an empty stream, becomes
quiescent, and parks has consumed nothing, so it emits no delta, so its marker carries no annotation
— and replay then has no provider identity, no stream key, no starting cursor boundary, and no
recorded no-data boundary, leaving it nothing to work from but live backend state, which this design
forbids.

B loses progress on every non-quiescent completion path, which is the failure ADR-004 exists to
prevent.

> Emit a `WorkflowStreamProgress` delta whenever the activation changed anything replay must
> reproduce. That includes deliveries, but also the first observation of a subscription, an
> activation that observed no records, and the boundary on which the activation returned.

Progress must reach the marker on **every** Workflow Task completion path that consumed external
data: normal completion; a completion carrying server-bound commands; a terminal command; parking
after quiescence; and rollover.

## Consequences

- The **first** delta for any subscription always carries provider identity, provider format version,
  stream key, and explicit starting cursor boundary, whether or not a record has ever been delivered.
- Every activation that ran a drain contributes a segment, **even an empty one**. The annotation
  grammar is `segment*` and `run*`, not `+`, and both empty cases must round-trip through the codec.
- The failure this rules out is specific and silent: if a consumed record influences a command that
  lands in History while the consumption is never marked, replay re-reads that record from the last
  committed cursor and delivers it again, while the command it produced is already durable. The
  divergence surfaces as an unrelated nondeterminism error much later.
