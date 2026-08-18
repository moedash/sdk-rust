# ADR-027 — A subscription created after the header is bound by its own frame

**Status:** Accepted · **Affects:** P5, P10b, P13 · **Spec:** `spec/annotation-format.md`

## Context

The annotation opens with a header frame carrying one binding per subscription — stream key, start
cursor, backend name, and the provider identity recorded for it. The header goes to Core in the
first observation delta of a Workflow Task.

`subscribe()` is ordinary Workflow code, and a retained Workflow Task spans many activations, so a
subscription can be created after that delta has already been sent. Core appends observation deltas
to a byte buffer and never rewrites what it holds, so the header it already has cannot be amended.

A wait with no binding still produces runs and a terminal entry, and replay then reads a `wait_id`
with no stream key, no backend, and no start cursor — reported as a wait "the Workflow did not
create", against code that never changed.

## Options

**A. Refuse a subscription after the header.** Keep one binding table, written once.

**B. Re-emit a header frame carrying the enlarged table.** No new frame kind.

**C. A bindings frame: the header's body under its own tag, carrying only the new waits.**

## Decision

**C.**

A makes an ordinary Workflow illegal. A subscription created inside a loop, after an activity, or on
a branch reached only once records arrive is normal code, and the annotation is an encoding detail
that must not be visible in what Workflow code is allowed to do.

B is ambiguous where C is not. A header frame also carries the schema version, so a decoder meeting
two of them has to decide whether the later replaces the earlier — for the version, and for every
binding that appears in both. Both readings are defensible, which is the problem: the frame would
mean one thing to the encoder and possibly another to a decoder written against the same grammar. A
bindings frame says exactly one thing, *these waits exist too*, and needs no such rule.

The frame is emitted with the delta of the activation that registered the wait, which puts it ahead
of both the segment that first records a run for the wait and the terminal — the two frames that can
name a `wait_id`. Decoding merges every bindings frame into the header's table, so replay reads one
flat table and never asks when a wait joined.

## Consequences

- `SCHEMA_VERSION` stays at **2**. The frame is purely additive: every annotation written before it
  decodes byte-identically, the grammar is self-describing through its frame tags, and a decoder
  that does not know the tag fails loudly on it rather than misreading the bytes that follow. A
  version exists to tell a reader what it is looking at; here the tags already do, and nothing needs
  to assume that version 2 implies no bindings frame.
- A wait is bound **exactly once** per annotation. A second binding for one `wait_id`, or any
  binding after the terminal, is a decode failure — replay must not choose between two stream keys,
  since the one it chose could be the one the records were not written to.
- A late wait carries **its own** start cursor rather than the position the waits already bound have
  reached, so replay begins it where the subscription began.
- The grammar is `header, (bindings | segment)*, terminal`: bindings and segments interleave, and
  neither ordering between them is fixed beyond a wait being bound before it is named.
