# ADR-039 — The continuation carries the provider binding, and it is must-understand

**Status:** Accepted · **Affects:** P15 · **Spec:** `spec/annotation-format.md`

## Context

A Continue-As-New cursor is meaningful only in the store and provider format that produced it. The
successor Run can perform its first live read before it has written a marker, so marker replay cannot
protect that first restoration.

The feature is private and unreleased. There is no deployed continuation format to preserve.

## Decision

Each continuation entry carries its `wait_id`, cursor, stream name, `provider_id`, and
`provider_format_version`. The successor checks the stream name for Workflow nondeterminism and the
provider identity/version for unsafe Worker reconfiguration before handing the cursor to the
backend.

The binding is part of the only current schema. The encoder always writes that schema and the
decoder accepts exactly that schema. There is no legacy decoder, configurable writer version,
reader stage, writer stage, or downgrade path.

## Consequences

- A Worker configured with a different provider or provider format fails before backend I/O.
- An incompatible schema fails while the successor runtime is built, before Workflow code runs.
- Format changes before release replace the current schema and its fixtures directly; they do not
  add compatibility code for prototype formats.
