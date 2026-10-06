# ADR-015 — A decode failure is a separate class from an integrity failure

**Status:** Accepted · **Affects:** P18, P13 · **Spec:** `spec/failure-taxonomy.md`

## Context

Replay reads a recorded record and hands it to the `DataConverter`. The conversion can fail. So can
the read. Both surface at the same point in the code.

## Options

**A. One error class** for "replay could not produce the recorded record".

**B. Two classes** — `StreamIntegrityError` and `StreamDecodeError` — with separate metrics.

## Decision

**B.**

The operator actions are different, and A sends operators to the wrong one. A converter mismatch is a
configuration error on the **consumer**: the stream is fine, and reporting it as stream integrity loss
sends someone to restore a backend that was never damaged.

Because ADR-003 requires structural immutability of every provider, the classification rule is
mechanical and unconditional:

> **If the range validated, the bytes are the bytes that were written**, so any subsequent decode
> failure is a decode error. Only a missing offset or a range that fails validation is integrity loss.

## Consequences

- Two error types, two metrics, two documented operator responses:
  - `StreamIntegrityError` → repair or restore the backend, or terminate the Run.
  - `StreamDecodeError` → align the consumer's converter/codec with the producer's.
- Producer binding requires the producer to use **the same `DataConverter`** the consuming Workflow
  uses, including any codec. A mismatch is detected at decode time on the consumer and surfaces as
  `StreamDecodeError` — not as arbitrary user data, and not as a stream-integrity failure.
- P18 declares both types *before* the replay read path that raises them, so the taxonomy is not
  invented incidentally by the first caller.
- A unit test asserts the classification rule in both directions, and the Milestone 1 list includes a
  case where a record's bytes are intact but the converter configuration does not match.
