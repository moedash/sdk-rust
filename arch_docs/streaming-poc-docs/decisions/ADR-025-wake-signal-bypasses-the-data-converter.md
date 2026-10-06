# ADR-025 — The wake Signal bypasses the user's `DataConverter`

**Status:** Accepted · **Affects:** C1, C11, P14 · **Spec:** `spec/wake-signal.md`

## Context

Core must read the wake Signal's fields — wait ID, park generation, chain identity — to decide whether
to resume the Run. Core has no access to the user's `DataConverter` or codec.

## Options

**A. Send it as a normal user Signal** through the configured `DataConverter`, and have Core ask the
language SDK to decode it.

**B. Define the envelope at the protocol level** and send it through a raw
`SignalWorkflowExecution` request built with the protocol's own serialization.

## Decision

**B.** The envelope is a single argument whose `Payload` uses metadata `encoding = "binary/protobuf"`
and `messageType = "coresdk.external_stream.WakeSignal"`.

Under A, a user codec that encrypts payloads makes the envelope unreadable to Core — the one component
that must read it. Round-tripping through the language SDK to decode a Signal that exists to wake the
language SDK inverts the dependency and cannot work when the Run is not cached.

**The Signal carries no user data, so bypassing the codec leaks nothing.**

## Consequences

- `WakeSignal` lives in its own proto file, `external_stream/external_stream.proto`. It is not a
  command or activation variant; it is the wire format of the reserved Signal's payload.
- The producer does **not** reuse the public Python Signal path. That path also generates a fresh UUID
  request ID per attempt, which would defeat the stable-request-ID requirement that lets the server
  deduplicate retries — a second reason the wake path is separate.
- `envelope_version` starts at 1; Core rejects unknown versions harmlessly.
- **Core suppresses the Signal from user handlers whether or not it validates**, so an unknown envelope
  version or a stale generation can never reach Workflow code as an unhandled Signal.
- The Signal name `__temporal_external_stream_wake` is fixed and versioned by the envelope rather than
  by the name.
