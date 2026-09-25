# ADR-046 — Output capacity is logical and segmentation is shared

**Status:** Accepted · **Affects:** output batching, replay · **Spec:**
`spec/annotation-format.md`

## Context

Encoded bytes can change when a payload codec is randomized, upgraded, or uses external storage.
A retained Workflow Task can span several activations, and one marker may contain both consumed
input and produced output. Letting each direction reproduce its own activation drains would double
the live schedule.

## Decision

Workflow output capacity is per Workflow Task and is computed from deterministic, pre-codec logical
frames. Fingerprint version 1 hashes ordered, length-prefixed frames whose payload metadata keys are
sorted by UTF-8 bytes. The first successfully staged encoded bytes remain authoritative; logical
counts and SHA-256 fingerprints define retry and replay identity.

The marker records one activation-segment schedule shared by input and output. Output contributes a
per-topic record-count vector to each segment, including empty segments. Replay disables live
capacity and latency policy, validates the recorded logical manifests, and performs the live number
of event-loop drains once across both directions.

An input schedule also retains activations with no input observation. If the first subscription
is created late in a task, earlier empty activations in that task precede its first observed
segment. This keeps the two schedules aligned when an Activity, timer, or another stream causes
an intervening activation. A prerelease combined marker with different input and output segment
counts is rejected: the positions of omitted empty input segments cannot generally be inferred
from those counts, and dropping output segments would weaken replay verification.

When another publish would exceed the record or logical-byte limit, `publish()` waits, the current
batch is staged with an output-capacity terminal, and Core forces a replacement Workflow Task. A
single oversized record or manifest is rejected before unsafe external I/O or marker growth.

## Consequences

- Codec size and randomness cannot move a deterministic batch boundary.
- Capacity is not reset by each activation of a retained task.
- Input and output replay drivers attach to one schedule; they never sum their drain counts.
- Marker bytes scale with topic and segment metadata, not payload size, and hard marker bounds still
  apply.
