---
doc_id: EWS-GUIDE-OPERATIONS
status: explanatory-pre-production
audience: [readers, operators, test-authors]
normative: false
---

# Operations and validation

External Workflow Streams depend on Temporal plus an external payload store. The feature remains
correct under ordinary task retry and Worker loss, but operators must preserve the records referenced
by retained History and must understand which wakeup guarantees come from producers rather than the
consumer SDK.

## Failure classes

| Symptom | Meaning | Expected response |
|---|---|---|
| Backend unavailable or timing out | Transient storage failure | Restore service; the Workflow Task retries |
| Recorded range, stage, or deciding History missing | Integrity loss | Restore/repair retained data or terminate the Run |
| Bytes present but payload conversion fails | Producer/consumer serialization mismatch | Align converter, codec, and serialization context |
| Replayed subscriptions differ from the marker | Workflow nondeterminism | Restore compatible code or use Workflow versioning |

The SDK reports these separately because retrying helps only the first class. Exact error types,
completion behavior, and metrics are in [`failure-taxonomy.md`](../spec/failure-taxonomy.md).

## Retention rule

The external backend must retain every input record referenced by retained Workflow History and every
pending output stage whose deciding History can still be consulted. Temporal retention alone does not
preserve external payloads.

Before deleting or trimming data, an implementation must prove that no retained marker or
Continue-As-New chain can refer to it. Provider-specific retention and garbage collection therefore
belong to the backend contract, not to Workflow code.

## Wake durability boundary

A producer appends first and then sends an acknowledged, idempotent wake when local delivery is not
possible. If a plain process dies after append and before the wake enters Temporal, a parked Workflow
can remain asleep even though its data is durable.

Deployments choose one explicit mitigation:

- Use a durable producer such as a retried Temporal Activity and treat wake acknowledgement as part
  of successful publication.
- Implement a durable provider outbox and relay.
- Add a Workflow Timer sweep that periodically rechecks, accepting its History and latency cost.

The SDK does not silently claim to close this distributed crash window. See the durability boundary
in [`wft-lifecycle.md`](../spec/wft-lifecycle.md).

## Provider requirements at a glance

- Immutable append-only records and stable, totally ordered opaque offsets.
- Exclusive live reads and inclusive exact-range replay reads.
- Idempotent append by stable record identity, with conflicts detected.
- Injective physical-key derivation across every stream identity component and direction.
- Race-free park-intent operations with conditional removal.
- Output stages with irreversible idempotent commit/abort and a readable-prefix barrier.
- Retention sufficient for replay and pending-stage reconciliation.

These bullets are only a checklist. The normative operations and conformance cases are in
[`backend-contract.md`](../spec/backend-contract.md).

## Trusting test results

The required-test lists are executable inputs to the Python test gate, not informal documentation.
They map each declared behavior to a real test and check the expected case count:

- [`tests-m1.md`](../required-tests/tests-m1.md) covers the implemented milestone behavior.
- [`tests-m2.md`](../required-tests/tests-m2.md) reserves the next milestone's required behavior.

Before acting on a test result, follow [`verification-hazards.md`](../verification-hazards.md). In
particular, verify the native extension is fresh, repositories and submodules are aligned, the gate
is reading the intended test list, and fault-injection tests actually observed their control
stimulus.

## History cost model

History cost follows Workflow Task boundaries rather than stream record count. On the input side,
many records and several retained-task activations can collapse into one marker; additional History
events come from completed input-consumption batches, wake Signals that move an idle Run back to
active work, and forced rollovers. On the output side, every latency- or capacity-driven flush adds a
marker and the surrounding Workflow Task lifecycle, so a larger publication window trades fresher
external reads for fewer History events.

Marker bytes are separately hard-bounded. A single-stream input run costs two offsets, a count, and
sparse control positions whether it covers ten records or 100,000. Alternating streams add schedule
transitions and may force extra rollovers. Output manifests scale with topics and activation segments,
not payload bytes; counts and fingerprints are aggregate fields. An individual frame that cannot fit
is rejected rather than allowing an unbounded marker.

Exact input and output encodings and budget behavior are defined in
[`annotation-format.md`](../spec/annotation-format.md). Flush and rollover boundaries are defined in
[`wft-lifecycle.md`](../spec/wft-lifecycle.md).

## Operational questions to answer before production

- Which component owns acknowledged wake retries?
- What retention policy protects replay ranges and pending stages?
- How are storage, integrity, decode, and shutdown-wake metrics alerted differently?
- Which producer owns each output topic's explicit terminal?
- How are provider credentials and Workflow-bound serialization contexts distributed?
- What publication latency is acceptable relative to added Workflow Task and History-event cost?

The feature is pre-production until its required validation is complete. Candidate extensions must
not be treated as implemented behavior; their status is maintained under [`../proposals/`](../proposals/).
