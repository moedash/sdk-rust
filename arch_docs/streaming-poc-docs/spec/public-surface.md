---
doc_id: EWS-SPEC-PUBLIC
status: normative-pre-production
audience: [implementers, coding-agents, reviewers]
canonical_for: public-names-roles-and-feature-boundary
related_adrs: [ADR-001, ADR-019, ADR-021, ADR-037, ADR-040, ADR-048]
---

# Public surface and feature boundary

The names, handle roles, configuration boundary, and user-visible distinctions of External Workflow
Streams. Detailed lifecycle and provider behavior belongs to the linked subsystem specifications.

## Feature boundary

External Workflow Streams coexist with `temporalio.contrib.workflow_streams`; they do not replace or
extend that package's storage model.

| Concern | `contrib.workflow_streams` | External Workflow Streams |
|---|---|---|
| Data storage | Append-only Workflow state represented in History | Pluggable external backend; payloads stay outside History |
| Workflow role | Produces for external clients | Consumes external input and/or produces external output |
| Input wake transport | Not applicable | Local readiness or a payload-free reserved Signal |
| Replay position | Dense offsets in Workflow state | Opaque provider boundaries and compact external-stream markers |

The public module is `temporalio.contrib.external_workflow_streams`. No public or reserved name in
this feature may begin with `__temporal_workflow_stream`; that prefix belongs to the existing package
(ADR-001).

Live Workflow interaction and marker-backed replay are one indivisible capability. The public
surface has no live-only mode that reads or publishes external records without recording the
corresponding input cursor boundaries or output commit manifest. Without that durable boundary, a
Workflow Task retry, eviction, or Worker restart could not distinguish work already observed from
work that had not yet been delivered or committed.

## Names and roles

| Role | Public entry point or type | Operations |
|---|---|---|
| Workflow input namespace | `external_stream` | `with_options(...)`, `topic(...)` |
| Workflow input topic | `ExternalStreamTopic` | `subscribe()` |
| Workflow input subscription | `ExternalStreamSubscription` | Async iteration, `close()` |
| Workflow input merge helper | `merge` | `merge(*subscriptions)` |
| External input producer connection | `ExternalStreamProducer` | `connect(...)`, `topic(...)` |
| External input producer topic | `ExternalStreamProducerTopic` | `publish(...)`, `finish_writing()`, append/wake recovery |
| Workflow output namespace | `external_output_stream` | `with_options(...)`, `topic(...)` |
| Workflow output topic | `ExternalOutputStreamTopic` | `publish(...)`, `finish()` |
| Direct output producer connection | `ExternalOutputStreamProducer` | `connect(...)`, `topic(...)` |
| Direct output producer topic | `ExternalOutputStreamProducerTopic` | `publish(...)`, `finish_writing()`, append recovery |
| External output reader connection | `ExternalOutputStreamClient` | `connect(...)`, `topic(...)` |
| External output reader topic | `ExternalOutputStreamClientTopic` | `subscribe(...)` |
| External output item | `ExternalOutputStreamItem` | decoded data plus opaque resume offset |

Consumer-side and producer-side topics are distinct types. A Workflow topic is bound to the running
Workflow and the Worker's configured provider. A producer topic is bound explicitly to credentials,
Workflow chain identity, serialization context, and a retry-stable session. No topic handle is passed
from Workflow code to an Activity or external process as a backend capability (ADR-019).

## Worker configuration

The Worker option is `external_stream_backend`. Its instance lives outside the Workflow sandbox and
Workflow code cannot select or access its connection.

`StreamBackend` and `OutputStreamBackend` are independent provider capabilities:

- an input-only provider remains valid;
- an output-only provider can serve direct output producers and external output clients; and
- Workflow-originated output requires the Worker's configured provider to implement both contracts
  in the first release.

Exact validation and operations: [`backend-contract.md`](backend-contract.md). Sandbox ownership and
dispatch: [`python-runtime.md`](python-runtime.md).

## Stream identity

A stream is identified by namespace, Workflow ID, first execution Run ID, direction, and topic name.
Direction physically isolates input and output topics with otherwise identical components. The first
execution Run ID is stable across Continue-As-New and isolates later reuse of the same Workflow ID.

Producer connections take the Workflow chain key; `topic(name)` adds the name exactly once. The
producer verifies the chain identity before its first append. Physical encoding must be injective for
all components; see [`backend-contract.md`](backend-contract.md).

## Input behavior visible through the API

- `external_stream` uses an `idle_timeout` of one second by default.
  `with_options(idle_timeout=...)` configures the value carried by subscriptions created from those
  options. Values must be positive; when a Workflow blocks on several subscriptions, their values
  reduce deterministically by `min` before Core starts the one timer for the complete wait set
  (ADR-016).
- `subscribe()` creates a new subscription with its own wait identity and cursor.
- Multiple subscriptions to one topic receive broadcast delivery; they do not form a work-sharing
  group (ADR-021).
- One subscription permits one active consumer. A second waiter is rejected at the waiter rather
  than corrupting cursor state (ADR-037).
- `finish_writing()` appends an ordered write fence for one producer session. It does not close the
  input stream and does not prevent later producers or later records (ADR-040).
- Cancelling or closing a subscription does not delete already recorded state or end the underlying
  topic.

Exact retention, parking, and cancellation behavior: [`wft-lifecycle.md`](wft-lifecycle.md) and
[`python-runtime.md`](python-runtime.md).

## Output behavior visible through the API

- Workflow `publish()` buffers a logical record. It never returns or exposes the eventual provider
  offset to deterministic Workflow code.
- Workflow `finish()` and direct-producer `finish_writing()` append the ordered `FINISH` terminal.
- Direct `finish_writing()` closes that producer's topic handles but does not write the Workflow's
  Continue-As-New state. Mixed publishers must designate one terminal owner.
- Workflow closure, failure, cancellation, termination, and Continue-As-New do not synthesize
  `FINISH` (ADR-048).
- External readers receive committed records only and resume strictly after an opaque boundary.
- No global order is promised across topics or between concurrently produced Workflow and direct
  output.

Exact staging and visibility behavior: [`backend-contract.md`](backend-contract.md),
[`wft-lifecycle.md`](wft-lifecycle.md), and [`annotation-format.md`](annotation-format.md).

## Reserved wake name

The input wake Signal is `__temporal_external_stream_wake`. It is protocol metadata rather than a
user payload and is intercepted before user Signal dispatch. Its exact envelope and request-ID rules
are defined in [`wake-signal.md`](wake-signal.md).

## Compatibility status

The feature is pre-production. Annotation, continuation, and wake encodings have one current format
and no legacy compatibility mode. Unsupported must-understand versions fail explicitly rather than
silently changing replay or continuation behavior.
