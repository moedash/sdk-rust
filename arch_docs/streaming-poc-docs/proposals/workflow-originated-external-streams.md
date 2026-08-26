# Proposal — Workflow-originated external output streams

**Status:** Proposed, not part of the accepted External Workflow Streams specification

## Summary

Add the complementary stream direction:

```text
Workflow or its Activities -> external backend -> external client
```

The current feature moves externally produced records into a Workflow without putting their
payloads in History. This proposal lets a Workflow publish externally observable records under the
same constraint. It is intended for agent token deltas, progress events, traces, and other ordered
output that clients must be able to resume without making Workflow History the data store.

This is not a storage-mode switch for `temporalio.contrib.workflow_streams`. It extends
`temporalio.contrib.external_workflow_streams` with a distinct output direction, distinct handle
types, and a client-side subscriber. The existing input direction and its accepted contracts remain
unchanged.

The central rule is:

> A client may observe an output batch only after the Workflow Task that produced it is durably
> committed in Temporal History.

Writing to the backend before Workflow Task completion without such a gate leaks output from a
failed or rejected task. Writing only after completion loses output if the Worker dies after the
server accepts the task but before the backend write. The proposed protocol therefore stages output
before completing the Workflow Task, records only a compact commit proof in History, and exposes the
staged records after the server accepts that proof.

Workflow-originated output has a **Workflow Task visibility quantum**. The first buffered publish
arms an output-flush deadline below the ordinary Workflow Task rollover deadline; reaching it
completes and rolls over a retained task even when its input subscriptions remain active. Records
therefore begin flushing no later than that configured quantum and become visible after the staging
and Workflow Task completion round trips under healthy server, Worker, and backend conditions. Each
such flush writes a marker and a Workflow Task lifecycle into History. Fine-grained, genuinely
high-rate output should consequently originate in Activities, whose direct output publishes do not
require one Workflow Task per latency window.

## Motivation

`temporalio.contrib.workflow_streams` serves the correct direction for UI and agent output, but its
append-only log is Workflow state and its payloads consequently become History payloads. High-volume
token streams then scale History bytes and replay work with token count.

The current External Workflow Streams feature cannot replace it because its direction is the mirror
image:

| Capability | Current external stream | Proposed external output stream |
|---|---|---|
| Workflow role | Consumer | Producer |
| Other endpoint | External producer | External client subscriber |
| Workflow API | `subscribe()` | `publish()` |
| Wakeup | Signal wakes the Workflow | Backend watch wakes the client |
| Replay fact | Which records the Workflow consumed | Which output batch the Workflow committed |

The Temporal Agent Harness makes the gap concrete. Activities and Workflow code publish a single
ordered event log, clients resume it by offset, and the log is the live UI protocol. Moving only the
Activity-to-Workflow token ingress to the current feature would be a useful hybrid, but it would not
externalize the client-facing log.

## Goals

- Keep output payload bytes out of Temporal History.
- Never expose output from a Workflow Task that did not commit.
- Recover output whose Workflow Task committed when the producing Worker died before making it
  visible.
- Give clients opaque resumable cursors and direct backend reads.
- Preserve publish order within each committed Workflow batch, and prevent later committed records
  from overtaking an unresolved staged predecessor on the same topic.
- Keep Workflow code deterministic and free of backend connections or credentials.
- Span a Continue-As-New chain and remain isolated from later reuse of the same Workflow ID.
- Reuse the configured backend, `DataConverter`, serialization context, immutable-record contract,
  and producer idempotency rules where their semantics match.

## Non-goals

- A transaction spanning Temporal and an arbitrary external database. The protocol supplies a
  recoverable commit proof, not distributed atomic commit.
- Making output visible before its producing Workflow Task commits.
- Returning a provider offset synchronously to Workflow code. That offset is not durable until the
  task commits and must not influence deterministic Workflow control flow.
- A global order or atomic snapshot across different topics. Ordering and batch visibility are
  per-topic.
- A deterministic total order between a concurrently running Activity and Workflow Task retry. The
  backend barrier prevents overtaking an unresolved predecessor, but an aborted attempt contributes
  no records and its retry may land after Activity output appended in between.
- Letting browser code hold backend credentials. A web service may proxy the client subscriber to
  SSE or WebSocket clients.
- Replacing the existing input direction or deprecating `contrib.workflow_streams` in the first
  release.
- Token-rate publishing from Workflow code at no History-event cost. Output-flush rollover bounds
  latency by trading one marker plus Workflow Task lifecycle events for each flush window.

## Proposed API shape

The names in this section are provisional. The separation of roles is not.

### Workflow publisher

```python
from datetime import timedelta

from temporalio.contrib.external_workflow_streams import external_output_stream

events = external_output_stream.with_options(
    max_publish_latency=timedelta(milliseconds=100),
).topic("agent-events", type=AgentEvent)

await events.publish(AgentEvent(...))
await events.finish()
```

`publish()` ordinarily accepts the value into the current Workflow Task's output batch without a
backend round trip and returns no external offset. Making it awaitable gives the runtime a
deterministic backpressure point: when the batch's record or logical-byte budget is exhausted, the
await yields so the current batch can be staged and a new Workflow Task can continue publishing.
The first accepted value also arms `max_publish_latency`; if that deadline wins first, Core stages
the accumulated batch and rolls over the retained Workflow Task outside the Workflow thread.

`finish()` appends an ordered terminal control record for this topic. It is explicit because a
Workflow failure or termination cannot reliably execute cleanup code. A client may also stop by
observing the Workflow's terminal status through Temporal; it must not infer completion from an idle
backend watch.

The Workflow-facing handle has `publish()` and no `subscribe()`. The current Workflow-facing input
handle keeps `subscribe()` and no `publish()`.

### Activity or external output producer

```python
producer = await ExternalOutputStreamProducer.connect(
    backend=RedisStreamBackend(url=...),
    workflow=WorkflowChainKey(
        namespace=...,
        workflow_id=...,
        first_execution_run_id=...,
    ),
    client=client,
    session_id=...,  # stable across Activity retry
)
events = producer.topic("agent-events", type=AgentEvent)

await events.publish(AgentEvent(...))
await events.finish_writing()
```

Activities need this path for live token and tool events. The chain key is explicit for the same
reason it is on the current producer: Activity info does not contain the first execution Run ID.
Workflow code threads the key through an Activity input or an opaque application context.

Unlike the current input producer, an output producer sends no wake Signal to the Workflow. Its
consumer watches the backend directly. Its records are immediately committed singleton batches,
idempotent on `(session_id, sequence)`.

### External client subscriber

```python
reader = await ExternalOutputStreamClient.connect(
    backend=RedisStreamBackend(url=...),
    workflow=WorkflowChainKey(...),
    client=client,
)
events = reader.topic("agent-events", type=AgentEvent)

async for item in events.subscribe(after=cursor):
    render(item.data)
    cursor = item.offset
```

The yielded item carries both the decoded value and an opaque provider offset. `BEGINNING` and
`AFTER(offset)` retain their current boundary meanings. A topic also exposes a committed `tail()`
boundary so a caller can establish a safe resume point before sending an Update that causes new
output.

The Temporal client is required, not optional. It verifies the Workflow chain binding and is the
fallback authority for resolving a staged batch when a Worker died in the acknowledgement window.
The common path remains a backend-only read after connection.

`tail()` is only a position boundary; it does not correlate a later Update with a particular output
record. A protocol that supports concurrent Updates must also return a stable operation identifier
such as `turn_id`, and the subscriber must scan for the matching `turn_started(turn_id)` rather than
adopting the first turn marker after the boundary.

## Visibility and History cost

With no retained input wait, a normal Workflow Task completes after its activation and output is
staged on that completion. With retained external-input waits, three conditions can flush output:

1. the activation produces another server-bound command, so the task already has to complete;
2. the output batch reaches its record or logical-byte budget; or
3. the output-flush deadline expires.

The third is a distinct Core timer, armed by the first output in an empty batch and clamped below the
ordinary rollover deadline. It completes with an output-flush terminal, writes the output marker,
and requests a new Workflow Task so input consumption continues. Replay follows the recorded
terminal and never arms a wall-clock output deadline.

### Output flush and input parking use one terminal race

The output deadline does not run beside the input direction's park handshake without coordination.
Core serializes the output-flush, idle/all-fenced park, readiness, ordinary rollover, and
server-command completion transitions for the open Workflow Task; exactly one terminal transition
wins.

- If `ParkSetConfirmed` wins, the output batch is staged and its marker is included before that park
  completion is sent to the server. The output timer is cancelled; parking already provides an
  earlier visibility boundary.
- If stream readiness wins, the current park preparation is rolled back by the existing input
  protocol and the output timer remains armed for the still-open Workflow Task.
- If the output-flush deadline wins while `PrepareExternalStreamPark` is outstanding, Core aborts
  that quiescence generation and cancels the preparation. Python removes every intent installed by
  the cancelled attempt under the existing cancellation/owed-removal rules. Only after that
  rollback is accounted for — removal confirmed or its retry recorded in the manager's owed ledger
  — does Core stage the output and complete with `force_new_wft = true`. The wait generations
  remain active and are reconstructed in the replacement task; the output flush never confirms a
  park generation.
- A late `ParkSetConfirmed`, readiness result, or deadline result for the losing transition is stale
  and cannot produce a second marker or completion.

The output-flush deadline is reduced by `min` across non-empty output topics and can only move
earlier as a shorter-latency topic joins the batch. It is clamped below ordinary rollover; an idle or
all-fenced park may still finish earlier and flush the same batch. These precedence and generation
rules are part of the required latency-flush ADR, not an implementation detail left to timer order.

The visibility guarantee is conditional, not a delivery SLA: under healthy dependencies a
Workflow-originated record is visible after at most `max_publish_latency` plus staging and Workflow
Task completion latency. A backend outage blocks that completion, and an ambiguous server outcome
can hold the client at a pending barrier until History decides it.

Marker **bytes** scale with topic/segment metadata rather than record payload bytes. History
**events** scale with output flushes: each latency- or capacity-driven flush adds an output marker
and the surrounding Workflow Task lifecycle events. Increasing the latency window batches more
output at the cost of staler clients. High-rate token deltas should be emitted by an Activity-side
`ExternalOutputStreamProducer`; Workflow publishes are for coarser deterministic lifecycle and
protocol events.

## Stream identity and isolation

Direction is part of the physical identity:

```text
(namespace, workflow ID, first execution Run ID, direction, stream name)
```

It is not a user-controlled prefix added to `stream name`. A topic named `events` may exist in both
directions without either side consuming the other's records, idempotency keys, or coordination
metadata. Providers may represent this with distinct key types or an internal direction component,
but key derivation must remain injective.

The first execution Run ID keeps the output stable across Continue-As-New and prevents a later
Workflow using the same Workflow ID from inheriting the earlier chain's output.

## Commit protocol

### Stage-attempt identity

The out-of-sandbox Worker runtime mints a cryptographically unique, opaque `stage_token` for every
attempt to stage one Workflow Task's output. The token is not derived from a
`WorkflowTaskScheduled` or `WorkflowTaskStarted` event ID: speculative Workflow Tasks may discard
those events and later reuse their IDs. It is never exposed to Workflow code.

One stage token covers the flush, with `(stage_token, topic, publish_index)` identifying each record
inside its topic sub-batch. Repeating a stage after losing the backend acknowledgement reuses the
same token and manifest and is idempotent. Re-executing the Workflow Task after rejection, timeout,
or Worker loss mints a fresh token, even if the server reuses every speculative event ID and the
logical output is byte-identical.

The staged metadata also carries a `history_floor_event_id`. Core derives it by locating this
Workflow Task's own `WorkflowTaskScheduled` in the ordered History view used to build the activation
and taking the event immediately preceding that Scheduled event. It does **not** try to decide
whether the Scheduled or Started events it was handed are persisted. For a speculative task those
events may disappear, but their predecessor is still the durable boundary before the speculative
transaction.

Reconciliation scans strictly above the floor. The derivation is mandatory because the safety
direction is asymmetric: a floor moved backward across a prior Workflow Task closing event lets a
resolver see that old close while this task is still in flight and abort output which later commits.
A floor moved forward, while still below this task's possible outcome, can only postpone resolution.
Core refuses to stage if it cannot identify the exact predecessor; an implementation must never
substitute a conservative lower event ID. The marker carries the unique token; the floor only says
where a resolver may begin looking for the first outcome capable of deciding that token.

An immutable manifest accompanies the staged batch:

- provider and format version;
- stage token, History floor, topic, and sub-batch identity;
- record count;
- a rolling fingerprint of the logical, pre-codec payloads and control records; and
- the logical, pre-codec byte count used for capacity enforcement.

The fingerprint detects a retry of the *same stage operation* trying to reuse its token for
different logical output without requiring encoded bytes to be deterministic. A payload codec may
use randomness; the first successfully staged encoded bytes remain authoritative while the logical
fingerprint is what a repeated stage compares. A new Workflow Task execution uses a new token and
cannot conflict with an orphaned speculative attempt.

"Logical" has one exact boundary: the Workflow sandbox runs the deterministic `PayloadConverter`,
then constructs a versioned canonical frame without serializing the `Payload` protobuf itself. For
fingerprint version 1, each record frame contains unambiguous unsigned length prefixes around:

1. the UTF-8 topic bytes and record kind;
2. every `Payload.metadata` key/value pair, keys sorted lexicographically by their UTF-8 bytes; and
3. the raw `Payload.data` bytes.

The logical byte count is the sum of those canonical frame lengths. The batch fingerprint is
SHA-256 over the ordered, length-prefixed frames. Sorting the metadata is load-bearing: it is a
protobuf map, and default protobuf serialization does not promise map-entry order. The fingerprint
format version rides in the manifest so a later canonicalization cannot be mistaken for the current
one.

The out-of-sandbox runtime subsequently applies the `PayloadCodec` and any external payload-storage
transform. A custom `PayloadConverter` used by Workflow code remains subject to Temporal's ordinary
determinism requirement; a randomized or version-changed `PayloadCodec` may alter stored bytes and
length without altering the logical identity or segmentation.

### Successful path

```text
Workflow activation
  -> publish values into a bounded language-side batch
  -> Worker runtime mints a unique stage token
  -> Core asks the out-of-sandbox runtime to stage the batch under that token
  -> backend durably stores immutable records as PENDING
  -> runtime acknowledges the stage
  -> Core completes the Workflow Task with a compact output marker
  -> Temporal Server commits the marker
  -> Worker marks the staged batch COMMITTED
  -> client watch may yield the records
```

The marker contains the stage token and compact manifest, never the payloads. Marker size is
therefore proportional to topic transitions and retained-task activation segments rather than
record payload bytes. A hard manifest byte budget forces Workflow Task rollover before the marker
can approach the server event-size limit.

Core must not send the server completion until staging is durable. The backend must not let a reader
pass a pending batch. Together those rules prevent both missing committed output and speculative
output reordering.

### Failed and ambiguous paths

| Failure point | Required outcome |
|---|---|
| Before staging | No backend records and no marker |
| During staging, outcome unknown | The same token is retried while its runtime exists; otherwise the batch remains pending for reconciliation |
| Stage succeeds, Workflow Task is explicitly rejected or fails | Batch is marked aborted and readers skip it |
| Server accepts the marker, Worker dies before commit | Batch remains pending; a reader or reconciler proves the marker from History and commits it |
| Commit acknowledgement is lost | Repeating commit is an idempotent success |
| Pending batch has no provable History outcome yet | Readers stop at it; they do not guess or pass it |
| History contains a later Workflow Task closing event or Workflow closure without this token | Batch is idempotently aborted |
| History needed to decide has expired | Integrity failure; never expose or silently discard the batch |

A pending batch is a barrier in topic order. Later Activity records may already be durable, but a
client cannot yield them until every preceding batch is committed or aborted. Without that barrier,
an Activity scheduled by the producing Workflow Task could overtake the Workflow events that
logically preceded it.

### Reconciliation is part of correctness

The acknowledgement window exists even when the Worker performs the normal post-commit transition:
the process can always die after the server commit and before the backend commit. Recovery cannot
depend on another Workflow Task because the committed task may have completed the Workflow.

Therefore both are required:

1. Workers opportunistically reconcile pending batches for the chains they service.
2. `ExternalOutputStreamClient` resolves a pending head lazily by reading the relevant History and
   matching the compact marker.

An optional standalone reconciler can keep the slow path away from readers, but it is an operational
optimization rather than the only recovery mechanism. Backend credentials alone are insufficient
to resolve pending state; the resolver also needs permission to read the Workflow's History.

Resolution is a positive predicate over History, never a timeout and never absence alone. The
resolver fetches a complete, strongly consistent History response from the staged
`history_floor_event_id` through the current durable end; it does not decide from a partial page or
an eventually consistent cache. It then applies these rules in order:

1. If an external-output marker carrying this exact `stage_token` exists, commit the batch.
2. Otherwise, if History contains a `WorkflowTaskCompleted`, `WorkflowTaskFailed`, or
   `WorkflowTaskTimedOut` event above the floor, or the Workflow has closed, abort the batch. A
   committed task's marker and closing event belong to the same durable server transaction; once a
   complete response contains that closing boundary without the token, the omitted token cannot be
   appended retroactively.
3. Otherwise the outcome is still undecidable and the batch remains pending.

This rule survives speculative Workflow Tasks whose Scheduled and Started events were never
persisted. A discarded attempt's unique token cannot match the retry's marker, and the first durable
Workflow Task closing boundary above its floor proves the orphan absent. Reused event IDs and
byte-identical manifests are irrelevant because neither is the identity.

The pending record remains a head-of-line barrier until that proof arrives. There is deliberately no
finite timeout guarantee for an abandoned speculative task. If its Worker dies, its Update caller
disconnects so the server never redelivers it, and nothing else touches the Workflow, the
speculative Scheduled/Started/TimedOut events may all remain absent and neither rule can fire. The
batch stays pending until a later durable Workflow Task outcome or Workflow closure provides the
positive boundary; if History retention expires first, it becomes an integrity failure.

The usual harness path resolves sooner because the stream reader is also waiting for the Update and
keeps that operation alive, but that is an application property rather than a stream guarantee.
Worker unavailability, server outage, History read failure, caller disconnect, or retry backoff can
extend the wall-clock delay without bound. Readers report that they are waiting on reconciliation
rather than presenting it as ordinary backend idleness. A future resolver may add another positive
proof source, but it may not turn elapsed time or a missing token alone into abort.

## Replay and determinism

On replay, Workflow code executes the same `publish()` calls. Core matches their topic, order,
control kind, logical size, and logical fingerprint against the recorded output marker. It does not
mint a new token, stage records, commit records, arm an output-flush timer, or recompute a batch
boundary. A mismatch is nondeterminism.

Batch segmentation is recorded and reproduced, not inferred from the current codec or current
runtime limits. The marker groups output commands into the same activation segments the live
retained Workflow Task observed, including an empty segment where a live activation performed a
drain but published nothing. Replay performs exactly the same number of event-loop drains as live
execution. As in ADR-018, a marker with *k* segments performs the first *k - 1* drains in the replay
driver and leaves the final drain to the activation's ordinary trailing `_run_once`.

Input and output annotations for one retained Workflow Task share this segment schedule. Their
drivers must not each perform *k* drains and double the live schedule; they attach their deliveries
and output-command expectations to one ordered sequence of segment frames. Capacity- and
latency-driven output terminals are part of that recorded schedule. The live budget check and
wall-clock deadline are disabled on replay, so a changed compression ratio or Worker option cannot
move the suspension boundary.

The marker is the durable fact that a batch belongs to the execution. Backend committed state is
not accepted as a substitute: backend state can outlive History, can be copied incorrectly, and is
not part of the deterministic event sequence.

Workflow code never observes:

- provider offsets;
- whether a client is connected;
- when post-commit promotion completes; or
- backend retry timing.

None of those facts may affect Workflow control flow. An application that needs a durable client
acknowledgement sends it back through a normal Update, Signal, or the existing external input
stream; output-stream delivery itself is not an acknowledgement channel.

## Backend contract additions

The exact interface is provider work, but its semantics must include:

- atomically stage an immutable, ordered batch as pending;
- idempotently commit or abort that exact batch;
- reject reuse of one stage token with a different manifest;
- read committed records strictly after a cursor while stopping at unresolved pending data;
- inspect a pending stage token, History floor, Run identity, and manifest for reconciliation;
- retain resolution metadata as long as a cursor or retained History may refer to it; and
- garbage-collect abandoned staging only after History proves abort or after an operator resolves an
  integrity incident.

Record immutability remains mandatory. Commit and abort are separate coordination metadata; they do
not rewrite record bytes. A Redis provider can use an append-only record log plus a status index and
atomic scripts. Providers that cannot stop a read at unresolved data do not satisfy the output
contract even if they satisfy the current input contract.

Input and output conformance suites are separate. Requiring every existing input backend to support
transactional output staging would turn an additive feature into a breaking change; a provider may
declare input-only support.

## Ordering guarantees

For one topic, clients observe:

1. Workflow records in publish order within their committed Workflow Task;
2. committed batches in backend order; and
3. directly produced Activity records at their assigned backend offsets, without passing an
   unresolved predecessor during reads.

Retries do not create a second visible record for the same publish identity. Aborted batches occupy
coordination positions but yield no data.

This is a barrier guarantee, not one deterministic total order between independently running
producers. An Activity may append while a Workflow stage is pending. If that stage commits, readers
observe its records before the Activity because the pending barrier was already ahead of it. If the
stage aborts, it yields nothing and a newly executed Workflow Task's replacement batch may append
after the Activity. That relative order can therefore change across a Workflow Task retry, and no
Workflow code may depend on it.

There is no cross-topic transaction. If one Workflow Task publishes to topics A and B, both markers
commit in Temporal together, but backend promotion and client observation may occur at different
times. A consumer needing one total order should publish envelopes to one topic and put the logical
channel in the envelope.

## Capacity and backpressure

Output payloads do not go to the server, but they still cross the Workflow sandbox and the
language/Core boundary. The runtime enforces record and **logical pre-codec byte** budgets per
output batch, which means per Workflow Task rather than per activation. A retained Workflow Task
may span many activations; all of their accepted output counts against the same batch bound. When
accepting another record would cross a limit, `await publish()` yields and requests Workflow Task
rollover; the next Workflow Task resumes after the preceding batch is staged.

Logical size is the deterministic serialized `Payload` protobuf size defined above, before any
`PayloadCodec`. Encoded size is recorded for metrics and provider admission but never chooses a
Workflow-visible boundary. A provider may stream one logical stage through several idempotent
transport writes under the same pending token, then seal its manifest; it must not force a new
Workflow segment merely because a codec changed compression ratio.

Replay takes segment boundaries from the marker and skips both capacity checks and the
output-flush deadline. It still validates the recorded logical counts and fingerprints. A Worker
with a lower current live limit can replay an older larger batch because the bytes are already
staged; applying the new limit would create nondeterminism instead of providing protection.

A single record larger than the configured maximum is rejected before it enters the batch. The SDK
must not split a user value into transport fragments unless the public codec contract explicitly
supports it.

Client-side subscribers have independent record and byte prefetch limits. A slow client affects its
own memory and retention position, not Workflow Task progress.

## Continue-As-New, completion, and retention

The output key uses the first execution Run ID, so Continue-As-New keeps the same topic. Staged
metadata names the current Run, and the independently unique stage token prevents retries and
successive Runs from colliding even when speculative Workflow Task event IDs are reused.

Continue-As-New is not a terminal record. An explicit `finish()` remains visible across the chain
and prevents later publishes to that topic. The finished-topic set, including each topic's provider
binding and format version, travels in a reserved must-understand Continue-As-New header. The
successor restores it before Workflow code runs and deterministically rejects a later `publish()`;
it never asks mutable backend state whether the topic was finished. This is the output analogue of
ADR-022 and ADR-039.

Normal Workflow completion may record terminal status for every still-open output topic in the
final batch, but failure, cancellation, and termination cannot rely on a final Workflow command. A
following client should combine backend watches with Temporal execution status when it needs to stop
automatically.

Committed records and their resolution metadata must remain available for at least the advertised
client resume window. Pending resolution requires the associated History to remain available.
Operators must configure backend retention and Temporal retention together; deleting either side
first can turn a recoverable acknowledgement window into an integrity failure.

## Availability and failure classification

Staging is on the Workflow Task commit path. If the output backend is unreachable, times out, or
rejects a transient operation, Core does not send the server completion: the Workflow Task fails
with the external-storage cause and normal Temporal retry, and the Workflow makes no progress until
staging succeeds. This maps to the transient `StreamStorageError` row in
`spec/failure-taxonomy.md`, with a distinct output-stage operation label on metrics.

That cost is deliberate. Completing the Workflow Task without a durable stage can permanently lose
committed output. Applications that prefer Workflow progress over output durability should use a
separately specified best-effort telemetry sink, not weaken this stream's contract.

A missing staged record, conflicting immutable manifest, or History that expired before a pending
token could be resolved is `StreamIntegrityError`: the reader or reconciler blocks rather than
choosing commit or abort. Decode failures remain `StreamDecodeError`. Backend or Temporal outages
during client reconciliation are transient storage failures and leave the barrier pending.

## Security and authorization

Output records are application data and inherit the `DataConverter` and codec configured for the
Workflow's serialization context. The output producer and client bind the converter to the same
namespace and Workflow ID context before encoding or decoding.

Backend possession is not proof that a caller may read every Workflow stream. A production provider
needs key-level authorization or a trusted service boundary. The SDK API should not imply that a
raw Redis URL is a suitable browser credential.

History access used for reconciliation is also privileged. A service may split ordinary committed
reads from reconciliation so most readers hold only backend read permission and delegate pending
resolution to a trusted component.

## Temporal Agent Harness migration

Once this proposal is implemented, the harness can migrate without changing its external event
vocabulary:

1. `AgentWorkflowRunner` publishes `AgentEvent` envelopes to one external output topic.
2. model and tool Activities use `ExternalOutputStreamProducer` for live deltas and lifecycle
   events on that same topic;
3. `AgentClient` and the web service subscribe with `ExternalOutputStreamClient` and persist opaque
   cursors instead of dense integer offsets;
4. the client obtains `tail()` before sending an agent-message Update, uses that earlier boundary
   as the merge start hint, then scans for the exact `turn_id` returned by the Update rather than
   accepting another concurrent turn's marker; and
5. the Nexus adapter carries opaque cursor tokens and reads the backend instead of calling the
   private WorkflowStream poll Update.

Taking `tail()` before the Update admits harmless earlier events but cannot miss the subsequent
`turn_started`. Matching `turn_started.turn_id` to the Update result makes the scan safe when another
caller submits a concurrent Update after the same boundary. This removes the harness's dependency on
the private in-Workflow `_on_offset()` head query, which an externally assigned opaque offset cannot
reproduce deterministically inside the Workflow.

A rollout may dual-publish to `contrib.workflow_streams` and the external output topic for
comparison, but consumers choose exactly one authoritative log. Merging both would duplicate every
event and give two incomparable cursor spaces.

## Required validation

An implementation is not complete without tests that force every acknowledgement window:

- no record becomes visible before the Workflow Task marker commits;
- an explicitly failed or rejected task leaves no visible output;
- a discarded speculative Workflow Task stages one token and its redelivery stages another when
  event IDs are reused, both when logical output is identical and when its manifest changes, and
  History commits only the token in its marker;
- a pending speculative token aborts after the first durable Workflow Task closing boundary above
  its History floor, without waiting for nonexistent Scheduled or Started events;
- the History floor is the event immediately preceding the producing task's Scheduled event; a
  previous Workflow Task close is excluded from the scan, and mutating the floor below it makes the
  test fail by demonstrating the otherwise-possible false abort;
- a disconnected speculative Update with no later durable event remains pending rather than being
  aborted by elapsed time, eventually classifying retention loss as integrity failure;
- crash after stage and before server completion resolves to abort or retry without a phantom;
- crash after server completion and before backend commit is repaired by a cold client after the
  Workflow has completed;
- repeated stage, commit, abort, and reconciliation calls are idempotent;
- identity reuse with a different logical fingerprint is rejected;
- replay validates output commands but performs no backend write, token mint, capacity split, or
  wall-clock flush;
- a codec whose encoded size changes between live execution and replay preserves the marker's batch
  and activation segmentation;
- a multi-activation retained Workflow Task reproduces exactly the live drain count when both input
  and output annotations are present, rather than summing both drivers' drains;
- a Workflow-originated publish made during a retained Workflow Task becomes visible within the
  configured output-flush quantum under healthy dependencies;
- several publishes inside one flush window produce one output marker, while crossing *n* latency
  windows produces *n* markers and the corresponding Workflow Task lifecycle cost;
- a pending batch blocks later Activity output from overtaking it;
- Activity retry produces one visible record per idempotency identity;
- cursor resume has neither gaps nor duplicates across Workflow Task rollover and
  Continue-As-New;
- input and output topics with the same user name remain isolated;
- randomized payload codecs do not make a logical retry conflict;
- two logically identical Payloads whose metadata maps were inserted in opposite orders have the
  same versioned canonical frame, byte count, and SHA-256 fingerprint;
- output-flush/park races in both orders produce exactly one Workflow Task terminal and one output
  marker; an output winner invalidates the quiescence generation, rolls back every installed intent,
  and forces a replacement Workflow Task, while a park winner stages the output before completing;
- concurrent message Updates scan from their boundaries to their own returned `turn_id` and cannot
  adopt each other's `turn_started`;
- a finished topic survives Continue-As-New through the reserved header and rejects a successor
  publish without reading backend state;
- an output-stage outage blocks Workflow Task progress and is classified as transient external
  storage failure;
- a missing staged record or expired History produces an integrity failure; and
- marker and activation batch budgets are hard bounds, including one oversized topic name and one
  oversized record.

Every crash test must prove that its injected failure reached the intended boundary before its
result is accepted; the existing verification-hazard rules apply unchanged.

## Promotion work after acceptance

This proposal does not change the accepted specs by itself. Acceptance requires moving the durable
rules into their single normative homes and recording at least these independent decisions:

- unique Worker-minted stage tokens plus the History-floor reconciliation predicate;
- latency-driven output flush, its Workflow Task/History-event tradeoff, and its precedence and
  quiescence-generation effects against the input park handshake;
- per-Workflow-Task logical capacity with marker-driven replay segmentation shared by input and
  output;
- the pending-batch ordering barrier and its deliberately limited mixed-producer guarantee; and
- finished-topic state in the must-understand Continue-As-New header.

The validation cases above then become entries in `required-tests/` with concrete mappings. The
vendored Core pointer must move with those lists before the Python M1/M2 gates can claim the cases;
otherwise verification hazard 3 leaves them unarmed. Until that promotion is complete, current
`spec/`, `decisions/`, and required-test lists continue to describe only the input direction.

## Alternatives considered

### Publish through an Activity

An Activity provides durable retries, but its argument or result payload is recorded in History.
Passing the output value as the Activity input therefore defeats the feature. Passing only a key
requires the value to have been written externally already and returns to the same commit problem.

### Write after Workflow Task completion

This prevents speculative output but loses a committed batch if the Worker dies in the
server-accepted/backend-not-written window. There is no later Workflow Task to repair it when the
task completed the Workflow.

### Write visibly before Workflow Task completion

This survives a Worker crash but leaks events from rejected tasks. A client can render an answer the
Workflow never committed, and later replay can produce a different answer under the same turn.

### Keep using `contrib.workflow_streams`

This remains valid for moderate-volume streams and requires none of the protocol above. It does not
meet the goal of keeping payload bytes out of History.

### Deliver Activity output directly and Workflow output through the old stream

This hybrid reduces token Signal volume and may be a useful intermediate harness migration. It
creates two logs, two cursor spaces, and no single ordering between Activity and Workflow events, so
it is not the end-state proposed here.

## Open decisions before acceptance

- Final public names for the output entry point, producer, client, topic, and yielded item.
- Whether lazy client reconciliation is mandatory in every client or delegated through a pluggable
  resolver interface with a mandatory default.
- Whether explicit `finish()` is sufficient or successful Workflow completion also synthesizes
  topic terminal records.
- The default `max_publish_latency`; the mechanism and its History-event tradeoff are required even
  if the initial default changes.
- Whether providers may expose output-only support as well as input-only support.
- The default committed-output retention and the operator API for resolving an integrity incident.
- Whether the first release supports Activity publishers in the same topic or restricts the stream
  to Workflow batches until mixed-producer ordering has its own conformance suite.

Until these decisions are accepted and recorded as ADRs, this document describes a candidate design
and must not be treated as the behavior of the current implementation.
