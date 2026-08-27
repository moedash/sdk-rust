# External Workflow Streams — overview

External Workflow Streams move high-volume stream payloads out of Temporal History and into a
pluggable stream backend such as Redis Streams. In the input direction a Workflow subscribes and the
SDK runtime reads the backend directly. In the output direction Workflow code or an Activity
publishes and an external client reads committed records directly. **No Temporal Server changes are
required.**

Deterministic replay is preserved with compact marker events describing consumed input ranges,
availability boundaries, and logical output manifests. Individual stream payloads are never copied
into History.

## The role of Signals, stated precisely

> In the input direction, Signals provide a server-visible wakeup whenever no open Workflow Task
> can accept local readiness. They never carry stream payloads.

That is broader than "Signals wake a parked Workflow". A Workflow Task that completes with
server-bound commands, or that rolls over, leaves subscriptions active with no park generation
installed, and a Signal is the wakeup there too — see `spec/wft-lifecycle.md`.

## Core changes are unavoidable

There is **no Python-only path** that keeps payloads out of History. A local-activity-based
implementation would get Workflow Task retention, an idle timer, and marker recording for free, but on
replay local-activity results come from the marker — so payloads would have to live in History,
defeating the feature's entire purpose. `ReplayExternalStreams` is precisely the hook that does not
exist today.

Live Workflow interaction and deterministic replay are one indivisible capability. The SDK exposes no
live-only mode that reads external records without recording their cursor boundaries: a Workflow
Task retry, eviction, or Worker restart would otherwise have no durable position from which to
distinguish a record already observed from one not yet delivered.

## Scope and lifetime

- A stream is identified by `(namespace, workflow ID, first execution Run ID, direction, stream
  name)`. Direction physically isolates an input and output topic with the same user-visible name.
  The first execution Run ID prevents collisions after Workflow ID reuse while remaining stable
  across a Continue-As-New chain.
- The stream spans the full Continue-As-New chain. A new Run continues from the cursor committed
  by the preceding Run — together with the binding that cursor is a position in — restored from a
  reserved internal header on the Continue-As-New command rather than from mutable backend state
  (ADR-022, ADR-039).
- Input streams remain open across producer calls and failures. Output topics end only through
  explicit `finish()` or `finish_writing()`; Workflow closure synthesizes no terminal.
- Workflow-originated output is implemented alongside the input direction. Its normative rules are
  in the specs and ADR-044 through ADR-048, while remaining feature-wide validation coverage is
  tracked in `required-tests/`.

## API sketch

The consumer-side and producer-side handles are **distinct types**. They are not the same object
passed across a process boundary: a Workflow handle is bound to the running Workflow's identity
and the Worker's configured backend, while a producer handle must be constructed explicitly from
credentials the producer holds (ADR-019).

```python
# Workflow (consumer side)
streams = external_stream.with_options(idle_timeout=timedelta(seconds=1))
tokens = streams.topic("tokens", type=str)

async for token in tokens.subscribe():
    process(token)
```

```python
# Activity or plain external process (producer side)
producer = await ExternalStreamProducer.connect(
    backend=RedisStreamBackend(url=..., credentials=...),
    workflow=WorkflowChainKey(        # fully explicit; nothing is inferred
        namespace=...,
        workflow_id=...,
        first_execution_run_id=...,
    ),
    data_converter=...,
    session_id=...,                   # stable across retries
)
tokens = producer.topic("tokens", type=str)

await tokens.publish(token)
await tokens.finish_writing()
```

```python
# Workflow (output publisher)
events = external_output_stream.with_options(
    max_publish_latency=timedelta(milliseconds=100),
).topic("events", type=AgentEvent)

await events.publish(event)
await events.finish()
```

```python
# Activity or external process (direct output producer)
producer = await ExternalOutputStreamProducer.connect(
    backend=output_backend,
    workflow=WorkflowChainKey(...),
    client=client,
    session_id=retry_stable_session,
)
events = producer.topic("events", type=AgentEvent)
await events.publish(event)
await events.finish_writing()
```

```python
# External output client
reader = await ExternalOutputStreamClient.connect(
    backend=output_backend,
    workflow=WorkflowChainKey(...),
    client=client,
)
async for item in reader.topic("events", type=AgentEvent).subscribe():
    persist(item.data, resume_at=item.offset)
```

Input `subscribe()`, Workflow output `publish()`/`finish()`, and direct output
`publish()`/`finish_writing()` exist on distinct handles. Input `finish_writing()` appends a write
fence and does not close the stream; output `finish()` and output `finish_writing()` append the
ordered `FINISH` terminal (ADR-048). A direct finish closes that producer's handle but does not
write the Workflow's Continue-As-New state, so mixed publishers must designate one terminal owner.

Multiple streams use normal Workflow concurrency or merge/select APIs:

```python
async for source, item in streams.merge(tokens, tool_events):
    process(source, item)
```

### Worker configuration

One stream backend is configured on the Worker, alongside the existing plugin and interceptor
options. Workflow code never holds the connection or chooses among backend instances:

```python
Worker(
    ...,
    external_stream_backend=RedisStreamBackend(url=..., credentials=...),
)
```

The backend instance lives outside the Workflow sandbox. `StreamBackend` and `OutputStreamBackend`
are independent capabilities: input-only providers remain valid, output-only providers can serve
direct producers and clients, and Workflow-originated output currently requires a Worker provider
implementing both. The feature is experimental, so its annotation and continuation encodings have
one current format and no legacy compatibility mode.

## Naming

This feature coexists with the shipped `temporalio.contrib.workflow_streams` package rather than
replacing it — the two are mirror images, not two implementations of one idea (ADR-001). Names
must not collide.

| Concern | This feature |
|---|---|
| Public module | `temporalio.contrib.external_workflow_streams` |
| Workflow entry points | `external_stream`, `external_output_stream` |
| Input handles | `ExternalStreamTopic`, `ExternalStreamProducerTopic` |
| Output handles | `ExternalOutputStreamTopic`, `ExternalOutputStreamProducerTopic`, `ExternalOutputStreamClientTopic` |
| Output client item | `ExternalOutputStreamItem` |
| Reserved Signal | `__temporal_external_stream_wake` |

**No name in this feature may begin with `__temporal_workflow_stream`.**

## Non-goals

- Transporting stream payloads through Signals or Temporal History.
- Exactly-once producer execution; the backend adapter provides idempotent append semantics
  instead (ADR-020).
- Changing Temporal Server protocols or persistence.
- Closing a stream when one producer finishes writing.
- Work-sharing between two subscriptions inside one Workflow (ADR-021).
- A global or cross-topic order between Workflow and direct output producers (ADR-047).

## Result

History **event** cost scales with Workflow Task input-consumption batches, output latency/capacity
flushes, idle-to-active transitions, and rollovers rather than directly with stream item count.
History **byte** cost per marker is capped by the annotation and output-manifest budgets, which force
an additional rollover or reject a single unrepresentable batch rather than growing a marker without
bound (ADR-007, ADR-046).

Within that cap, input annotation bytes scale with cross-stream schedule transitions and sparse
control positions. Output manifest bytes scale with topics and activation segments, not payload
bytes; logical record counts and fingerprints are aggregate fields. Structural immutability is what
keeps input replay validation compact (ADR-003). So the precise input claim is:

- **Single stream:** marker bytes do **not** grow with item count. One run encodes 100,000
  records.
- **Alternating multi-stream:** marker bytes grow with schedule transitions, and total History
  cost is bounded by the budget converting that growth into additional Workflow Tasks.

Workflows can consume high-volume input and publish externally visible output efficiently while
retaining deterministic replay, durable wakeup, and recoverable output commit proof. Input wakeup
and output visibility boundaries are stated explicitly in `spec/wft-lifecycle.md` rather than
assumed.
