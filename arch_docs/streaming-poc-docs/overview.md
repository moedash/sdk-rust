# External Workflow Streams — overview

External Workflow Streams move high-volume stream payloads out of Temporal History and into a
pluggable stream backend such as Redis Streams. A Workflow subscribes through the SDK and, while
a Workflow Task is open, the SDK runtime reads the external stream directly. **No Temporal Server
changes are required.**

Deterministic replay is preserved with compact marker events describing consumed offset ranges
and the availability/blocking boundaries observed during the original execution. Individual
stream items are never copied into History.

## The role of Signals, stated precisely

> Signals provide a server-visible wakeup whenever no open Workflow Task can accept local
> readiness. They never carry stream payloads.

That is broader than "Signals wake a parked Workflow". A Workflow Task that completes with
server-bound commands, or that rolls over, leaves subscriptions active with no park generation
installed, and a Signal is the wakeup there too — see `spec/wft-lifecycle.md`.

## Core changes are unavoidable

There is **no Python-only path** that keeps payloads out of History. A local-activity-based
implementation would get Workflow Task retention, an idle timer, and marker recording for free, but on
replay local-activity results come from the marker — so payloads would have to live in History,
defeating the feature's entire purpose. `ReplayExternalStreams` is precisely the hook that does not
exist today.

## Scope and lifetime

- A stream is identified by the tuple `(namespace, workflow ID, first execution Run ID, stream
  name)`. The first execution Run ID prevents collisions after Workflow ID reuse while remaining
  stable across a Continue-As-New chain.
- The stream spans the full Continue-As-New chain. A new Run continues from the cursor committed
  by the preceding Run — together with the binding that cursor is a position in — restored from a
  reserved internal header on the Continue-As-New command rather than from mutable backend state
  (ADR-022, ADR-039).
- A stream remains open across producer calls and producer failures. Independent stream
  identities and lifecycles are future work.

## API sketch

The consumer-side and producer-side handles are **distinct types**. They are not the same object
passed across a process boundary: a Workflow handle is bound to the running Workflow's identity
and the Worker's backend registry, while a producer handle must be constructed explicitly from
credentials the producer holds (ADR-019).

```python
# Workflow (consumer side)
streams = external_stream.with_options(idle_timeout=timedelta(seconds=1))
tokens = streams.topic("tokens", backend="tokens-redis", type=str)

async for token in tokens.subscribe():
    process(token)
```

```python
# Activity or plain external process (producer side)
producer = await ExternalStreamProducer.connect(
    backend="tokens-redis",           # name registered on the Worker, or an explicit
                                      # provider instance for non-Temporal processes
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

`subscribe()` exists only on the consumer handle; `publish()` and `finish_writing()` exist only on
the producer handle. `finish_writing()` appends a write-fence control record behind every publish
invoked before it on that stream, so its claim holds for concurrent callers too (ADR-040); it does
not close the stream.

Multiple streams use normal Workflow concurrency or merge/select APIs:

```python
async for source, item in streams.merge(tokens, tool_events):
    process(source, item)
```

### Worker configuration

Named stream backends are registered on the Worker, alongside the existing plugin and interceptor
options, so Workflow code names a backend rather than holding a connection:

```python
Worker(
    ...,
    external_stream_backends={"tokens-redis": RedisStreamBackend(url=..., credentials=...)},
)
```

Backend instances live outside the Workflow sandbox. Workflow code may only name them.

One further option exists, and it is a rollout control rather than a feature switch:

```python
Worker(
    ...,
    external_stream_continuation_schema_version=2,  # unset = the release's shipped stage
)
```

It selects the schema version this Worker *writes* into a Continue-As-New cursor header, which is
read by the successor Run's Worker and so must be a version that Worker can decode. Left unset it
takes the stage the release ships in. Raising it is the writer half of the two-stage rollout ADR-039
describes; it cannot be raised past what this Worker can itself read, and Worker construction fails
rather than the first Continue-As-New if it is.

## Naming

This feature coexists with the shipped `temporalio.contrib.workflow_streams` package rather than
replacing it — the two are mirror images, not two implementations of one idea (ADR-001). Names
must not collide.

| Concern | This feature |
|---|---|
| Public module | `temporalio.contrib.external_workflow_streams` |
| Entry point | `external_stream` (not `workflow_stream`) |
| Handle types | `ExternalStreamTopic`, `ExternalStreamProducerTopic` |
| Reserved Signal | `__temporal_external_stream_wake` |

**No name in this feature may begin with `__temporal_workflow_stream`.**

## Non-goals

- Transporting stream payloads through Signals or Temporal History.
- Exactly-once producer execution; the backend adapter provides idempotent append semantics
  instead (ADR-020).
- Changing Temporal Server protocols or persistence.
- Closing a stream when one producer finishes writing.
- Work-sharing between two subscriptions inside one Workflow (ADR-021).

## Result

History **event** cost scales with Workflow Task consumption batches, idle-to-active transitions,
and rollovers rather than with stream item count. History **byte** cost per marker is capped by
the annotation byte budget, which forces an additional rollover rather than growing a marker
(ADR-007).

Within that cap, marker bytes scale with cross-stream schedule transitions and with sparse
control positions, and with nothing else — because structural immutability is required of every
provider, no part of the encoding is per-record (ADR-003). So the precise claim is:

- **Single stream:** marker bytes do **not** grow with item count. One run encodes 100,000
  records.
- **Alternating multi-stream:** marker bytes grow with schedule transitions, and total History
  cost is bounded by the budget converting that growth into additional Workflow Tasks.

Workflows can consume high-volume streams efficiently while retaining deterministic replay,
durable wakeup, and normal Temporal failure recovery — with the wakeup durability boundary stated
explicitly in `spec/wft-lifecycle.md` rather than assumed.
