# Reserved wake Signal

The server-visible wakeup used whenever no open Workflow Task can accept local readiness. It never
carries stream payloads.

## Where it is used

```text
new record
    ├─ open WFT  → Python readiness call → Core activation
    └─ otherwise → Temporal Signal → server creates WFT
```

"Otherwise" is three states, not one — parked, cached with no open Workflow Task, and evicted. All
three send this Signal; they differ in what the watcher does afterwards. See
`core-lang-protocol.md`.

## Signal name

`__temporal_external_stream_wake`. Fixed, versioned by the envelope rather than by the name, and
distinct from every `__temporal_workflow_stream_*` name already reserved by
`temporalio.contrib.workflow_streams` (ADR-001).

## Envelope

Core must read this Signal's fields, and Core has no access to the user's `DataConverter` or codec.
The envelope is therefore defined at the protocol level and is deliberately **not** a user payload.

A single argument whose `Payload` uses metadata `encoding = "binary/protobuf"` and
`messageType = "coresdk.external_stream.WakeSignal"`, carrying:

```protobuf
message WakeSignal {
  uint32 envelope_version = 1;      // starts at 1; Core rejects unknown versions harmlessly
  string stream_name = 2;
  uint32 wait_id = 3;
  // 0 is reserved and means "no park generation" -- an unparked wake. Park
  // generations are quiescence generations, which start at 1.
  uint64 park_generation = 4;
  string first_execution_run_id = 5; // chain identity, not the current run
  string producer_session_id = 6;    // diagnostics only
}
```

## Parked and unparked wakes use the same envelope

`park_generation = 0` means the sender knows of no confirmed park for this wait and is asking for a
Workflow Task anyway (ADR-023). Core validates chain identity for an unparked wake and otherwise
accepts it as a recheck request: the runtime rechecks every active subscription on wakeup
regardless, so an unnecessary unparked wake costs at most one empty Workflow Task.

A **non-zero** generation that the current Run does not recognize is still ignored as stale, because
there the sender is making a claim that turned out to be wrong.

## Three properties that make this work

- **Codec bypass.** The producer sends this Signal through a raw Workflow Service
  `SignalWorkflowExecution` request built with the protocol's own serialization, *not* through the
  user's `DataConverter` (ADR-025). A user codec that encrypts payloads would otherwise make the
  envelope unreadable to Core, which is the component that must read it. The Signal carries no user
  data, so bypassing the codec leaks nothing.
- **Stable request ID.** The Temporal `request_id` on the signal request is derived deterministically
  from `(namespace, workflow_id, first_execution_run_id, stream_name, wait_id, park_generation)`, so
  a producer retrying after an ambiguous failure produces the identical request and the server
  deduplicates it. Generating a fresh UUID per attempt — which the public Python Signal path does —
  would defeat this, which is another reason the wake path does not reuse it.

  For an **unparked** wake the generation is 0 and therefore carries no attempt identity of its own,
  so the derivation additionally includes the sender's identity and a per-sender monotonic wake
  counter, both held fixed across retries of that one attempt. Without that, two Workers shutting
  down at different times would derive the same request ID and the server would deduplicate the
  second wake away — turning a correct retry mechanism into silent loss.

  That sender identity is unique per sender **instance**, drawn once when the sender is constructed
  and held for its lifetime. The client identity is not enough: two Workers in one process share a
  `Client` and each one's counter restarts at 1, so their first unparked wakes would derive
  byte-identical request IDs and the collision above is exactly what happens. Drawn once rather
  than per attempt, because a value that changed between attempts would make the retry a second wake. The value is
  random, which is safe here because the wake Signal is sent from the Worker's own event loop and
  never from Workflow code, so it is not replay-visible; the client identity is carried alongside it
  so a request ID stays traceable to a client in server-side logs.
- **Chain identity.** `first_execution_run_id` lets Core classify a Signal that arrives after
  Continue-As-New: same chain and a live wait means wake, same chain and an unknown generation means
  ignore harmlessly, different chain means reject. The Signal is addressed to the Workflow ID without
  a Run ID, so it always lands on the current Run of the chain.

## Interception

Core intercepts the Signal before user Signal dispatch, decodes the envelope without a
`DataConverter`, validates chain identity and generation, and issues `ResolveExternalStreamWaits`.
The first valid Signal creates or accompanies the new Workflow Task; duplicate wake Signals are
harmless. Python rechecks **every** active stream rather than only the stream named by the Signal.

**Core suppresses the Signal from user handlers whether or not it validates**, so an unknown envelope
version or a stale generation can never reach Workflow code as an unhandled Signal.

## Producer send sequence

1. Append the record.
2. Observe or lease-claim the current park generation.
3. Send the Signal idempotently.

Only successfully appended records may trigger wakeup: a wake for a record that did not land produces
a Workflow Task that finds nothing.

**The Signal is sent whether or not the claim was granted.** Losing the claim means another producer
*intends* to send; it is not evidence that one did. A lease permits takeover once it expires, but it
schedules nobody to take over, so a producer that crashed between claiming and signalling strands
the generation until some later append happens along — and the producer that stayed silent has
already reported an acknowledged wake. Sending anyway costs almost nothing, because a **parked**
wake's request ID is derived from the generation and ignores sender identity: racing producers issue
byte-identical requests and the server collapses them into one wake. A granted claim therefore saves
a round trip, not a wakeup. What the claim is for, and what its lease buys, is in
`backend-contract.md`.

Wake signaling is independently retryable and idempotent. Retrying the wake step is a producer
obligation, not an automatic property; `publish()`'s acknowledged-wake contract (P6b) is what turns
it into one.

### All three steps after the append are inside the guarantee

`publish()` distinguishes exactly two outcomes: the append failed, or the append succeeded and the
wake did not. **Steps 2 and 3 both belong to the second**, not only step 3. A coordination call that
raises whatever the provider raises — a bare `ConnectionError` from observing the parked set or
claiming a generation — passes straight through a caller watching for the durable-but-unacknowledged
error, and takes the offset with it. The caller then has no statement about the record that already
landed, and its obvious move, retrying `publish()`, appends a **second** record: the sequence number
has advanced and the idempotency key with it.

The two failures differ in what recovers them, so the error says which:

| Failed at | `pending` | Recovery |
|---|---|---|
| Step 3, the Signal | the wakes still owed | re-send them verbatim (`retry_wake`) |
| Step 2, observe or claim | empty, `restart` set | call `wake()` again |

`retry_wake` refuses an empty list rather than returning quietly, because a no-op there looks like
recovery while the record stays durable and unannounced. Calling `wake()` again is safe: it
re-observes the parked set, and a parked wake's request ID is derived from the generation rather than
the sender, so a wake another producer already sent deduplicates against it.

`finish_writing()` carries the identical contract. The duplicate it prevents is a second write fence,
which reads back as a producer session that ended twice.
