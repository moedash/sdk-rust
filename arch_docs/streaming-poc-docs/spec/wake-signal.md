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

### The append itself has an acknowledgement window

A backend commits on its own side and only then answers, so an `append()` that did not answer is not
an `append()` that did not happen. The Redis provider runs its atomic script server-side and receives
the result in a separate client-side step; a cancellation or a dropped connection between the two
leaves a durable record whose offset nobody holds.

That window is therefore a **third outcome**, `AppendNotAcknowledgedError`, and not a failure. It
carries the interrupted *operation* — the exact record, byte-identical and still holding its
`(session_id, sequence)`, together with the stream it was for, the wake it owed and the lease it
chose — and `resolve_append()` re-appends *that* record on *that* topic, defaulting to the wake the
interrupted call was going to send. One call is right for both histories, because a
repeat append of byte-identical content under a used key writes nothing and returns the original
offset (ADR-020), while a key the backend never saw is appended now. Until it is settled the stream
refuses further appends from that producer, because the caller's obvious next move — `publish()` again
— draws a fresh sequence number and puts the value in the stream twice if the first attempt landed.
For `finish_writing()` the duplicate is a second write fence, which reads back as a producer session
that ended twice.

The recovery is refused unless it is the producer instance, topic and exact bytes the outstanding
append belongs to. Each binding stops a different duplicate: a record does not name its own stream and
idempotency is scoped per stream, so another topic would append a second copy rather than deduplicate;
and a replacement producer with the same session id has its sequence and wake counters back at zero,
so settling there leaves its next publish reusing a sequence number and its next unparked wake reusing
a request ID. A producer that is gone recovers the way an Activity retry already does — same session
id, same calls, same order — which re-derives the keys and leaves the counters right (ADR-038).

If `resolve_append()` itself is interrupted after calling the backend, its effective wake and lease
replace the previous attempt's in the one retained operation before the new
`AppendNotAcknowledgedError` is raised. Those values are therefore the next recovery's defaults.
Cancellation is sticky instead: once any attempt receives it, later transport failures do not erase
the caller's obligation to honour it after settling the append. A later refusal reports this same
canonical state, so its error cannot direct recovery with older wake or lease instructions.

`AppendConflictError` is the one exception and the only one the contract can support: it says the key
was used with *different* bytes, so the record did not land and re-appending it would raise the
identical error. Cancellation delivered before the backend is called at all — while the sequence is
being drawn or the payload encoded — still propagates as cancellation, with nothing appended and
nothing owed (ADR-038).

### All three steps after the append are inside the guarantee

Once the append is acknowledged, `publish()` distinguishes exactly two further outcomes: the append
succeeded and the wake did not, or both did. **Steps 2 and 3 both belong to the first**, not only
step 3. A coordination call that
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

**Cancellation after the append is one of these failures, not an exception to them.** It leaves the
identical state — durable record, unsent wake — and gets the identical recovery, with a flag saying
cancellation is what ended the attempt. `CancelledError` derives from `BaseException`, so a bare one
escaping here carries no offset, no `pending` and no `restart`, and cannot be told apart from
cancellation *before* the append: the caller can then neither wake the record nor safely re-publish
the value, since a second `publish()` draws a new sequence number and appends it twice. Cancellation
delivered before the backend is called still propagates as cancellation, because nothing was sent and
there is nothing to recover; cancellation delivered *inside* the call is the unknown outcome above,
not this one (ADR-036, ADR-038).

`retry_wake` refuses an empty list rather than returning quietly, because a no-op there looks like
recovery while the record stays durable and unannounced. Calling `wake()` again is safe: it
re-observes the parked set, and a parked wake's request ID is derived from the generation rather than
the sender, so a wake another producer already sent deduplicates against it.

`finish_writing()` carries the identical contract. The duplicate it prevents is a second write fence,
which reads back as a producer session that ended twice.
