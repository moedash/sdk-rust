---
doc_id: EWS-SPEC-BACKEND
status: normative-pre-production
audience: [implementers, coding-agents, reviewers]
canonical_for: backend-provider-contract
related_adrs: [ADR-002, ADR-003, ADR-012, ADR-019, ADR-020, ADR-040, ADR-044, ADR-047]
---

# Backend contract

What a stream provider must implement to be registrable, and what the producer side must supply.

## Independent provider capabilities

Input and output are separate ABCs and conformance suites:

- `StreamBackend` supplies append, inclusive replay reads, exclusive live reads, and input
  park/wake coordination.
- `OutputStreamBackend` supplies staged output, direct committed append, barrier-aware reads,
  committed tail, stage inspection, and offset comparison.

An input-only provider remains valid. An output-only provider can serve
`ExternalOutputStreamProducer` and `ExternalOutputStreamClient`. The first-release Worker option is
validated as `StreamBackend`, so Workflow-originated output requires the configured provider to
implement **both** contracts. Capability separation prevents output staging from becoming a
breaking requirement for existing input providers.

Every implemented capability declares structural immutability, `provider_id`, and
`provider_format_version`.

## Input operations

A backend implementation must provide:

- Immutable, append-only records.
- Stable, totally ordered offsets within a stream.
- An **inclusive range read** over an explicit `[first_offset, last_offset]` pair, and a separate
  **exclusive** watch for records strictly after a boundary. These are two distinct operations —
  see "Cursor semantics".
- **A guarantee that a record's bytes cannot change once written.** Mandatory, checked at
  registration (ADR-003).
- Atomic or otherwise race-free coordination for parking and wakeup.
- Retention sufficient to re-read every record referenced by retained Workflow History.
- Idempotent coordination operations and detection of missing records.
- **Idempotent append that is idempotent on identity, not on key alone** (ADR-020). An append
  reusing an existing `(session_id, sequence)` with byte-identical content is a no-op returning
  the original offset; the same pair with *different* bytes must be rejected as an error.
- **Key derivation that is injective** — see "Key derivation must be injective".

Control records and data records share the same offset sequence. Control records are consumed by
the runtime and are not yielded to Workflow code.

## Output staging and reads

Workflow output is staged per topic sub-batch under an immutable `OutputStageManifest` carrying the
output `StreamKey`, provider binding, unique stage token, current Run, exact History floor,
sub-batch index, fingerprint version and digest, record count, and logical byte count (ADR-044).
The provider exposes:

- `stage_output(manifest, records)`: atomically places an ordered `PENDING` sub-batch. Repeating the
  exact logical manifest returns the first placed offsets and bytes; another manifest under the
  same `(stage_token, sub_batch_id)` is a conflict.
- `commit_output(manifest)` and `abort_output(manifest)`: idempotent terminal transitions that
  cannot be reversed.
- `output_stage(manifest)`: exact stage inspection for reconciliation.
- `append_output(key, record)`: an immediately committed singleton used by Activities and external
  processes.
- `read_output_after(key, boundary, ...)`: the committed prefix strictly after the boundary plus,
  when present, the first unresolved `PendingOutputBarrier`.
- `output_tail(key)`: the boundary after the readable committed prefix. It does not cross pending
  data.

A pending stage occupies provider order but is not readable. No read may return a record at or
beyond its first offset until ADR-044 commits or aborts it (ADR-047). Aborted records are skipped;
committed records are yielded in offset order. Commit and abort are coordination metadata and never
rewrite record bytes.

Workers reconcile their own pending stages after reporting when possible. Every
`ExternalOutputStreamClient` also performs built-in lazy reconciliation at a pending head: it reads
the exact producing Run's complete History strictly above `history_floor_event_id`, commits on the
exact marker token, aborts on the first durable Workflow Task closing boundary or Workflow closure,
and otherwise leaves the barrier pending. A backend or Temporal outage is transient storage
failure; unavailable deciding History is integrity loss.

## Cursor semantics

A cursor is a **position boundary**, not the identity of a record (ADR-002):

```text
cursor := BEGINNING
        | AFTER(last_consumed_offset)
```

`BEGINNING` is the provider's beginning-of-stream boundary and is not required to be the offset of
any real record. `AFTER(x)` names the boundary immediately following the record at offset `x`,
whether or not a record after `x` exists yet.

The boundary form maps directly onto the two primitives every provider must expose:

- **Live resume and watch read strictly after the boundary.** In Redis this is `XREAD BLOCK` from
  `last_consumed_offset`, or from the beginning sentinel `0-0` for `BEGINNING`. `XREAD` returns
  entries *strictly after* the supplied ID, which is exactly exclusive-after semantics.
- **Replay reads an explicit recorded range, never "from the cursor".** The annotation records
  each run's `first_offset` and `last_offset`, and replay issues an inclusive range read for
  precisely that range — in Redis, `XRANGE <first> <last>`. Replay never asks the backend what
  comes next; the answer is already in the marker.

A provider may represent the boundary as `(offset, inclusive | exclusive)` instead. What it may
not do is require the cursor token to be the offset of a record that does not exist yet.

**Offsets are compared by their provider's ordering rule, not lexically.** Redis IDs compare as
numeric `(milliseconds, sequence)` tuples; string comparison is wrong as soon as the millisecond
component changes width.

## Structural immutability is a registration precondition

**Every provider must guarantee that a record's bytes cannot change once written.** A backend that
cannot make the guarantee does not satisfy this contract and is rejected when it is registered on
the Worker, rather than compensated for at runtime. Redis Streams qualifies: an entry can be
deleted by `XDEL` or removed by trimming, but its fields cannot be rewritten in place.

Given that guarantee, replay needs to detect exactly one class of damage — a record that is **no
longer there** — and offsets are sufficient for it. Deletion, trimming, and retention expiry are
the realistic failure modes in operation, and all of them are caught by the four range checks in
`annotation-format.md`.

What the assumption costs, stated plainly: if a provider silently violates immutability — a buggy
custom backend, or out-of-band surgery on the stream — replay will deliver the altered bytes as
though they were original, and no error is raised. That risk is accepted deliberately and is
bounded by the registration requirement and the conformance suite. `schema_version` leads the
annotation encoding, so a per-record content-hash mode can be introduced later without a format
break if a provider ever needs one. See ADR-003.

**A decode failure is not an integrity failure.** With immutability guaranteed, bytes present
within a validated range are exactly the bytes that were written, so a DataConverter or codec that
cannot decode them indicates a configuration mismatch between producer and consumer — not a
damaged stream (ADR-015). See `failure-taxonomy.md`.

## Key derivation must be injective

**Distinct `StreamKey` values must map to distinct physical keys**, and to distinct keys for every
structure derived from one: records, idempotency state, park intents, and claims.

Every string component of a stream identity — namespace, Workflow ID, first execution Run ID, and
stream name — is user-chosen, and direction is an additional enum component. All five participate
in physical identity. An input and output topic with the same four strings must not share records,
idempotency state, park metadata, stage status, or claims.

Each string may contain a provider's delimiter. Joining them raw
is therefore not injective: with `:` as the delimiter, `("ns", "wf", r1, f"{r2}:tokens")` and
`("ns", f"wf:{r1}", r2, "tokens")` render identically. Two unrelated Workflows then share one
stream, one idempotency hash, one park intent and one claim. Each reads the other's records, which
is visible; and each concludes the other's claim has taken responsibility for the wake, so neither
producer signals and both Runs wait forever on records already durable, which is not.

Any injective encoding satisfies this — escaping the delimiter, percent-encoding each component, or
length-prefixing them. A provider that derives a pattern match from a key owes the same property
there: an identity carrying a metacharacter must not widen a scan onto another stream's intents.

## Conformance suite requirements

The suite is the deliverable, not the interface. It must contain, at minimum:

- a case that parks a consumer at the current tail, appends a record whose ID could not have been
  predicted, and resumes — a backend that requires a nameable next ID fails it;
- a case that fails a backend implementing the inclusive range read with **exclusive** semantics,
  since that error is invisible until the first replay;
- a case that fails a backend comparing offsets lexically, crossing a millisecond-width boundary;
- a case that fails a backend accepting an idempotency-key reuse with different bytes;
- park intents keyed by stream alone failing the two-subscription case (ADR-012);
- a case that removes an installed intent and requires `current_park_generation` to report nothing
  afterwards, since a provider that keeps answering is wrong only where nothing else looks at it;
- a case that removes the same intent twice, and one that removes an intent that was never
  installed, both of which must succeed — a removal that failed is retried later against whatever
  the key holds by then, so a provider that raises on the second call turns a completed cleanup into
  a permanent one;
- a case that conditionally removes an intent by Run ID and park generation, refuses mismatches,
  leaves a replacement intent intact, and reports an absent key as absent rather than as a
  mismatch; and
- a claim that never expires failing the leased-claim case;
- output staging repeated with the exact manifest returning the first placed records and actual
  terminal status, including a retry after commit or abort;
- reuse of `(stage_token, sub_batch_id)` with a changed manifest failing atomically without partial
  records;
- committed direct output after a pending stage remaining unreadable until that stage commits or
  aborts;
- repeated output commit and abort being idempotent while reversal is rejected; and
- input and output keys with otherwise identical components remaining physically isolated.

## Parking operations

`install_park_intent`, `remove_park_intent`, `remove_park_intent_if_matches`, `park_intent`,
`recheck`, `parked_wait_ids`, `claim_park_generation`, `current_park_generation`.

`parked_wait_ids` is enumeration rather than a new concept — the `wait_id` half of the intent key,
for one stream — and it is what makes the other operations reachable from the producer side at all.
A producer knows the stream it appended to and nothing about the Workflow's subscriptions, because
`wait_id` comes from a per-Run counter inside `subscribe()` that no producer can see.

Intents are keyed **`(stream key, wait_id)`**, never by stream key alone (ADR-012), carrying the
cursor boundary, the `park_generation`, and the current Run ID as the intent's *value*.

The key does not include the current Run ID: `wait_id` is stable across a Continue-As-New chain
and the stream key already contains the first execution Run ID, so the key is unique within a
chain, and only one Run of a chain is live at a time. Carrying the Run ID as the value means a new
Run's intent deterministically replaces its predecessor's for the same key rather than
accumulating alongside it.

The value must read back as it was written, because that is how a removal decided on earlier
identifies what it is removing. `wait_id` restarts at 1 in a Continue-As-New successor while the
stream key does not change, so a key alone does not distinguish a predecessor's abandoned intent
from a successor's live one. `remove_park_intent_if_matches` must compare the recorded
`park_generation` and Run ID and remove the intent in one atomic backend operation
(`wft-lifecycle.md`). Splitting the comparison and removal across calls lets a successor replace the
intent between them and lets the predecessor delete a live park.

**It answers with three outcomes, not two.** *Removed*, *absent*, and *mismatch* — and the last two
may not be collapsed, even though neither changes anything. A provider can commit the delete and lose
the connection before its reply arrives; the retry that follows meets a key it cleared itself. Told
"absent", the consumer knows the intent it named is gone and that a record the intent silenced may
need announcing again. Told "mismatch", it knows an intent it must leave alone is in the way and that
the suppression has not ended. One answer for both makes the first case look like the second, which
is a record left undelivered rather than an intent left installed.

**An intent exists only while its park is outstanding.** The consumer holds up one half of that
across three points — the resolve that ends a park the Run is sitting in, the reconciliation at
registration for an intent this Worker inherited rather than installed, and the cancellation that
closes a wait — with a removal none of them got confirmed carried as owed until one of them does
(`wft-lifecycle.md`). All of that is needed because the intent is durable backend state while the
record of which Worker installed it is per-Worker, so an eviction or a handoff otherwise leaves it
forever. The provider holds up the other half: once an intent is removed, `current_park_generation` must report nothing for that
subscription. A provider that answered from a remembered "last generation" beside the intent passes
every other requirement here and still breaks both of that call's readers, in the same direction and
invisibly. A producer would name a generation Core has already discarded, and a non-zero generation
the Run does not recognize is exactly what Core ignores as stale — so the record is appended, the
Signal is sent, and the Workflow is never woken. The consumer's own shutdown sweep would read the
same answer and send a parked wake where it owes the unparked one; a parked wake's request ID ignores
sender identity by design, so it arrives byte-identical to the wake that ended that park and the
server deduplicates it away.

**Claims must be leased and renewable.** A claim is how a provider learns a wake is in flight, and
its expiry is what makes one abandoned by a crashed producer takeable again; a claim that never
expires says a wake is in flight for the life of the store, and nothing can take it back. What
keeps a producer crashing between claiming and signalling from stranding the generation is not the
lease — expiry permits takeover, it schedules nobody to take over — but the producer contract that
a producer which loses the claim sends the wake anyway (`wake-signal.md`). A provider that cannot
lease must expose observe-only semantics and always grant, which is the same behaviour every
provider's callers already rely on.

## Producer binding

A producer needs five things, none of which it can infer (ADR-019):

- **The Workflow chain key**, including the first execution Run ID. `temporalio.activity.Info`
  exposes `workflow_run_id` but *not* the first execution Run ID, so an Activity cannot derive the
  key. The Workflow passes it to the producer explicitly — as an Activity argument, or through
  whatever channel a non-Temporal producer already uses — and the producer verifies it by
  describing the Workflow before its first append. Publishing under an unverified key is a
  configuration error, not a silent no-op.
- **A backend connection.** A Worker has one configured backend; a plain process constructs a
  provider directly.
- **A Temporal client**, for the wake Signal.
- **The same `DataConverter`** the consuming Workflow uses, including any codec, **bound to the
  same serialization context** — which the producer derives from the chain key it was given, so it
  matches what the consuming Worker converts under (`python-runtime.md`). The context is part of the
  requirement rather than an implementation detail of it: two context-free sides agree and two bound
  sides agree, but one of each encrypts under one key and decrypts under another (ADR-035). A
  mismatch either way is detected at decode time on the consumer and surfaces as a distinct decode
  failure.
- **A stable producer session ID and sequence**, which is what makes append idempotent under
  Activity retry. Activities default it to a value derived from the Activity's identity so a
  retried attempt reuses it; plain processes must supply one, and the API requires it rather than
  defaulting to a fresh random value.

  *Stable* is a claim about the sequence too, and it holds only because the number is drawn when the
  call is made rather than when its payload finishes encoding. A codec may do real I/O — an external
  payload store, a KMS round trip — and its completion order is not reproducible across attempts, so
  a sequence drawn afterwards belongs to that order instead of to the call. Two concurrent publishes
  then exchange keys whenever the store answers the other way round: on one stream the backend sees
  a stable key reused with different bytes and raises the conflict below, and across topics, where
  deduplication is per stream key, the swap appends duplicates and raises nothing. Either way a valid
  concurrent Activity is made non-retryable, or silently doubled, by external timing.

The stream name appears exactly once on the producer side, in `topic()`. `connect()` takes the
Workflow *chain* key — namespace, Workflow ID, first execution Run ID — and `topic(name)`
completes it into the full stream identity, so one connection serves several topics and no two
arguments can disagree about the name.

The direct output producer uses the same explicit `WorkflowChainKey`, chain verification,
Workflow-bound serialization context, stable session ID, and invocation-order sequence rule. It
sends no wake Signal: output clients watch the backend. An append with no reported outcome raises
`OutputAppendNotAcknowledgedError`; recovery repeats the exact carried record through
`resolve_append()` rather than drawing a new sequence. `finish_writing()` waits for every earlier
publish on that topic and then appends `FINISH`; unlike the input producer's write fence, it closes
the producer's topic handles.

## Write-fence semantics

`finish_writing()` means:

> All writes in this producer session preceding the fence have been appended. If a consumer drains
> through the fence and no later record is immediately available, it may park now.

The fence is an ordered stream record, so its relationship to concurrent data is unambiguous:

```text
offset 100  data
offset 101  data
offset 102  WRITE_FENCE(producer-session-id)
offset 103  data from another producer
```

At offset 102, the runtime continues if offset 103 is already available; otherwise that
subscription becomes immediately parkable. The fence neither closes the stream nor asserts that
all producers are finished. A fence on one stream only marks that stream parkable — the Workflow
Task parks early only when every active subscription is immediately parkable; otherwise the idle
timeout remains authoritative.

### The claim has to be enforced, not just documented

**A fence is appended only after every `publish()` invoked earlier on that stream has reached a
durable append** — earlier by invocation order, across every handle `topic()` has returned for the
name, since the stream is one thing and the handle is not. Without that hold the claim is simply
false: a publish draws its sequence *before* awaiting the payload codec (see "Producer binding"), so
one invoked first can still be encoding when the fence is called, and a fence that overtook it parks
a consumer in front of data that has not been written. In a `wake=False` batch it is worse than
early — the fence carries the batch's only wake, so it spends it on records that do not exist yet.

**The order holds fences behind data writes and behind nothing else.** Two concurrent fences make
independent claims about the publishes each of them was invoked after, and neither is inside the
other's claim, so they are unordered with respect to each other. Ordering them would only let a
fence that never reached the backend — cancelled while waiting, or refused — stand in for a data
write that went missing, and refuse a fence with every write it claims already durable (ADR-040).

What an earlier publish's outcome does to the fence follows the three outcomes an append has
(`wake-signal.md`):

| Earlier publish | The fence |
|---|---|
| Durable | Appended behind it |
| Failed — no record, and none coming | Refused with `PrecedingWriteFailedError`, nothing appended |
| Unknown — the acknowledgement window | Refused with **that** operation's `AppendNotAcknowledgedError` (ADR-038) |

A fence appended over the second row's hole tells a consumer the batch is complete when it is short
a record; the caller chooses between publishing the value again — which draws a new sequence number
and lands ahead of a later fence — and accepting the batch without it, and either way the next
`finish_writing()` appends a fence because the failed write is no longer outstanding.

The third row is a refusal on the operation's own terms and not on the fence's, because the stream
takes no further append at all until it is settled, and the record may well be durable and belong in
front of the fence. **`resolve_append()` is therefore what decides which of the first two rows the
fence lands in**, and a fence must read that decision rather than infer it: a durable resolution
puts the record ahead of the fence, and an `AppendConflictError` proves it absent and makes the
fence's answer the second row, chained from that conflict. The two are indistinguishable from
whether the record is still outstanding, since resolving it either way stops it being outstanding.

Where both an unresolved and a definitely-failed write precede one fence, the unresolved one is
reported, because it is the one that blocks the other's recovery: republishing a failed value is
itself refused while an append on that stream is unsettled.

## Retention

Records and control metadata must remain available and immutable for as long as any retained Run
in the Workflow-ID chain may replay them. Backend retention must therefore cover at least the
namespace's Temporal retention for that chain. Garbage collection is allowed only after the chain
is terminal and its applicable Temporal retention/replay window has elapsed, or after an explicit
stronger archival policy guarantees replayability.

This is an operational prerequisite, not a code deliverable. Violations surface as stream-integrity
failures.

Output providers additionally retain committed records for their advertised client-resume window
and retain pending-stage resolution metadata for at least as long as the deciding Temporal History.
The SDK chooses no default retention period and exposes no operator force-commit/force-abort API.
Garbage collection is safe only after History proves abort, after the resume and History windows
both expire without a remaining reference, or as part of explicit integrity-incident repair.
Elapsed time alone never resolves a pending stage.
