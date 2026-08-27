---
doc_id: EWS-PROPOSAL-W2W-DESIGN
status: future-not-implemented
audience: [implementers, coding-agents, design-reviewers]
normative: false
---

# Detailed proposal — Workflow-to-Workflow External Stream Subscriptions

**Status:** Future enhancement; not implemented

**Scope:** Temporal Python SDK, SDK Core, and External Workflow Streams providers

**Depends on:** External input streams, Workflow-originated external output streams, and their
existing replay, output-staging, and wake-signal protocols

For a short decision overview, read
[`workflow-to-workflow-external-streams-overview.md`](workflow-to-workflow-external-streams-overview.md).

## Summary

Allow one Workflow to consume a committed external output stream owned by another Workflow without
copying payloads through Temporal History or through a second external stream:

```text
source Workflow A
    -> OUTPUT stream in the external backend
    -> durable visibility notification
    -> wake Signal carrying no payload
    -> consumer Workflow B reads A's committed OUTPUT records directly
```

The data path is zero-copy: A's output records remain under A's existing `OUTPUT` `StreamKey`, and B
reads those records in place. B does **not** alias A's output as one of B's input streams. Its replay
marker records the exact source binding and committed output ranges that B observed.

The hard part is wakeup, not reading. An output key identifies A but does not identify the workflows
that consume it. The design therefore adds durable consumer registrations and provider-backed
reconciliation and notification outboxes. Making output visible and creating notification
obligations are one atomic provider transition. Background dispatchers resolve stranded pending
stages and turn visibility obligations into idempotent raw Temporal Signals addressed to B. This
remains correct when A, B, or either Worker restarts.

The first version is same-namespace only, broadcast-only, explicitly authorized, and available only
for providers that implement a new optional cross-Workflow subscription capability.

## Decision

Introduce a distinct Workflow-side subscription handle for reading another Workflow's external
output. Reuse the existing:

- source output physical records and pending-stage barrier;
- Workflow Task marker and activation-segment schedule;
- global wait-set quiescence and parking protocol;
- raw, codec-bypassing wake Signal transport; and
- opaque provider cursors.

Add:

- an exact `WorkflowOutputStreamReference` containing the source chain and topic;
- an optional `WorkflowOutputSubscriptionBackend` provider capability;
- a durable registration for every foreign output subscription;
- atomic output-visibility notifications plus leased reconciliation and notification outboxes;
- a versioned cross-Workflow wake envelope;
- source-output range bindings in B's replay annotation and Continue-As-New state; and
- replay-retention leases that outlive active registrations; and
- a default-deny Worker authorizer for cross-Workflow reads.

Do not copy records from A's `OUTPUT` key to B's `INPUT` key. A relay that performs that copy remains
a valid application-level alternative, but it is not the native feature designed here.

## Terminology

| Term | Meaning |
|---|---|
| Source | The Workflow chain whose external `OUTPUT` topic holds the records; A in the examples |
| Consumer | The Workflow chain whose code subscribes to the source; B in the examples |
| Source binding | Source `WorkflowChainKey`, `OUTPUT` direction, topic, provider binding, and initial cursor |
| Registration | Durable provider state saying that one B wait consumes one A output topic |
| Visibility transition | A commit or abort that advances the readable committed prefix of an output topic |
| Notification | A durable obligation to announce a visibility transition to one consumer registration |
| Replay-retention lease | Provider state preventing GC while B's retained History can reference source records |
| Binding ID | A protocol hash of the immutable source binding and consumer wait identity |

## Goals

- Let B consume A's committed external output directly from Workflow code.
- Keep every stream payload out of both Workflows' Histories.
- Never expose output from an A Workflow Task that did not commit.
- Preserve B's deterministic replay, including ordering across several local and foreign streams.
- Guarantee that a committed, readable record eventually wakes a parked B while a dispatcher and
  Temporal are available, including after Worker restart.
- Preserve source output order, opaque resume cursors, explicit `FINISH`, and Continue-As-New chain
  identity.
- Support broadcast fan-out: each B has an independent cursor and replay history.
- Make authorization explicit and replay-safe.
- Keep existing input-only and output-only providers valid.

## Non-goals

- Work-sharing or consumer groups. Every subscription receives the complete source stream.
- Cross-namespace consumption in the first version.
- Reading another Workflow's external **input** stream.
- Inferring completion from A closing. Only an ordered output `FINISH` ends iteration.
- Exactly-once execution of B's application side effects. Temporal replay semantics still apply.
- Global order across source topics or across different source Workflows.
- Detecting cycles or deadlocks between Workflows that wait on one another.
- Revoking a source binding after an authorization decision has committed. Revocation is future work.
- Browser access to backend credentials.
- Making cross-Workflow consumption work on a provider that cannot atomically create visibility
  notifications.

## Why the current APIs cannot be composed directly

Today, `external_stream.topic(name).subscribe()` derives an `INPUT` key from the running Workflow's
own namespace, Workflow ID, and first execution Run ID. It accepts no foreign chain key.
`ExternalOutputStreamClient` can read A's `OUTPUT` key, but it performs backend and Temporal service
I/O and therefore cannot run inside deterministic Workflow code.

Simply permitting B to name A's key leaves four correctness gaps:

1. **No wake target.** A's output key identifies A, while a wake must be sent to B.
2. **No durable subscriber registry.** If B is evicted, a backend watch in B's old Worker no longer
   exists, and A has no way to discover B.
3. **Wrong replay binding.** B's current input annotation implies B's own `INPUT` key and B's
   serialization context, not A's `OUTPUT` key and A's context.
4. **Pending output.** B must not read a staged A batch until A's producing Workflow Task has a
   durable marker proving commit.

Direction must remain part of physical identity. Treating A's output and B's input as the same key
would violate provider isolation, mix record formats and coordination metadata, and route wakeups to
the wrong chain.

## Public API

The proposed names are intentionally explicit. The source reference is data and may be passed in a
Workflow input, Signal, Update, result, or Activity argument.

```python
from temporalio.contrib.external_workflow_streams import (
    BEGINNING,
    WorkflowChainKey,
    WorkflowOutputStreamReference,
    external_stream,
)

source = WorkflowOutputStreamReference(
    workflow=WorkflowChainKey(
        namespace="payments",
        workflow_id="agent-A",
        first_execution_run_id="source-chain-run-id",
    ),
    topic="events",
)

events = external_stream.from_workflow_output(
    source,
    type=AgentEvent,
    after=BEGINNING,
)

async for event in events.subscribe():
    process(event)
```

Proposed types:

```python
@dataclass(frozen=True)
class WorkflowOutputStreamReference:
    workflow: WorkflowChainKey
    topic: str

class ExternalWorkflowOutputTopic(Generic[T]):
    source: WorkflowOutputStreamReference
    value_type: type[T] | None
    after: Cursor

    def subscribe(self) -> ExternalWorkflowOutputSubscription[T]: ...
```

Rules:

- `WorkflowChainKey` is mandatory. Resolving a Workflow ID to whichever chain happens to own it at
  subscription time is not supported in v1; it races with Workflow ID reuse.
- The source namespace must equal B's namespace in v1.
- `after` accepts `BEGINNING`, `AFTER(offset)`, or an `Offset` with the same meaning as
  `ExternalOutputStreamClient.subscribe`. There is no implicit `TAIL` option in v1. A caller that
  needs tail semantics obtains a committed tail outside Workflow code and passes the cursor in.
- On B Continue-As-New, the restored durable cursor overrides the first-Run starting cursor. The
  recreated API call must name the same source and topic or replay fails as nondeterministic.
- The handle yields decoded values, not backend offsets. Cursor ownership remains inside the
  Workflow runtime after the initial boundary, matching existing Workflow input subscriptions.
- A source must exist and its first execution Run ID must verify before the first registration.
- `FINISH` ends iteration. Source completion, failure, cancellation, or termination does not
  synthesize `FINISH`.

An external convenience function may resolve and verify a source reference from a client handle,
but the Workflow-side API always receives the exact resolved reference.

## Stream identity and serialization

The physical source key remains:

```text
(source namespace,
 source Workflow ID,
 source first execution Run ID,
 OUTPUT,
 source topic)
```

B's identity is not added to that key. It belongs in the registration, because adding it to the
stream key would create one physical copy per consumer rather than broadcast one source log.

Output payloads were encoded using A's Workflow serialization context. B therefore decodes with its
configured `DataConverter` bound to A's namespace and Workflow ID, not B's. The provider ID, provider
format version, source chain, topic, and source serialization context identity are recorded in B's
first binding annotation. A codec or key configuration that cannot decode A's immutable bytes raises
`StreamDecodeError`, not an integrity error.

The source and consumer Workers must address the same logical provider data and agree on provider ID
and format version. They do not need to share a provider connection object or credentials.

The provider format advertises whether visibility notification is atomic. Registering a foreign
consumer against a source topic without that capability is rejected. All Workflow and direct
publishers writing a workflow-consumable provider prefix must use the capability-aware format;
provider-side version metadata rejects an older publisher instead of accepting a write that cannot
notify existing registrations. This is a deployment compatibility boundary, not a best-effort check.

## Authorization

Cross-Workflow reads are default-deny. Enabling the provider capability alone does not grant every
Workflow on a Worker access to every output stream visible to its backend credentials.

The Worker accepts an out-of-sandbox authorizer:

```python
Worker(
    ...,
    external_stream_backend=backend,
    external_stream_authorizer=authorizer,
)
```

The authorizer receives the source key, consumer chain, current consumer Run, topic, requested start
cursor, and operation `SUBSCRIBE_WORKFLOW_OUTPUT`. It returns `ALLOW(decision_id)` or `DENY(reason,
decision_id)`. The decision ID is non-secret and stable enough for diagnostics; credentials and
policy documents never enter History.

Authorization is an external observation and therefore part of deterministic state:

- an allowed binding and its decision ID are recorded in B's first stream marker;
- a denial caught by Workflow code is recorded as a denied binding outcome if that Workflow Task
  commits; and
- replay uses the recorded outcome and never calls the authorizer.

An uncommitted Workflow Task attempt may be authorized again on retry, just as it may observe newer
external records; only the accepted attempt becomes replay history. Once an allowed binding commits,
it remains authorized for that Workflow chain. Operators requiring immediate revocation must stop B
or deny access in the backend as an operational intervention; a deterministic revocation protocol is
outside v1.

## Provider capability

Add an optional capability independent of `StreamBackend` and `OutputStreamBackend`:

```python
class WorkflowOutputSubscriptionBackend(OutputStreamBackend):
    ...
```

An output-only provider remains valid for external clients and direct producers. A provider must
implement this additional capability only when a Workflow calls `from_workflow_output`.

### Durable registration

The registration key is injectively encoded from:

```text
(source OUTPUT StreamKey,
 consumer namespace,
 consumer Workflow ID,
 consumer first execution Run ID,
 consumer wait ID)
```

Its value contains at least:

- current consumer Run ID;
- binding ID and authorization decision ID;
- consumer cursor used for recheck;
- current quiescence/park generation, where zero means unparked;
- monotonically increasing registration version; and
- creation/update metadata for diagnostics and garbage collection.

Unlike the existing input park intent, this registration exists while the subscription is active,
not only while B is parked. That persistent identity is necessary because A's output key cannot imply
which workflows should receive an unparked wake.

Continue-As-New updates the value to B's successor Run while retaining the same consumer chain and
logical wait binding. Cancellation or subscription close conditionally removes the registration by
consumer Run ID and registration version so stale cleanup cannot delete a successor's registration.
An unconfirmed removal is an owed cleanup operation and is retried.

### Required operations

Names are illustrative; their transactional properties are normative.

- `register_output_subscription(registration, after)` atomically installs or updates the
  registration and rechecks the readable committed prefix after `after`.
- `update_output_subscription(registration_version, cursor, park_generation)` conditionally updates
  the current Run, cursor, and generation.
- `remove_output_subscription_if_matches(...)` returns removed, absent, or mismatch.
- `read_committed_output_range(key, first, last)` returns exactly the committed, readable records in
  an inclusive provider range and reports any unresolved barrier at or before `last`.
- `make_output_stage_visible(manifest, decision)` atomically commits or aborts a stage, advances the
  readable prefix, and creates/coalesces notifications for every active registration whose readable
  view advanced.
- `stage_output_for_subscribers(manifest, records)` preserves existing pending-stage semantics and
  creates/coalesces a durable stage-reconciliation item when the source currently has registrations.
- `append_committed_output_and_notify(key, record)` atomically performs a direct output append and
  creates/coalesces the corresponding notifications.
- `claim_stage_reconciliations` and its renew/ack/release operations implement durable recovery of
  pending heads. A registration installed behind an existing pending head creates the same work.
- `claim_notifications`, `renew_notification_claim`, `ack_notification_if_current`, and
  `release_notification_claim` implement the leased wake outbox.
- `advance_replay_retention` monotonically widens a consumer binding's referenced output range, and
  `close_replay_retention` sets its safe release boundary without deleting it early.

Existing `commit_output`, `abort_output`, and `append_output` may delegate to these operations when
the capability is present. Providers must not advertise cross-Workflow subscriptions if output
visibility and notification creation can be separated by a crash.

### Atomic registration/recheck race

Registration and visibility use one provider serialization point:

```text
commit before registration  -> registration recheck returns the new committed records
registration before commit  -> commit creates a durable notification
```

There is no third interleaving in which the records become visible but neither path observes them.

### Commit and abort both notify

A notification is tied to advancement of the **readable prefix**, not merely to appending a record.
This distinction is required for output barriers:

```text
offset 10  PENDING stage S
offset 11  committed direct output, hidden behind S
```

Committing S makes offsets 10 and 11 readable. Aborting S also makes offset 11 readable. Both
terminal decisions must create notifications. A design that notifies only on append or commit loses
the abort case permanently.

## Reconciliation and notification outboxes

There are two kinds of durable coordination work:

- **Stage reconciliation** proves whether an unresolved pending A stage committed or aborted. It is
  keyed by the exact stage manifest and coalesced across all consumers of that source topic.
- **Visibility notification** announces an already readable prefix to one B registration. It carries
  no stage decision and no payload.

Creating a pending stage while registrations exist creates reconciliation work. Registering behind
an already pending head does the same. This closes the case in which A's Worker dies after its
Workflow Task commits, B's Worker also restarts, and no cached process remains to perform the lazy
client reconciliation that current external output clients rely on.

Dispatchers process reconciliation before the affected visibility notification: they read A's exact
deciding History, apply commit or abort through `make_output_stage_visible`, and let that atomic
transition create the notification obligations. An undecided stage is retried with backoff. A stage
with no registrations may continue to use today's source-Worker and external-client lazy
reconciliation; a later registration makes its reconciliation durable.

### Notification wake dispatch

Each notification identifies the source binding, consumer registration, latest newly visible tail,
registration version/generation, and notification revision against which it was created. The
provider may coalesce several visibility transitions for the same registration by increasing that
revision and moving the tail forward; payload records are never copied into the outbox.

Every Worker configured with a capable backend may run a lightweight global dispatcher. Dispatchers
do not require B to be cached and do not reconstruct B's Workflow state. They:

1. lease-claim a notification;
2. read the current registration;
3. build a raw wake request addressed to B;
4. send the Signal idempotently;
5. conditionally acknowledge the notification only if the registration version, generation, and
   claimed notification revision still match; and
6. release/recompose when B changed generation during the send.

Claims are renewable and expiring. Losing a claim is not proof that another dispatcher sent the
Signal, so racing dispatchers may send. Stable request IDs make that safe.

The global scan is essential. A per-run backend watch is a useful low-latency fast path while B is
cached, but it disappears on Worker restart. Likewise, relying only on A's Worker to reconcile and
wake fails if A completes and that Worker never returns. Durable reconciliation and notification
outboxes plus any live dispatcher remove both dependencies.

### Wake envelope v2

Use the existing reserved Signal name with a new envelope version. It still bypasses the user's
`DataConverter` and contains no stream payload.

Conceptually:

```protobuf
message WakeSignalV2 {
  uint32 envelope_version = 1;              // 2
  string consumer_first_execution_run_id = 2;
  uint32 wait_id = 3;
  uint64 park_generation = 4;               // 0 means unparked
  bytes binding_id = 5;                     // source binding + consumer wait
  string notification_id = 6;               // diagnostics/deduplication
  string dispatcher_identity = 7;            // diagnostics only
}
```

Core stores the binding ID beside the registered wait. It accepts a wake only when the target
consumer chain, wait ID, binding ID, and non-zero generation match. Generation zero requests a
recheck of B's complete active wait set. Core suppresses every version of the reserved Signal from
user Signal handlers, including malformed or stale envelopes.

Request IDs are derived as follows:

- parked: `(consumer chain, binding ID, wait ID, park generation)` so every notification for the
  same parked generation collapses to one server wake;
- unparked: the same fields plus the durable notification ID and claimed revision, so later
  visibility transitions do not reuse a request ID that the server already accepted.

After Signal acknowledgement, the dispatcher conditionally acknowledges the outbox item. If B
changed its registration version or park generation, or if newly visible output advanced the
notification after it was claimed, the item remains pending and is recomposed. This prevents a stale
but successfully delivered Signal from consuming the only wake owed to B's new park or to output
that became visible after the signaled Workflow Task had already rechecked.

Closed or reused target chains are terminal for that registration. After the Temporal service proves
the target closed or the first execution Run ID mismatched, the dispatcher conditionally removes the
stale registration and acknowledges its notifications. Transient service failures remain retryable.

## Live consumption

When B creates the subscription, its out-of-sandbox runtime:

1. validates the full source reference and same-namespace rule;
2. verifies A's chain against Temporal;
3. verifies provider capability and provider binding;
4. obtains an authorization decision;
5. binds B's decoder to A's serialization context;
6. atomically registers and rechecks from the requested cursor; and
7. returns readiness to B through the existing external-stream activation path.

For later records, either B's local backend watch reports readiness directly to Core or an outbox
dispatcher Signals B. A valid Signal causes `ResolveExternalStreamWaits`; Python rechecks all active
waits, including local input and foreign output subscriptions.

A foreign output wait participates in the same global quiescent set as existing input waits. Mixed
waits therefore retain one Workflow Task, share one idle-timeout reduction, and use one terminal race.
Parking conditionally writes the current generation to every foreign registration before Core
confirms the park. Readiness concurrent with parking is resolved by the provider's atomic
registration/recheck transition and the existing Core park handshake.

Slow B consumers never block A or external clients. They increase retained provider data and their
own replay ranges, but source publishing does not wait for their application code.

## Output staging and pending barriers

B reads only the committed readable prefix. An unresolved A stage remains a hard ordering barrier;
B may consume committed records before it but never at or after it.

The consumer runtime may reconcile a pending head exactly as `ExternalOutputStreamClient` does:

- read the exact producing A Run's History strictly above the manifest's recorded floor;
- commit on the exact output stage token in A's marker;
- abort on the first durable Workflow Task closing boundary that proves the token absent; and
- leave the stage pending if History proves neither result.

Applying either decision uses `make_output_stage_visible`, so any newly readable prefix creates
notifications atomically. History unavailability needed for a decision is integrity loss; a backend
or Temporal outage is a transient storage failure.

The durable reconciliation outbox applies the same algorithm when no source or consumer Run is
cached. A dispatcher acknowledges that work only after the exact provider stage reaches the proven
terminal status. Losing the dispatcher before acknowledgement repeats an idempotent decision.

B never observes speculative output. A Workflow Task attempt in A that stages records and then fails
can only be aborted, after which those records remain invisible and are skipped.

## Replay annotation

B's marker adds a foreign-output binding table. Each binding contains:

- wait ID and binding ID;
- complete source `OUTPUT` `StreamKey`;
- provider ID and format version;
- initial cursor for the first Run of B's chain;
- source serialization context identity;
- authorization outcome and decision ID; and
- finished state when `FINISH` has been consumed.

The existing activation-segment schedule is reused. A segment run refers to a wait ID and an exact
inclusive output range. For foreign output, each run additionally records the committed record count
needed to validate ranges that contain provider offsets belonging to aborted stages.

On replay:

1. Core issues the recorded replay job and segment schedule.
2. Python reconstructs the binding from the marker, not from mutable Workflow arguments, Temporal
   Describe, the authorizer, or provider tail state.
3. The provider reads each exact inclusive committed range.
4. Python verifies provider binding, monotonic offsets, exact first and last offsets, record count,
   absence of an unresolved barrier at or before the last offset, and presence of every referenced
   record.
5. Records are decoded under A's recorded serialization context and delivered in the recorded global
   segment order.

Replay never asks what comes after the recorded last offset and never resolves a pending stage.
Immutable output bytes plus exact range validation provide the same integrity boundary as current
input replay. Missing, reordered, unexpectedly pending, or no-longer-committed records raise
`StreamIntegrityError`.

An output stage cannot change from committed to aborted or vice versa. A barrier that was pending
during live execution cannot later insert records inside a range B already consumed, because B could
not read past that barrier. This monotonicity makes exact committed-range replay well-defined.

## Continue-As-New

B's internal continuation header carries, for every unfinished foreign subscription:

- source reference and binding ID;
- provider binding;
- last durably consumed cursor;
- authorization decision ID;
- source serialization context identity; and
- logical wait identity needed to recreate the subscription.

The successor Run updates the durable registration's current Run ID conditionally. A predecessor's
late cleanup cannot remove the successor registration. The source stream remains stable across A's
own Continue-As-New because its key uses A's first execution Run ID.

B's finished subscriptions carry terminal state but no active provider registration. Re-publishing
to a topic after `FINISH` remains an output protocol error; a successor does not reopen it.

## Cancellation, closure, and cleanup

Closing or cancelling B's subscription removes the registration conditionally. B Workflow
completion and Continue-As-New finalization include the same cleanup obligation. An unconfirmed
remove is retained as owed manager state and retried; it is never converted into deletion of whatever
new value happens to occupy the key.

Termination may prevent B from running cleanup. Stale registrations are therefore also removed when
a dispatcher receives authoritative proof that the target consumer chain is closed or reused. Until
then, a stale registration costs only coalesced notification state and harmless failed wake attempts;
it never blocks A's readable prefix.

A's closure does not remove consumer registrations or end B's iterator. Only `FINISH` does. This
matches current output semantics and avoids making a transient inability to Describe A look like an
ordered terminal record.

## Retention

Once B consumes A's output, those records become replay dependencies of B's History. The provider
must retain them for the longer of:

- A's external-client resume policy;
- A's own pending-stage reconciliation window; and
- every consuming B chain's open lifetime plus applicable Temporal History retention/replay window.

This can be much longer than the source's original client-resume window. Provider GC must consult
replay-retention leases and terminal/retention metadata or use a configured conservative retention
that satisfies all consumers. Elapsed source age alone is not sufficient.

An active registration is not itself the retention record. On first delivery, the consumer runtime
creates a replay-retention lease before handing the record to Workflow code, then monotonically
widens its first/last referenced range as delivery advances. Updating before Workflow Task acceptance
may over-retain after a failed attempt, which is safe; updating only afterwards creates a crash window
in which History commits a dependency the provider does not know to keep.

Closing a subscription, consuming `FINISH`, or completing B removes the active registration but does
not delete its replay-retention lease. Authoritative B closure changes the lease to a terminal state
whose release time covers B's applicable Temporal retention, archival, and replay policy. A
Continue-As-New successor keeps the same consumer-chain lease active. The provider may trim a source
range only when no active or terminal lease can still reference it.

Missing retained records are integrity failures, not an instruction to restart at the current tail.
The SDK chooses no default retention duration.

## Delivery and ordering semantics

- Each subscription is broadcast and has an independent cursor.
- Records are yielded in provider order within one source topic.
- `FINISH` is ordered and terminal.
- Ordering across topics or source Workflows is only the order B actually observes. The existing
  activation-segment schedule records that order for replay.
- Direct Activity output and Workflow-originated output share the source topic's existing barrier and
  committed-prefix rules.
- Replaying B re-executes its code against the same logical records. This is deterministic replay,
  not an exactly-once guarantee for external side effects performed by application code.

## Failure model

| Failure | Required behavior |
|---|---|
| A stages output but its Workflow Task is rejected | Stage aborts; B never observes its records |
| A's task commits and A's Worker dies before provider commit | Any reconciler proves the token from A's History, commits atomically with notifications |
| Both Workers restart while A's stage is pending | Global reconciliation work proves the stage, then creates visibility notifications |
| A's stage abort exposes later direct output | Abort atomically advances the readable prefix and notifies B |
| Dispatcher dies after claiming | Lease expires; another dispatcher retries |
| Dispatcher dies after Signal commit but before outbox ack | Retry uses the same request ID; Temporal deduplicates it |
| B changes park generation while a Signal is in flight | Conditional outbox ack fails; notification is recomposed for current registration state |
| B Worker restarts while B is parked | Global dispatcher scans durable outbox and Signals B without requiring cached Run state |
| Registration races source visibility | Atomic register/recheck or visibility notification observes the transition |
| Backend is unavailable during live read | Transient `StreamStorageError`; Workflow Task retries |
| Authorization service is unavailable before binding commits | Workflow Task fails transiently and retries |
| Authorization is denied and Workflow catches it | Denied outcome is recorded and replayed without reauthorizing |
| Source chain key is wrong or reused | Binding fails; no registration is installed |
| Output bytes cannot be decoded with A's context | `StreamDecodeError` |
| A referenced replay range was trimmed or altered | `StreamIntegrityError` |
| B closes without consuming `FINISH` | Registration is removed; source remains unchanged |

## Backpressure, quotas, and fan-out

Payload data is not duplicated per B, but coordination is. A source with `N` consumers creates up to
`N` coalesced notification obligations per visibility transition. Providers must support:

- a maximum active registration count per source topic and per consumer Workflow;
- notification coalescing by registration and park generation;
- bounded dispatcher batches and leased claims;
- retry backoff with jitter for transient Temporal failures;
- metrics for oldest pending notification age; and
- operator inspection of registrations and poison notifications.

Exceeding a registration quota fails the new subscription before it becomes visible to Workflow
code. It never silently drops an existing consumer. A slow consumer does not apply backpressure to A;
retention quotas and application policy decide whether to stop that consumer.

## Observability

At minimum, emit:

- active foreign output registrations;
- registration attempts, denials, and conditional-cleanup mismatches;
- notifications created, coalesced, claimed, acknowledged, retried, and oldest age;
- pending-stage reconciliation items created, resolved, retried, and oldest age;
- parked versus unparked wake attempts and Signal latency;
- stale/closed target cleanup;
- local-watch versus Signal readiness;
- pending-stage reconciliations performed on behalf of Workflow consumers;
- foreign output records and logical bytes delivered;
- replay range reads and integrity/decode failures; and
- active and terminal replay-retention leases plus oldest protected source offset.

Logs and metrics identify source and consumer using safe hashed binding IDs by default. Raw Workflow
IDs and topic names follow the SDK's existing telemetry privacy policy.

## Core and language protocol changes

Core does not read provider data. It needs enough information to preserve Workflow Task lifecycle and
validate wakes:

- a binding variant distinguishing local `INPUT` waits from foreign `OUTPUT` waits;
- binding ID and source/provider metadata in progress markers and continuation state;
- record count on foreign-output replay ranges;
- wake envelope v2 parsing and binding-ID validation;
- the existing resolve/replay jobs generalized to carry the binding kind; and
- the existing wait-set, activation budget, parking, rollover, shutdown, and WFT-admission invariants
  applied uniformly to both kinds.

Unknown binding kinds, provider formats, annotation schema versions, and wake envelope versions are
must-understand failures or harmlessly suppressed wakeups according to the existing protocol boundary;
they never fall through to user code.

## Compatibility and rollout

This is additive and remains experimental with the broader feature.

1. Specify the annotation, continuation, wake envelope, registration, and outbox formats.
2. Add provider conformance tests and an optional backend capability without changing current
   provider registration requirements.
3. Add Core protocol support behind a Worker feature flag.
4. Add Python runtime support with the authorizer defaulting to deny.
5. Implement Redis registration/outbox scripts with atomic visibility notification.
6. Enable the public API only when Core and the configured provider both advertise support.
7. Remove the feature flag only after crash, replay, Continue-As-New, retention, and multi-Worker
   handoff tests pass.

An older Worker that encounters a marker containing a foreign-output binding must fail clearly as an
unsupported annotation version. It must not replay the Workflow while ignoring that binding.
Likewise, a capability-aware provider prefix rejects output mutations from SDKs that predate atomic
notification creation. Mixed source publisher versions must fail closed rather than create readable
records with no durable wake obligation.

## Required tests

### API and binding

- B can consume A output from `BEGINNING` and from an explicit opaque cursor.
- Missing, mismatched, reused, and cross-namespace source references are rejected before registration.
- A source and B input topic with the same name remain physically isolated.
- Source DataConverter context is used; consumer context is not substituted.
- Default deny, allowed authorization, caught denial replay, and authorization outage are distinct.

### Visibility and races

- B never observes output from a rejected A Workflow Task.
- Committing a stage notifies every existing B registration.
- Aborting a pending stage exposes and notifies later committed direct output.
- A pending stage is reconciled after both source and consumer Workers restart with neither Run
  cached.
- Registration-before-commit and commit-before-registration both deliver without polling luck.
- A registration concurrent with `FINISH` sees the terminal exactly once.
- Several commits coalesce without losing the latest readable tail.

### Wake durability

- A, B, and dispatcher crashes are injected before and after provider mutation, Signal send, Signal
  acknowledgement, and outbox acknowledgement.
- A parked B is woken after both Workers restart and neither Run was cached.
- A generation change during dispatch prevents stale acknowledgement and causes recomposition.
- Racing dispatchers send the same parked request ID.
- Later unparked visibility transitions use distinct request IDs.
- A closed or reused B chain is removed without deleting a successor registration.

### Replay and lifecycle

- B replay reads exact A output ranges and never the current tail.
- Missing first, middle, and last records; wrong counts; reordered offsets; and an unexpected pending
  barrier fail as integrity errors.
- Mixed local input, own output, and foreign output in one retained Workflow Task reproduce one
  activation-segment schedule.
- B Continue-As-New restores the source binding and cursor; a late predecessor cleanup preserves the
  successor registration.
- A Continue-As-New preserves the source stream; A Workflow ID reuse does not.
- B cancellation and completion clean up registrations; B termination is eventually cleaned by
  dispatcher target validation.
- Registration cleanup preserves replay-retention leases until B's replay window ends.
- A crash before Workflow Task acknowledgement may over-retain but cannot under-retain a delivered
  range.
- `FINISH` ends iteration, while source Workflow closure without `FINISH` does not.

### Scale and conformance

- Many B workflows receive one source topic independently.
- One slow B neither blocks A nor another B.
- Registration quotas fail new subscribers without dropping current ones.
- Every advertised provider passes atomic register/recheck, commit-notify, abort-notify, both outbox
  lease protocols, conditional revision acknowledgement, replay-retention, exact committed range,
  and injective key tests.

## Alternatives considered

### External relay from A output to B input

This works with today's APIs and remains the operational fallback. It duplicates every payload,
requires another service with cursor/idempotency state, doubles provider retention, and introduces a
second stream whose replay relationship to A must be managed by the application. It is simpler for
the SDK but does not provide native zero-copy Workflow-to-Workflow streaming.

### Let B call `ExternalOutputStreamClient`

Rejected. Backend reads, sleeps, and Temporal History requests inside Workflow code are
nondeterministic and bypass Workflow Task replay markers.

### Alias A output as B input

Rejected. It violates direction isolation, uses incompatible record and staging contracts, decodes
under the wrong Workflow context, and still does not solve the wake target.

### Long-running Activity in B

An Activity can read A and return batches, but Activity results place payloads in B's History unless
it republishes them to B's input stream, which reduces to the external relay. Activity cancellation,
heartbeat, and retry state also become part of the streaming protocol.

### Temporal Timer polling

A durable polling timer eventually rechecks after restarts but adds recurring History events and
fixed latency while idle. It may be offered later as an optional safety net, not as the primary
wakeup protocol.

### Provider pub/sub without a durable outbox

Rejected. Pub/sub is a useful local fast path, but a notification published while B's Worker is down
is lost. Correctness cannot depend on a process being continuously connected.

## Correctness invariants

Implementation must preserve all of the following:

1. **Committed-only visibility:** B receives no record from an uncommitted A Workflow Task.
2. **No lost readiness:** every readable-prefix advancement is observed by atomic registration
   recheck or creates a durable notification for every active registration; pending output that can
   block such advancement has durable reconciliation work.
3. **Wake durability:** a notification is not acknowledged until an idempotent Signal is accepted for
   the registration state it names.
4. **Exact replay:** B re-reads only ranges named by its marker and reproduces their global delivery
   order.
5. **Binding isolation:** source chain, direction, topic, provider, consumer chain, and wait ID cannot
   collide or be substituted.
6. **Source serialization:** live and replay decoding both use A's recorded serialization context.
7. **Monotonic visibility:** pending output is a barrier; commit and abort are irreversible and may
   only advance the readable prefix.
8. **Source independence:** a slow, failed, or closed B never blocks A's output commit or external
   clients.
9. **Successor safety:** stale cleanup or wakes from a predecessor Run cannot delete or satisfy a
   successor's registration.
10. **Replay retention:** active-registration cleanup cannot release any record while retained B
    History can still reference it.
11. **Payload-free Temporal transport:** markers and wake Signals contain identities, cursors,
    counts, and proofs, never stream payload bytes.

These invariants, rather than the illustrative class and method names, define the feature.
