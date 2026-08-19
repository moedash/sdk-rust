# Workflow streaming implementation follow-up review

Date: 2026-08-18

This is a static follow-up review of the fixes made after the independent
implementation review in `review-guide.md`. No tests were run for this review.
It covers correctness, durability, determinism, and liveness; it does not cover
code style.

The earlier review listed fifteen findings and now says that all fifteen are
fixed. The concrete reproducers for many of them have been addressed, but that
blanket status is too strong. Eight findings appear fully addressed. The seven
findings below are only partially addressed: their principal happy paths are
fixed, while adjacent failure modes still violate the same invariant.

The subscription-teardown finding also considers the current uncommitted
changes to `_api.py`, `_runtime.py`, and `_manager.py`. Those changes improve the
normal close path but do not close its failure path.

## P0 — Inherited park-intent reconciliation is attempted only once

**Related original finding:** “A park intent cannot be removed after a Worker
handoff.”

**Status:** Partially addressed.

Registration now detects and removes an intent installed by a previous Worker.
That fixes the ordinary handoff case. However,
`StreamSubscriptionManager._reconcile_inherited_park()` treats a removal error
as best-effort cleanup and retries only when the wait is registered again.
Registration normally happens once for the lifetime of the cached Run, so a
transient backend failure can leave the inherited intent in place indefinitely.

That intent is not inert. Both producer and Worker wakes consult
`current_park_generation()`, receive the generation of a park Core has already
left, and send a parked wake naming that dead generation. Core correctly rejects
it as stale. If no later registration occurs, the Workflow can remain asleep
with a record already buffered or durable in the stream.

**Code:**
`sdk-python/temporalio/contrib/external_workflow_streams/_manager.py`,
`StreamSubscriptionManager._reconcile_inherited_park`.

**Proposed unit test:** Have manager A install and confirm a park, then shut it
down while preserving the backend intent. Register the same wait on manager B
with a backend whose first `remove_park_intent()` call fails and whose later
calls succeed. Do not re-register the wait. Recover the backend and assert that
manager B retries until the intent is absent. Then append another record and
assert the wake uses generation zero rather than manager A's generation. The
current implementation makes one removal attempt and leaves the stale intent.

## P0 — Replay does not require an empty recorded subscription to be recreated

**Related original finding:** “Replay never verifies that recorded waits match
Workflow subscriptions.”

**Status:** Partially addressed.

Replay now verifies a binding when the corresponding subscription is
registered, and it fails when a recorded delivery remains unconsumed. Neither
check proves that every recorded binding was recreated. If a marker binds a
wait but records no deliveries for it, removing that `subscribe()` call leaves
no registration to compare and no delivery for the end-of-segment check to find.
Replay therefore accepts an annotation whose subscription set differs from the
one created by Workflow code.

This contradicts the feature's stated determinism rule that inserting,
removing, or reordering `subscribe()` calls is a versioned Workflow-code change.
An empty subscription is still present in the annotation header and terminal,
and its wait ID participates in numbering later subscriptions.

**Code:**
`sdk-python/temporalio/contrib/external_workflow_streams/_runtime.py`,
`WorkflowStreamRuntime.begin_replay`, `register`, and
`verify_replay_consumed`.

**Proposed unit test:** Build an annotation whose header binds wait 1 and whose
terminal contains wait 1 at `BEGINNING`, but whose segments contain no records.
Prepare and drive replay with Workflow code that creates no subscriptions.
Assert that replay raises `workflow.NondeterminismError` naming wait 1. The
current implementation completes replay because there is neither a
registration mismatch nor an unconsumed record.

## P1 — A failed live wake is not retried while the Worker remains running

**Related original finding:** “Wake and readiness failures have no working retry
path.”

**Status:** Partially addressed.

Readiness notifier failures are now bounded and guarded, and the shutdown sweep
can observe a raising wake callback and retry it. The live watcher path still
does not retry a Signal that fails. `_report_ready()` increments `wakes_owed`,
logs the send failure, and returns. The watcher has already advanced its
prefetch cursor past the buffered record, so its next `read_after()` waits after
that record. With no second append, `_report_ready()` is never called again.

The shutdown sweep is not a live retry policy. A Worker may remain healthy and
running for hours after a transient Signal failure, during which the Workflow
can remain asleep on data already held by that Worker.

**Code:**
`sdk-python/temporalio/contrib/external_workflow_streams/_manager.py`,
`StreamSubscriptionManager._report_ready` and `_watch`.

**Proposed unit test:** Buffer one record, have the readiness notifier return
`NO_OPEN_WORKFLOW_TASK`, and make the wake sender fail once and then succeed.
Append no additional records and do not invoke shutdown. Assert that the live
manager retries the same wake request and that the second send is acknowledged.
The current implementation makes only the failed first attempt.

## P1 — Cancellation bypasses park rollback

**Related original finding:** “A failed multi-wait park leaves a partial
externally visible park.”

**Status:** Partially addressed.

Ordinary exceptions during intent installation or recheck now withdraw every
intent installed by that attempt. `asyncio.CancelledError` is explicitly
re-raised before `_withdraw_park()` runs. Cancellation after the first install
therefore leaves a partial park visible in the backend even though Core never
received a confirmed result for that generation.

Cancellation is a normal async termination path during Worker shutdown and task
teardown. It must preserve the same all-or-nothing invariant as an ordinary
storage exception. Otherwise producers observe a non-zero generation for a
park that does not exist and send wakes Core discards as stale.

**Code:**
`sdk-python/temporalio/contrib/external_workflow_streams/_manager.py`,
`StreamSubscriptionManager._prepare_park` and `_withdraw_park`.

**Proposed unit test:** Register two waits. Let the first intent installation
succeed and make the second installation block on a controllable future. Cancel
the `prepare_park()` task while the second install is blocked, await its
`CancelledError`, and assert that neither wait has an intent for the attempted
generation. The current implementation leaves the first intent installed.

## P1 — Off-thread decoding drops Workflow serialization context

**Related original finding:** “Payload decoding performs arbitrary async work on
the Workflow thread.”

**Status:** Partially addressed.

External retrieval and payload-codec decoding now run on the Worker's async
loop, while synchronous payload conversion runs on the Workflow thread. That
removes asynchronous work from `activate()`. Both halves, however, are built
from the Worker's context-free `DataConverter`.

Ordinary Workflow payload decoding first binds a
`WorkflowSerializationContext` containing the namespace and Workflow ID.
Components implementing `WithSerializationContext` rely on that binding. The
stream manager's `_prepare()` and the runtime's `codec_for()` never apply it, so
context-aware payload codecs and payload converters see their context-free
instances or fail outright. External-stream values therefore do not use the
same converter semantics as other payloads delivered to the same Workflow.

**Code:**
`sdk-python/temporalio/worker/_workflow.py`,
`_WorkflowWorker._stream_manager` and `_create_external_stream_runtime`;
`sdk-python/temporalio/contrib/external_workflow_streams/_manager.py`,
`StreamSubscriptionManager._prepare`; and
`sdk-python/temporalio/contrib/external_workflow_streams/_runtime.py`,
`WorkflowStreamRuntime.codec_for`.

**Proposed unit test:** Configure a payload codec implementing
`WithSerializationContext`. Make its context-free `decode()` fail and make the
copy returned by `with_context()` record the supplied context and decode
successfully. Deliver one external-stream record through a Worker and assert
that decoding succeeds with a `WorkflowSerializationContext` containing the
correct namespace and Workflow ID. The current implementation invokes the
context-free codec.

## P1 — Closing a subscription can permanently abandon its park intent

**Related original finding:** “An unused or cancelled subscription remains
logically blocked.”

**Status:** Partially addressed, including the current uncommitted teardown
changes.

The committed implementation now starts an unused subscription outside the
blocked set, removes an abandoned readiness wait from that set, and provides a
public `close()` that ends iteration. The uncommitted follow-up additionally
schedules Worker-side cancellation, stops the watcher, and attempts to remove
the park intent.

The failure ordering in `StreamSubscriptionManager.cancel()` is still unsafe.
It removes the subscription from `_runs` before awaiting intent removal. If the
backend removal fails, the exception is logged and the watcher is stopped, but
no registered subscription or cleanup object remains to retry the removal. The
log says the intent will be reconciled if the Run is registered again; a closed
wait is not expected to be registered again. The stale intent can therefore
remain for the rest of the chain and corrupt subsequent producer wake
generation selection.

**Code:**
current uncommitted changes in
`sdk-python/temporalio/contrib/external_workflow_streams/_api.py`,
`ExternalStreamSubscription.close`;
`sdk-python/temporalio/contrib/external_workflow_streams/_runtime.py`,
`WorkflowStreamRuntime.unsubscribe`; and
`sdk-python/temporalio/contrib/external_workflow_streams/_manager.py`,
`StreamSubscriptionManager.cancel`.

**Proposed unit test:** Install a park intent for a subscription, make the first
`remove_park_intent()` call fail and subsequent calls succeed, and call
`subscription.close()`. Assert that the watcher stops, the wait remains outside
the blocked set, and cleanup is retained and retried until the backend intent is
absent. Finally assert that a producer sees no current park generation. The
current implementation drops the manager subscription after the failed removal
and never retries it.

## P1 — `merge()` still starves waits beyond one activation's budget

**Related original finding:** “`merge()` can starve every stream except the
lowest wait ID.”

**Status:** Partially addressed.

`merge()` now takes at most one record from each subscription per pass. This is
fair only when the number of ready subscriptions does not exceed
`MAX_RECORDS_PER_ACTIVATION`, currently 256. Every activation begins its pass at
the lowest wait ID. With 257 ready subscriptions, waits 1 through 256 can spend
the complete budget before wait 257 is considered. The next activation starts
at wait 1 again, so continuously backlogged lower waits starve wait 257
indefinitely.

The claim that skew is bounded to one record therefore needs either a bound on
the number of subscriptions below the activation budget or a deterministic
rotation carried across activation boundaries.

**Code:**
`sdk-python/temporalio/contrib/external_workflow_streams/_api.py`, `merge`.

**Proposed unit test:** Create 257 subscriptions. Keep waits 1 through 256
backlogged across at least three simulated activation-budget resets and place
one ready record on wait 257. Drive `merge()` using the same readiness rearming
performed at activation completion. Assert that wait 257 is yielded within a
bounded number of activations. The current implementation spends every
activation on waits 1 through 256 and never reaches it.

## Summary

The following original findings appear fully addressed by static inspection:

1. a producer losing a wake claim now sends the idempotent wake itself;
2. backend/provider identity is recorded and checked per wait;
3. late subscriptions are emitted in bindings frames;
4. stale readiness is re-reported against the current generation;
5. the park handshake uses Core's wait-set membership;
6. the external-storage failure taxonomy is connected to completions and
   metrics;
7. consumption advances only after successful decoding; and
8. Redis stream identity and intent scans are injective and glob-safe.

The seven sections above require additional work before the original review can
accurately state that all fifteen findings are fixed.

## Re-audit and implementation handoff — 2026-08-19

**The per-finding statuses above are the reviewer's own and are left as
written.** This section and the one after it supersede them; the table below is
the current one.

This section supersedes the statuses above for the current `sdk-python` working
tree. It is a static code review only; no build or test was run. Some fixes
described here are still uncommitted, so the next agent should preserve and
review the existing working-tree changes rather than rebuilding them from
scratch.

Current status, updated 2026-08-19 once the owed-removal ledger landed:

| Follow-up finding | Status | Design record |
|---|---|---|
| Inherited park-intent reconciliation is attempted only once | Fixed | ADR-031, `spec/wft-lifecycle.md` |
| Replay does not require an empty recorded subscription to be recreated | Fixed | ADR-033, `spec/annotation-format.md` |
| A failed live wake is not retried while the Worker remains running | Fixed | `spec/wft-lifecycle.md` |
| Cancellation bypasses park rollback | Fixed | ADR-032, `spec/wft-lifecycle.md` |
| Off-thread decoding drops Workflow serialization context | Fixed | ADR-035, `spec/python-runtime.md` |
| Closing a subscription can permanently abandon its park intent | Fixed | ADR-031, ADR-030 |
| `merge()` still starves waits beyond one activation's budget | Fixed | ADR-034, `spec/python-runtime.md` |

The first, the sixth, and the second door under the sixth — a close after a
failed reconciliation, which attempted no removal at all because the Worker's
mirror was empty — are one fix: a removal is owed by the **Run**, not recorded
on the `Subscription` every removal path happened to reach it through.

The replay fix now checks, after the final segment, that every wait bound by the
marker was recreated. The live wake path uses bounded retries. Park rollback
catches `BaseException`, so the first cancellation still withdraws the intents
already installed. `merge()` retains a deterministic cursor and resumes after
the last wait that took a record, bounding skew across activation-budget resets.
Each of these four changes has focused regression coverage in the working tree.

Three pieces of work remained when this was written. The third has since
landed; the two owed-removal items below are open, with dated status lines.

### P0 — Owed-removal cleanup can delete a successor Run's live intent

**Status (2026-08-19):** Open, and narrowed. The drain reads the intent and
removes it only when the recorded park generation *and* Run ID both still match,
so a replacement that is already in place when the drain reads is forgotten
rather than removed. A replacement installed between that read and the delete is
still removed. The residual window and what closing it needs — a conditional
delete as a provider obligation — are recorded in ADR-031 and in
`spec/wft-lifecycle.md`.

The new owed-removal ledger correctly preserves cleanup responsibility after a
subscription is dropped. Its drain is not atomic, however:

1. `_drain_owed_removals()` reads `park_intent(stream_key, wait_id)` and checks
   the recorded park generation and Run ID.
2. It later calls the unconditional
   `remove_park_intent(stream_key, wait_id)`.

A Continue-As-New successor reuses the same stream key and starts wait IDs at
1. Between those two backend calls, the successor can overwrite the old intent
with its own newly confirmed park. The predecessor's drain then removes the
successor's intent. Per-Run `_park_lock` instances do not close this race: the
two Runs have different locks, and they may be held by different Workers.

The existing `test_a_drain_never_removes_the_intent_of_a_park_that_replaced_it`
checks only replacement **before** the read. It does not exercise replacement
between the read and delete.

**Code:**
`sdk-python/temporalio/contrib/external_workflow_streams/_manager.py`,
`StreamSubscriptionManager._drain_owed_removals`.

**Required design change:** Make removal conditional on the intent still
matching the recorded generation and Run ID at deletion time. This likely
requires an atomic compare-and-delete backend operation and a corresponding
backend-contract/conformance obligation. A process-local or per-Run lock cannot
provide the required cross-Worker exclusion.

**Proposed unit test:** Use a backend that lets `park_intent()` return Run A's
intent and then pauses the drain before deletion. While paused, install Run B's
intent at the same `(stream key, wait_id)`, then resume Run A's cleanup. Assert
that Run B's intent remains installed. The current read-then-unconditional-delete
implementation removes it.

### P1 — The owed-removal ledger has no autonomous retry

**Status (2026-08-19):** Open, and deliberate for now. ADR-031 records the
background retry task as deferred rather than rejected, with the two things it
needs first: an owner that teardown awaits, and the conditional delete above —
an autonomous retry has the widest read-to-delete window of any drain.

Inherited reconciliation and `cancel()` now retry removal three times and keep
a per-Run ledger when all attempts fail. That fixes a one-blip reproducer and
prevents cleanup responsibility from dying with the `Subscription` object.
The ledger is drained only by another park, resolve, registration, or eviction.

If the backend remains unavailable for the bounded retry window and recovers
afterwards while the Run stays cached and idle, none of those events is
guaranteed to occur. The stale intent therefore still can remain indefinitely.
This affects both the inherited-intent and close findings. A stale intent keeps
`parked_wait_ids()` non-empty; producers omit the unparked fallback, send only a
wake naming the dead generation, and Core discards it as stale.

**Code:**
`sdk-python/temporalio/contrib/external_workflow_streams/_manager.py`,
`StreamSubscriptionManager._reconcile_inherited_park`, `cancel`, and
`_drain_owed_removals`.

**Required design change:** Give ledger entries an eventual retry mechanism
that does not depend on another Workflow/Core lifecycle event. A bounded
background retry task may be appropriate, provided teardown owns and awaits it
and retries remain generation-safe through the atomic operation described
above. Another possible trigger is the live readiness/wake path, but that alone
does not clean an idle stream on which no record arrives.

**Proposed unit test:** Make every immediate removal attempt fail, wait until the
retry budget is exhausted, then recover the backend without invoking park,
resolve, registration, eviction, or shutdown. Assert that the intent is
eventually removed. For the inherited case, append after recovery and assert
the resulting wake is unparked rather than naming the old generation. The
current passive ledger makes no further removal attempt.

### P1 — External-stream decoding still ignores serialization context

**Status (2026-08-19):** Fixed, symmetrically. The runtime is built on the
Worker's side with a converter bound to
`WorkflowSerializationContext(namespace, workflow_id)`; the manager, which is
shared across Runs, derives that context **per record** from the record's own
stream key — the subscription's on the live path, the annotation header's on
replay, since the Workflow has not re-subscribed yet; and the producer binds the
same context once at construction. All three key on `workflow_id`, because a
stream spans the Continue-As-New chain.

The producer half is the part this finding did not name and the part that makes
the fix safe to ship: producer and consumer were context-free **together**, so
binding only the consumer would have broken every working deployment whose codec
keys on the Workflow. `with_context` is used rather than `_with_contexts`; the
store context and the one gap left open are recorded in ADR-035 and
`spec/python-runtime.md`.

This finding is unchanged. The Worker's manager and each
`WorkflowStreamRuntime` receive the context-free `DataConverter`.
`StreamSubscriptionManager._prepare()` runs external retrieval and the payload
codec from that converter, and `WorkflowStreamRuntime.codec_for()` uses it for
the synchronous payload-converter half. Neither applies the
`WorkflowSerializationContext(namespace, workflow_id)` that ordinary Workflow
payload decoding supplies.

Consequently, a payload codec or payload converter implementing
`WithSerializationContext` sees its context-free instance for external-stream
records even though it sees a bound instance for every other payload delivered
to the same Workflow.

**Code:**
`sdk-python/temporalio/worker/_workflow.py`,
`_WorkflowWorker._stream_manager` and `_create_external_stream_runtime`;
`sdk-python/temporalio/contrib/external_workflow_streams/_manager.py`,
`StreamSubscriptionManager._prepare`; and
`sdk-python/temporalio/contrib/external_workflow_streams/_runtime.py`,
`WorkflowStreamRuntime.codec_for`.

**Required design change:** Bind the converter to the target Workflow's
serialization context before either decode half runs. Because the manager is
shared across Runs, storing one context-bound converter on the manager is not
sufficient; preparation needs the converter or context belonging to the
specific subscription/Run. The synchronous half must use the same bound
converter so codec and payload-converter behavior match ordinary activation
payload decoding.

**Proposed unit test:** Configure a payload codec implementing
`WithSerializationContext`. Its context-free `decode()` should fail, while the
copy returned by `with_context()` records the context and succeeds. Deliver one
external-stream record through a real Worker and assert that both live and
replay decoding receive a `WorkflowSerializationContext` with the correct
namespace and Workflow ID. Also cover a context-aware payload converter for the
synchronous half. No such regression test exists in the current tree.
