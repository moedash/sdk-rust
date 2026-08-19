# Review guide — External Workflow Streams

**This is a review artifact, not part of the design.** Everything else in this
directory states what is true now and carries no history; this file is the
exception, because a reviewer needs to know what to review. It is a snapshot,
valid at the two commits stamped below and stale the moment anything else lands.

## The two repositories

The feature spans a Rust core and a Python SDK that vendors it. Both live on the
branch `task/python-sdk-streaming`.

| | Repository | Base | Head at writing |
|---|---|---|---|
| Python SDK | `temporalio/sdk-python` (fork `mfateev/sdk-python`) | `680a6b4f` | `62b3ff12` |
| Core | `temporalio/sdk-rust` (fork `mfateev/sdk-core`) | `6e90e6d5` | `3abd2a46` |

Core is vendored at `sdk-python/temporalio/bridge/sdk-core` and pinned to
`3abd2a46`, so reviewing the Python repository at its head reviews both. These
documents live in the Core repository, which is why the submodule carries them.

**Change surface**, excluding the vendored copy from the Python totals:

| | Commits | Files | Lines |
|---|---|---|---|
| Python | 34 | 75 | +21,918 / −146 |
| Core | 18 | 62 | +12,224 / −119 |
| Core, excluding these documents | | 23 | +9,110 / −119 |

## Where everything is

On disk, under `/Users/maxim/workarea/workspaces/projects/tasks/python-sdk-streaming/`:

| Path | What it is |
|---|---|
| `sdk-python/` | The Python SDK. Everything Python-side is here. |
| `sdk-python/temporalio/bridge/sdk-core/` | Core, vendored as a submodule, pinned to the head below. **These documents are inside it.** |
| `sdk-rust/` | A second checkout of the same Core repository and branch. |

The two Core checkouts share one branch, so either shows the same content; the
vendored one is what the Python build compiles and what the test suite reads
these lists from.

## Reading order

1. [`README.md`](README.md) — the full map of this document set.
2. [`overview.md`](overview.md) — what the feature is and the cost model that
   motivates it.
3. [`spec/wft-lifecycle.md`](spec/wft-lifecycle.md) and
   [`spec/core-lang-protocol.md`](spec/core-lang-protocol.md) — the contract
   between Core and lang, which is where most of the difficulty lives.
4. [`decisions/README.md`](decisions/README.md) — 30 records, one per decision,
   each with the alternatives that were rejected. Code comments say why the code
   is as it is; only these say why the other option was not taken.
5. [`verification-hazards.md`](verification-hazards.md) — before running
   anything, or judging any test result.
6. The commits below, in order.

## Python commits

**Setup** — `ec200384`, `8cdaa2e7`: submodule moved to the task fork and the
bridge build fixed. No feature content.

**Foundations** — `245ccbaf` protos, record model, Redis fixture · `e179ab61`
backend contract, annotation codec, failure taxonomy · `42ff4593` parking
contract, Redis provider, registry, producer, bridge · `f9b199ea` subscription
manager and the Workflow-facing API · `e533438c` regenerated protos ·
`4c6ddc5a` wiring into the Worker.

**Deliverables** — `4f95dbe9` replay read path · `f9ee870b` producer wake-signal
path · `65e48faf` `publish()` acknowledged-wake semantics · `6fbd4dcc`
Continue-As-New cursor · `c25123af` multiple streams and `merge` · `f202383c`
Worker shutdown wake sweep · `ead76442` Milestone 2 required tests.

**Gates, and the defects writing them exposed** — `3cc7c246` makes the milestone
gates enforceable · `4d8bdcfe` failure-taxonomy row and P20's missing retry ·
`6bfe3cfd` replay through the real `Replayer` · `8e58be14` a record buffered
while Workflow code was elsewhere · `b30b3dff` a livelock that did not exist ·
`7313e00f` the per-activation delivery budget · `faa1c84c` four defects ·
`838810f7` unparked wake sender identity · `c70ec80c`, `ca4008dd` rollover cases
· `ac12d277` gate at 53/55 · `0ce689df` the handoff window · `c301f17e` four
more defects · `93f03068` the deadlock closed · `8d5ee1f1` the last case ·
`62b3ff12` these documents' new home. Submodule bumps: `d6ed30ac`, `d191bbde`,
`08f1206f`.

## Core commits

**Design** — `67a15f15` introduced these documents.

**Implementation** — `6ebf7eb2` protos, wait-set types, local-work retention ·
`78638ebf` input routing and run-level timers · `dec56bd8` quiescence retains
the Workflow Task · `4ebffd4f` readiness, deltas, rollover state · `18326120`
reserved wake-Signal interception · `ba250f3b` the marker machine · `5c2a36b5`
finalization protocol and durable rollover · `d40f310d` park handshake and
replay marker lookahead · `f5e9ba21` shutdown and eviction transitions.

**Corrections** — `4ea2956b` anchors the rollover deadline at the Workflow
Task's start · `a82ea4f0` queues the resolve job before the activation is built
· `d4c59441` separates registering a wait set from retaining for it · `8d12894b`
sender identity · `08a3c8bc` the delivery budget.

**Documents** — `2631687a`, `e3044d7f`, `3abd2a46`.

## Where to look hardest

Writing the required-test lists found seventeen defects in code that a
429-test suite already called green. The later ones share a shape worth carrying
into the review: **two concepts fused that the specification treats as
separate**, where each half behaves correctly alone. Unit tests structurally
cannot see these.

The four that were hardest to get right, and are the most worth re-deriving from
the specification rather than reading for plausibility:

- **Registering a wait set versus retaining the Workflow Task.** Fusing them
  made a Workflow that starts a timer and first blocks on a stream in the same
  activation unresumable by any wake — a permanent deadlock in ordinary user
  code. `spec/wft-lifecycle.md`, and `managed_run.rs`.
- **The annotation's terminal versus its header.** Losing either produces a
  marker that cannot be decoded at all, and three separate defects did.
  `spec/annotation-format.md`, `_runtime.py`, `_workflow_instance.py`.
- **The wake request ID.** A parked wake must ignore sender identity so racing
  producers collapse to one Workflow Task; an unparked wake must not, or two
  Workers' wakes deduplicate into one and a Run stalls. `spec/wake-signal.md`.
- **Probing a Run's state versus acting on the answer.** They cannot happen at
  the same point in shutdown; Core's state lane closes in between.

## Known state, including what is not finished

- 463 Python tests pass with nothing skipped, marked, or expected to fail; 101
  external-stream and 492 workspace tests in Core; clippy and `fmt` clean apart
  from two warnings that predate this work in `crates/client/src/{dns,lib}.rs`.
- Both required-test gates are met: Milestone 1 at 55/55, Milestone 2 at 12/12.
  **The gate checks that every case maps to a test that exists, not that it
  passes** — deliberately, since the suite already checks the latter.
- **Not closed:** rollover bounds the Workflow Task, not an activation, so an
  activation that outlives the Workflow Task timeout still reaches a
  `dbg_panic` in Core. Making that non-fatal is a design decision, not a bug
  fix, and was left rather than taken unilaterally.
- Pre-existing lint debt in Python files this work touched (`E722`, `E731`) was
  left alone; `ruff format` is clean throughout.
- `verification-hazards.md` records two ways a test result in this repository
  can be confidently wrong. Both produced written defect reports against correct
  code before they were understood. A reviewer running anything should read it
  first.

## Running it

A live Temporal dev server and Redis are required; `start-env.sh` in the task
directory starts both. Then, from `sdk-python`:

```
uv run maturin develop --uv          # from the repository root, not temporalio/bridge
uv run pytest tests/contrib/external_workflow_streams/ -q
```

In the vendored Core: `cargo test --lib external_stream` and
`cargo test --workspace --lib`.

## Independent implementation review — 2026-08-18

Reviewed at Python `e6d4cd92` and Core `414c28a4`. This section reports
correctness, durability, determinism, and liveness defects only; it deliberately
contains no code-style findings. The already-documented activation-over-WFT-timeout
`dbg_panic` is also excluded because it is explicitly listed above as unfinished.

**Status lines were added afterwards**, from Python `7e040b7e` onward; the
reviewer's text under each is unchanged, including where a fix took a different
shape from the proposed test. **All fifteen findings are fixed**, all of them
Python-side; Core is unchanged since the review.

### P0 — A park intent cannot be removed after a Worker handoff

**Status:** Fixed — Python `0442dc4e`, *Reconcile an inherited park intent, and make parking match
Core's wait set*. Registration is now the second enforcement point: a Worker that finds an intent
for a wait it is registering, having installed none itself, removes it (`spec/wft-lifecycle.md`).

**Code:** `sdk-python/temporalio/worker/_workflow.py:874-883` and
`sdk-python/temporalio/contrib/external_workflow_streams/_manager.py:684-735`

`ResolveExternalStreamWaits` is processed before user Workflow code runs. On a
fresh Worker, there are therefore no subscriptions yet. Even if registration has
already happened, `resolve_park()` removes an intent only when the local
`Subscription.installed_park_generation` field is set. That field existed only
on the Worker that installed the park and is lost on eviction or shutdown; the
intent itself correctly survives in the external backend.

The first producer wake after a handoff resumes the Workflow, but the old intent
remains. If the resumed Workflow Task produces a command and closes, the next
append observes that stale non-zero generation instead of sending an unparked
wake. Core rejects it as stale; repeated parked wakes also derive the same
request ID and may be server-deduplicated. The Workflow can then wait forever
with a durable record already present.

**Proposed unit test:** Use two `StreamSubscriptionManager` instances sharing
one memory backend. Manager A registers a wait and confirms a park, then is
evicted without deleting the external intent. Have manager B process the
resolve job before registering the reconstructed subscription (the real Worker
ordering), then register it. Assert that the old `(stream key, wait_id)` intent
is removed. It currently remains. Extend the assertion with a second append and
verify that its wake is unparked rather than the stale generation from A.

### P0 — `publish()` can report an acknowledged wake when nobody sent one

**Status:** Fixed — Python `a9c03eca`, *Bind each wait to its own backend, and stop reporting unsent
wakes as sent*. The producer that loses the claim signals anyway; the duplicate collapses because a
parked wake's request ID ignores sender identity (`spec/wake-signal.md`).

**Code:**
`sdk-python/temporalio/contrib/external_workflow_streams/_producer.py:283-339`
and `:382-434`

The public contract says returning from `publish()` means the record is durable
and the wake was acknowledged. When `claim_park_generation()` returns `False`,
however, `wake()` silently skips that target and returns success. The assumption
that the other claimant “will send” is false when it crashed after claiming but
before signaling. A lease permits takeover after expiry; it does not schedule a
future retry. If this publish is the last producer action, no caller remains to
take over and the parked Workflow is stranded.

**Proposed unit test:** Pre-install a park and an unexpired short lease owned by
`"crashed"`, then make a second producer perform the only `publish()`. Assert
that the call either waits for lease expiry and sends an acknowledged Signal or
raises `WakeNotAcknowledgedError` with a retryable pending wake. It currently
returns an offset immediately, sends no Signal, and still sends none when the
lease later expires.

### P0 — Replay never verifies that recorded waits match Workflow subscriptions

**Status:** Fixed — Python `a9c03eca`, *Bind each wait to its own backend, and stop reporting unsent
wakes as sent*. The binding is verified at registration and at the start of replay, and a replay
that ends holding an unconsumed recorded delivery fails; both are reported as nondeterminism rather
than integrity loss. Only the stream *name* is compared, because a replay harness supplies its own
namespace and Run ids.

**Code:**
`sdk-python/temporalio/contrib/external_workflow_streams/_manager.py:737-771`,
`sdk-python/temporalio/contrib/external_workflow_streams/_runtime.py:251-296`,
and `sdk-python/temporalio/worker/_workflow_instance.py:838-889`

Replay preparation trusts the annotation's own `wait_id -> StreamKey` table.
Later, Workflow code registers its current subscriptions, but nothing compares
their stream keys, backend selections, or start cursors with that table.
Delivery is joined solely by integer `wait_id`, and the replay driver does not
check that every prepared delivery was consumed. Consequently, changing wait 1
from stream A to stream B can deliver A's historical bytes through B's
subscription instead of producing the row-four nondeterminism required by the
failure taxonomy. Removing a subscription can likewise discard that wait's
prepared deliveries without a direct mismatch error.

**Proposed unit test:** Create an annotation in which wait 1 is bound to stream
`"left"` and contains one record. Prepare it, then run the unchanged replay
driver with Workflow code that registers wait 1 as stream `"right"`. Assert a
`workflow.NondeterminismError` is raised before any value is yielded. Today the
left-hand value is yielded from the right-hand subscription.

### P0 — The annotation cannot identify the backend that owns each wait

**Status:** Fixed — Python `a9c03eca`, *Bind each wait to its own backend, and stop reporting unsent
wakes as sent*. Schema version 2 moves the provider identity into a per-wait binding and adds the
Worker-registered `backend_name` the Workflow chose; a version-1 annotation is rejected rather than
read as though its single label applied everywhere.

**Code:**
`sdk-python/temporalio/contrib/external_workflow_streams/_annotation.py:189-194`,
`sdk-python/temporalio/contrib/external_workflow_streams/_runtime.py:701-724`,
and `sdk-python/temporalio/contrib/external_workflow_streams/_manager.py:753-769`

The API permits each topic to name a different registered backend, but the
annotation has one global `provider_id` and format version. `_header()` takes
those values from the first subscription, while replay assigns every wait to
the first registered backend with that provider ID. This is ambiguous even for
two Redis instances using different clusters or key prefixes, because they
share a provider ID. With different provider types, one wait simply has no
backend. The recorded `provider_format_version` is never checked at all, so an
incompatible implementation with the same provider ID is accepted and read.

This is a schema-level defect: replay needs a stable backend/provider binding
per wait, not one provider label for an entire multi-backend annotation.

**Proposed unit test:** Register two separate memory backend instances under
`"left"` and `"right"`; both declare the same provider ID. Put wait 1's recorded
range only in the left instance and wait 2's only in the right, then call
`prepare_replay()` before subscriptions exist. Assert each range is read from
its owning instance. The implementation routes both to the first instance.
Parameterize the same test with a matching provider ID but a different
`provider_format_version` and assert replay rejects it before any read; today it
reads it silently.

### P1 — A subscription created after the first delta is absent from the header

**Status:** Fixed — Python `1e369ec3`, *Bind a late subscription, and replay a segment in its
recorded order*. A late wait's binding rides its own frame (ADR-027). The same commit fixed a
second defect this one did not name: a replay drain searched its segment for its own wait id, so a
segment recorded as (wait 2, wait 1) replayed in the reverse order.

**Code:**
`sdk-python/temporalio/contrib/external_workflow_streams/_runtime.py:251-296`
and `:689-717`

The annotation header is emitted when the accumulator is first created and can
never be extended. `register()` nevertheless permits a new subscription at any
later activation in the same retained Workflow Task. The otherwise-unused
`announced` flag is set for the waits present in the first header but is never
used to encode later bindings. If the new wait receives a record, the marker
contains a run for that wait but no stream key or start cursor for it; replay of
unchanged code fails as “Workflow did not create” that wait.

**Proposed unit test:** Register wait 1 and flush the first observation delta so
the header is fixed. Register wait 2, record a delivery for it, add the terminal,
and replay the resulting annotation with the same two subscriptions. Assert the
plan is valid and wait 2's record is delivered. It currently fails because wait
2 is missing from `header.streams`.

### P1 — A `Stale` readiness acknowledgement strands the buffered record

**Status:** Fixed — Python `0c92c99f`, *Make readiness reporting total, and stop dropping a stale
answer*. The report is re-sent against the generation Core currently holds, bounded.

**Code:**
`sdk-python/temporalio/contrib/external_workflow_streams/_manager.py:553-622`

Core's contract says `Stale` means “re-probe; do not signal.” The manager treats
it exactly like `Accepted` and returns. The watcher already appended the record
and advanced `prefetch_cursor`; its next backend read starts after that record.
Unless another record happens to arrive, nothing re-reports the buffer and the
Workflow remains blocked forever.

**Proposed unit test:** Buffer exactly one record and use a notifier that returns
`Stale` once, then updates the subscription to Core's current wait generation
and returns `Accepted`. Assert the notifier is invoked again without a second
append and the record remains available for delivery. Today it is invoked once
and the watcher blocks after the buffered record.

### P1 — Wake and readiness failures have no working retry path

**Status:** Fixed — Python `0c92c99f`, *Make readiness reporting total, and stop dropping a stale
answer*. The Worker's wake callback now raises, the watcher guards its own call to it, a raising
notifier is treated as the sixth answer the five do not name, and the sweep's retries and
`external_stream_shutdown_wake_failed` are reached.

**Code:** `sdk-python/temporalio/worker/_workflow.py:1024-1086` and
`sdk-python/temporalio/contrib/external_workflow_streams/_manager.py:553-635`
and `:937-973`

The Worker's wake callback catches Signal errors and returns normally. The
manager therefore counts an unacknowledged wake as success: the live watcher
does not retry, and the shutdown sweep's three-attempt loop exits after its
first call without incrementing `external_stream_shutdown_wake_failed`. The
comment that the watcher retries “on its next pass” is not true once prefetch
has moved past the only buffered record.

The opposite failure mode is also terminal: an exception from
`notify_external_stream_ready` is outside the watcher's backend-read `try`, so
it escapes `_watch()` and permanently ends the watcher task. Thus changing the
wake callback to propagate failures is not sufficient without giving readiness
reporting its own retry/error policy.

**Proposed unit test:** Use `NO_OPEN_WORKFLOW_TASK` readiness and a Signal sender
that fails twice then succeeds. Drive the real Worker callback through
`_sweep_wake()` and assert three send attempts and no failure metric; with an
always-failing sender, assert three attempts and one metric increment. Today
both cases make one attempt and report success. Add a parameterized case where
the Core notifier itself fails once, then accepts, and assert the watcher stays
alive and retries; today its task exits.

### P1 — The park handshake ignores Core's exact wait set

**Status:** Fixed — Python `0442dc4e`, *Reconcile an inherited park intent, and make parking match
Core's wait set*. The park set is the job's `waits`; the runtime supplies only each wait's cursor
boundary.

**Code:** `sdk-python/temporalio/worker/_workflow.py:945-950` and
`sdk-python/temporalio/contrib/external_workflow_streams/_manager.py:639-682`

`PrepareExternalStreamPark.waits` is Core's complete blocked snapshot, but the
Worker ignores it and passes `runtime.blocked_snapshot()`, which includes every
registered subscription. The manager then ignores even that mapping's
membership: it installs and rechecks every manager subscription, using a cursor
fallback for absent waits. A registered subscription that is not currently
blocked can therefore abort parking for the actual blocked set, and intents are
created for waits Core is not parking.

**Proposed unit test:** Register waits 1 and 2, but invoke `prepare_park()` with a
blocked map containing only wait 1. Make wait 2 have an immediately available
record. Assert only wait 1 receives an intent/recheck and the park confirms.
Today wait 2 is installed and rechecked, its record returns `became_ready=True`,
and the legitimate park is aborted.

### P1 — A failed multi-wait park leaves a partial externally visible park

**Status:** Fixed — Python `0442dc4e`, *Reconcile an inherited park intent, and make parking match
Core's wait set*. Every intent an attempt installed is withdrawn on any failure, and the original
storage error propagates.

**Code:**
`sdk-python/temporalio/contrib/external_workflow_streams/_manager.py:639-682`

Park intents are installed one at a time with no rollback around either the
install loop or the final rechecks. If installing wait 2 fails after wait 1 was
installed, or a recheck raises after all installs, the activation fails but the
completed intents remain visible to producers. An eviction then loses the local
`installed_park_generation` bookkeeping while preserving those orphaned
backend objects. Producers can send non-zero wakes for a park Core never
confirmed, which Core correctly discards as stale.

**Proposed unit test:** Use two waits and a backend that raises on the second
`install_park_intent` (and, as a parameterized case, on a recheck). Assert
`prepare_park()` propagates the storage error only after removing every intent
installed for that attempted generation. Today the first case leaves wait 1's
intent and the second leaves both.

### P1 — The specified external-storage failure taxonomy is not connected

**Status:** Fixed — Python `8ac891f6`, *Decode off the Workflow thread, and connect the failure
taxonomy*. Every failed completion is inspected rather than an exception, because a decode raised on
the Workflow thread is converted inside `activate()` and never exists as one out here; all three
failing rows now reach a completion carrying the external-storage cause and increment exactly one
counter (`spec/failure-taxonomy.md`). The same commit renames the shutdown counter to the
`temporal_external_stream_shutdown_wake_failed` the taxonomy module documents.

**Code:**
`sdk-python/temporalio/contrib/external_workflow_streams/_errors.py:90-180`,
`sdk-python/temporalio/contrib/external_workflow_streams/_replay.py:235-255`,
`sdk-python/temporalio/contrib/external_workflow_streams/_api.py:525-528`, and
`sdk-python/temporalio/worker/_workflow.py:524-569`

`StreamMetrics` and `classify_read_failure()` have no production caller.
Replay read failures do get storage/integrity Python exception types, but the
activation failure path never sets
`WORKFLOW_TASK_FAILED_CAUSE_EXTERNAL_STORAGE_FAILURE` and never records the
storage or integrity counters. Payload conversion exceptions are allowed to
escape raw, so `StreamDecodeError` is never constructed in production and its
counter cannot fire. This collapses the operator-distinct rows the design says
must remain separate.

**Proposed unit test:** Parameterize a focused Worker activation test over (1) a
backend read exception, (2) a missing/count-mismatched recorded range, and (3) a
validated record whose codec raises. Assert the failed completion has external
storage as `force_cause`, exposes respectively `StreamStorageError`,
`StreamIntegrityError`, and `StreamDecodeError`, and increments only the matching
counter. All three assertions fail in the current integration.

### P1 — Payload decoding performs arbitrary async work on the Workflow thread

**Status:** Fixed — Python `8ac891f6`, *Decode off the Workflow thread, and connect the failure
taxonomy*. Decoding is split at the one seam that permits it: retrieval and the user's codec run on
the Worker's loop — in the watcher before buffering, and over the replay plan's segments — while the
Workflow thread runs only the synchronous `from_payloads` that needs the topic's type (ADR-028,
`spec/python-runtime.md`). A record that arrives unprepared under a converter with a codec or
external storage is refused rather than decoded late. (`0c92c99f` had changed the order of decode and
consumption, not where decoding happens.)

**Code:**
`sdk-python/temporalio/contrib/external_workflow_streams/_api.py:426-439`
and `:525-528`,
`sdk-python/temporalio/contrib/external_workflow_streams/_codec.py:52-68`, and
`sdk-python/temporalio/worker/_workflow.py:494-522`

The manager buffers raw payload bytes, and the async iterator awaits the full
`DataConverter.decode()` inside `workflow.activate()`. A DataConverter can run
an async payload codec or external-payload retrieval; normal activation payload
decoding is awaited before entering the Workflow executor for this reason.
External stream decode can therefore perform real I/O in the deterministic
event loop, synthesize Workflow commands from codec awaits, or exceed the
two-second deadlock timeout. This violates the stated invariant that the
Workflow thread only pops already-prepared values from bounded buffers.

**Proposed unit test:** Configure a payload codec whose `decode()` records the
thread/loop and blocks longer than the deadlock timeout, then deliver one
buffered stream record through a Worker activation. Assert decoding completes
on the Worker's async/manager side and the Workflow receives the value without
`_DeadlockError`. It currently runs in the Workflow executor and deadlocks (or
fails the codec's loop assertion).

### P1 — A record is consumed before decoding succeeds

**Status:** Fixed — Python `03969dbf`, *Make merge fair, and stop consuming a record before it
decodes*. Decoding happens first; consumption is committed only once the value is in hand.

**Code:**
`sdk-python/temporalio/contrib/external_workflow_streams/_api.py:426-439` and
`:477-528`

`_take()` removes the record and advances the consumption cursor before
`await _decode(record)`. If decoding is cancelled, or Workflow code catches a
converter error and continues, the value was never yielded but its cursor is
committed as consumed. It is absent from the ready list and can be skipped by a
Continue-As-New successor; the same ordering also spends the delivery budget
on a value the Workflow never received.

**Proposed unit test:** Use a codec whose decode awaits a controllable future.
Start `anext(subscription)`, cancel it while decode is suspended, and then
assert the consumption cursor is unchanged and a subsequent `anext()` retries
the same record. Today the cursor is after the record and the record has
disappeared.

### P1 — An unused or cancelled subscription remains logically blocked

**Status:** Fixed — Python `03969dbf`, *Make merge fair, and stop consuming a record before it
decodes*, completed by `88c3578d`, *Finish close(): stop the watcher and take back the park intent*.
A subscription nobody has iterated is not blocked, a cancelled wait leaves the blocked set, and
`close()` ends the iteration. The Worker-side teardown the proposed test also asks for is now part of
it: closing hops onto the manager's loop, which removes the wait's park intent and then stops its
watcher (ADR-029, ADR-030). What closing keeps is the wait's recorded state, which replay and a
Continue-As-New successor both still read.

**Code:**
`sdk-python/temporalio/contrib/external_workflow_streams/_api.py:393-441` and
`:490-523`, plus
`sdk-python/temporalio/contrib/external_workflow_streams/_runtime.py:88-126`

Subscription state starts with `blocked=True`, before Workflow code has ever
iterated it. `_await_readiness()` also sets it true, but its cancellation
`finally` only removes the pending future. It never clears the blocked state or
cancels the manager subscription; `_finished` is initialized but is never set
and there is no public close/cancel path. Merely constructing a subscription,
or racing it against a timer and cancelling the losing wait, therefore reports
a quiescent wait that no coroutine is awaiting. Core can retain and eventually
park the Workflow Task solely for that ghost wait, while its watcher remains
alive for the Run.

**Proposed unit test:** First create a subscription without iterating it and
assert `runtime.quiescent_snapshot()` is empty. Then start `anext()` on that
empty subscription, let it register its readiness future, cancel and await the
task, and assert the snapshot is empty again (and closing the subscription tears
down its manager watcher and park intent). The current snapshot contains the
wait in both cases and no API path performs the teardown.

### P1 — `merge()` can starve every stream except the lowest wait ID

**Status:** Fixed — Python `03969dbf`, *Make merge fair, and stop consuming a record before it
decodes*. Each pass takes at most one record per subscription, which bounds the skew between two
streams at a single record without a rotating start position that replay would have to reproduce.

**Code:**
`sdk-python/temporalio/contrib/external_workflow_streams/_api.py:338-351`

Each merge pass drains a subscription's entire ready list before considering
the next wait. The first wait can fill the complete 256-record activation
budget by itself. After readiness is re-armed and the budget resets, iteration
again starts at the same lowest wait ID. A continuously backlogged first stream
therefore prevents a ready record on every later stream from ever being
yielded. The ordering is deterministic, but it is not a functioning merge.

**Proposed unit test:** Give wait 1 an inexhaustible source and wait 2 one ready
record. Drive at least three activation-budget cycles, resolving/re-arming as
the Worker does, and assert wait 2 is yielded within a bounded number of cycles.
It is never yielded by the current wait-ID-first whole-buffer drain.

### P1 — Redis maps distinct logical streams to the same physical keys

**Status:** Fixed — Python `ee4fbf86`, *Make the Redis key layout injective, and stop a stream name
widening a scan*, with the obligation lifted into the contract by `7e040b7e`, *Require a provider's
key derivation to be injective* (`spec/backend-contract.md`).

**Code:**
`sdk-python/temporalio/contrib/external_workflow_streams/_redis.py:133-148`
and `:277-286`

Redis keys are formed by joining unrestricted identity fields with `:` and no
escaping or length prefix. Two valid `StreamKey` tuples can therefore collide.
For UUIDs `r1` and `r2`, for example,
`("ns", "wf", r1, f"{r2}:tokens")` and
`("ns", f"wf:{r1}", r2, "tokens")` produce the same Redis key. Their records,
idempotency hashes, park intents, and claims become one data structure. In
addition, `parked_wait_ids()` inserts the unescaped key into a Redis glob, so a
stream name containing `?`, `*`, or `[` can enumerate another stream's intents.

**Proposed unit test:** Construct the two keys above and assert
`RedisStreamBackend.stream_key()` differs, then append one distinct record to
each and verify isolated reads. The first assertion currently fails. Add a
parameterized stream name containing each Redis glob metacharacter and assert
`parked_wait_ids()` returns only intents for the exact logical stream.
