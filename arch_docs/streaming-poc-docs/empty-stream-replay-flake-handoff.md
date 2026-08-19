# Empty-stream replay flake: root-cause handoff

**Purpose:** focused handoff for the intermittent failure of
`test_an_empty_stream_parked_and_evicted_replays_from_the_recorded_cursor`.

**Analysis date:** 2026-08-19  
**Code analyzed:** `sdk-python` revision `5a887335`, `sdk-rust` revision `49150bf6`  
**Verification status:** static analysis only; the failing test was not executed as part of this
investigation. The workspace may contain uncommitted fixes for these findings, so compare against
`5a887335` when reproducing the original behavior.

## Resolution

**Addressed** in Python `8abb8eb8`. Findings 1 and 2 are fixed, all six required fix properties hold,
and the secondary test-harness race is fixed too. What follows is what was done and what the analysis got right and
wrong, kept because the "why this test fails frequently" reasoning is still the best map of this
interleaving.

| Item | Status |
|---|---|
| Finding 1 — replay performs an unrecorded extra drain | Fixed. The driver drains the first *k − 1* segments and leaves the last to the activation's own `_run_once`. |
| Finding 2 — reposition is fire-and-forget | Fixed. The retraction is synchronous on the Workflow thread; only the `asyncio.Event` wakeup is posted back. |
| Property 5 — reposition/append atomic against the epoch | Fixed. The epoch is compared *inside* `_append`, under the lock the reposition takes. |
| Property 6 — zero-segment annotations | Fixed, and it needed its own fix: see below. |
| Secondary test-harness race — eviction spy | Fixed. The spy records the Run *after* awaiting `evict_run`, so the test proceeds on teardown having finished rather than started. |

Three corrections to the analysis, all found by running the suite rather than reading it:

- **Property 6 was not free.** Deferring the close to the activation's drain is right for a marker
  *with* segments and wrong for one without: with no recorded segment for that drain to serve, the
  drain is a live one, and repositioning after it retracts exactly what it has just delivered. A
  zero-segment marker therefore closes inside the job, before that drain. This is the mirror image of
  Finding 2 and was introduced by the fix for Finding 1.
- **The flake is not fully explained by Findings 1 and 2.** The test still failed after both were
  fixed, and — decisively — **it also fails at `5a887335` with the fixes stashed**, in one of three
  baseline full-suite runs. It is a pre-existing load-dependent flake that these defects made more
  likely, not a failure they alone produce. A second, unrelated pre-existing flake
  (`test_bridge.py::test_the_status_probe_is_repeatable`) failed in two of five runs across both
  states.
- **Closing a replay from a bare `finally` masks real errors.** Not in this document, and a direct
  consequence of moving the close after the activation's drain: the consumed check raises whenever a
  recorded delivery is still armed, which it always is after a failed drain, and that replaces the
  exception being propagated. A failing activation now abandons the replay instead of closing it.

The proposed unit tests were written as far as the available harness allows. The epoch-atomicity
variant is covered as a contract on `_append` — a single-threaded loop cannot place a reposition
between a check and an append that have no await between them, whereas in production the two run on
different threads. The coalesced-readiness variant is covered at the runtime/manager level with a
poison record the annotation does not name, driving the activation *shape* rather than a real
`activate()`; building a `_WorkflowInstanceImpl` in a unit test was judged out of proportion, and the
end-to-end coverage stays where it was.

Core was not changed: `maybe_issue_external_stream_resolve()` still has no replay guard, as this
document says it need not.

## Executive summary

The test is flaky because replay hands control back to the live stream buffer before the replay
activation has actually finished, and the cursor/buffer reposition intended to make that handoff
safe is only queued on another event loop.

Two implementation defects combine:

1. `_apply_replay_external_streams()` performs one `_run_once()` for every recorded segment, but
   `activate()` performs its normal `_run_once()` after applying the activation jobs as well. A
   marker with *k* recorded segments therefore gets *k + 1* drains.
2. Before the activation's final drain, `_apply_replay_external_streams()` queues
   `reposition_to_committed()` and immediately calls `end_replay()`. The reposition is implemented
   with `call_soon_threadsafe()` and has not necessarily run when `end_replay()` changes `drain()`
   back to the live manager buffer.

If a `ResolveExternalStreamWaits` job is coalesced into the replay activation, it resolves the
Workflow's pending stream future. The activation's extra final drain can then consume whatever the
watcher placed in the live buffer. Whether it sees old, future, poison, or no records depends on
the scheduling order of the Workflow executor and the manager event loop.

This test makes the race likely rather than creating it: `alpha` and `beta` already exist when the
evicted Run is rebuilt, and the recorded-ranges-only replay backend immediately returns late
"poison" records from every live `read_after()` path.

## Causal path

1. The evicted Run is reconstructed and calls `subscribe()` while replaying its history.
2. Registration starts a normal live watcher on the manager loop. The stream already contains
   records, so that watcher can fill its live buffer and notify Core immediately.
3. Core accepts readiness during replay and can coalesce a `ResolveExternalStreamWaits` job with
   pending replay work. `maybe_issue_external_stream_resolve()` has no replay guard.
4. Python applies `ReplayExternalStreams`. The replay driver:
   - arms each recorded segment;
   - resolves pending stream futures;
   - calls `_run_once()` for every segment;
   - verifies consumption;
   - requests cursor reposition;
   - leaves replay mode.
5. Cursor reposition has only been posted to the manager loop. There is no happens-before edge
   between the post and the remainder of the Workflow activation.
6. Python applies the coalesced `ResolveExternalStreamWaits`, which resolves the stream future.
7. `activate()` performs its standard final `_run_once()`. Replay mode has already ended, so
   `ExternalStreamRuntime.drain()` falls through to `StreamSubscriptionManager.drain()` and can
   consume the live buffer.
8. The manager-loop reposition and watcher refill race that drain. The resulting observation is
   scheduler-dependent.

## Finding 1: replay performs an unrecorded extra drain

Severity: **P1**

The replay driver's comment says the live Run's *k* activations become *k* drains. Its surrounding
caller makes that statement false:

- [`_apply_replay_external_streams()`](../../../sdk-python/temporalio/worker/_workflow_instance.py)
  calls `_run_once()` once for every `plan.segments` entry.
- [`activate()`](../../../sdk-python/temporalio/worker/_workflow_instance.py) subsequently calls
  `_run_once()` for the non-query job set.

The extra drain is already a determinism bug for `wait_condition()` because a condition can be
evaluated one more time than History records. For external streams it is worse: replay mode is
cleared before that drain, allowing it to become a live drain.

### Proposed deterministic unit test

Drive replay through `activate()`, not by calling `_apply_replay_external_streams()` directly.
Construct one activation containing:

- `InitializeWorkflow`;
- `ReplayExternalStreams` for an annotation with exactly one empty segment; and
- `ResolveExternalStreamWaits` after the replay job.

Use a Workflow that subscribes and blocks, and preload the manager's live buffer with one poison
record not present in the annotation. Assert all of the following:

- exactly one Workflow event-loop drain represents the one recorded segment;
- the poison record is not delivered;
- the Workflow remains blocked after reproducing the empty segment; and
- replay does not produce a new observation delta for the poison record.

The pre-fix implementation can deliver the poison record during `activate()`'s final drain.

## Finding 2: replay cursor reposition is fire-and-forget

Severity: **P1**

[`ExternalStreamRuntime.reposition_after_replay()`](../../../sdk-python/temporalio/contrib/external_workflow_streams/_runtime.py)
calls `StreamSubscriptionManager.reposition_to_committed()`, but
[`reposition_to_committed()`](../../../sdk-python/temporalio/contrib/external_workflow_streams/_manager.py)
only posts `_reposition_to_committed()` with `call_soon_threadsafe()` and returns.

The caller treats that method as a completed handoff: it immediately enters `finally` and calls
`end_replay()`. At that moment:

- the live buffer may still contain records covered by the marker;
- `delivery_cursor` and `prefetch_cursor` may still describe the old position; and
- the watcher may still have a read in flight from the old prefetch epoch.

The epoch check protects a reposition that has already happened. It cannot protect a live drain
that occurs before the queued reposition begins.

### Proposed deterministic unit test

Create a manager and registered subscription with a stale, marker-covered record already buffered.
From the Workflow-side thread:

1. call `reposition_to_committed(run_id, {wait_id: recorded_boundary})`;
2. do not yield the manager loop; and
3. immediately call `drain(run_id, wait_id)`.

Assert that the stale record is absent and that committed, delivery, and prefetch cursors already
equal the recorded boundary. The pre-fix implementation returns before any of those postconditions
hold.

A second concurrency variant should pause a watcher after its backend read but before append, run
the reposition, then release the watcher. Assert that the old-epoch records cannot be appended.
This catches an incomplete fix where reposition becomes cross-thread synchronous but the epoch
check and append remain separate critical sections.

## Why this particular test fails frequently

The test contains both high-probability triggers:

- After eviction, it publishes `alpha` and `beta` before waking the Run. A reconstructed
  subscription's watcher therefore does not need to wait for a future append.
- [`RecordedRangesOnlyBackend.read_after()`](../../../sdk-python/tests/contrib/external_workflow_streams/test_replay_end_to_end.py)
  returns the late poison records immediately on its first live read. Those records are not named
  by any marker, so any delivery is observable as either replay nondeterminism or a different final
  observation.

Depending on the winning schedule, symptoms can include:

- the resumed live Run timing out;
- a standalone `Replayer` result containing `replay_failure`;
- `late-one` or `late-two` appearing in replay observations;
- a recorded segment being left unconsumed because the Workflow completed from live data first; or
- replayed records being delivered twice after a stale buffer survives the handoff.

## Required fix properties

A safe fix should preserve all of these properties:

1. For *k* recorded segments, replay performs exactly *k* event-loop drains.
2. The activation's ordinary drain serves as the final recorded drain. The replay driver should
   internally drain only the first *k - 1* segments and leave the last segment armed.
3. Replay verification, cursor reposition, and `end_replay()` occur only after the activation's
   ordinary drain.
4. Buffer and cursor reposition is synchronous with respect to the Workflow thread. Only the
   `asyncio.Event` wakeup needs to be posted back to the manager loop.
5. Reposition and watcher append are atomic with respect to the prefetch epoch. A watcher either
   appends before reposition and is cleared, or observes the changed epoch and discards its read.
6. Zero-segment annotations still finish replay without accidentally manufacturing a live drain.

Suppressing readiness in Core while replaying could reduce the interleavings, but it does not
repair the Python driver's *k + 1* segmentation or the false synchronous contract of cursor
reposition. Those defects need direct fixes.

## Secondary test-harness race

The eviction spy in the end-to-end test appends `run_id` to `evicted` before awaiting the real
`evict_run()`. The test then waits only for membership in that list before publishing records.
Consequently, the assertion proves that teardown started, not that watcher teardown and Core's
eviction activation completion finished.

This should be tightened by recording completion after `await evict_run(...)`, or by using an event
whose meaning is explicitly "manager teardown complete." If the test requires Core eviction
completion rather than only watcher teardown, it needs a later synchronization point because the
Worker completes the Core eviction activation after manager teardown returns.

This test-harness issue can add timing noise, but it does not explain failures in the two standalone
replays performed after the live Run has finished. The replay-to-live handoff defects do.

## Source map

- Python activation scheduling and replay driver:
  [`temporalio/worker/_workflow_instance.py`](../../../sdk-python/temporalio/worker/_workflow_instance.py)
- Runtime replay/live drain selection:
  [`temporalio/contrib/external_workflow_streams/_runtime.py`](../../../sdk-python/temporalio/contrib/external_workflow_streams/_runtime.py)
- Watcher, buffer, epoch, and reposition logic:
  [`temporalio/contrib/external_workflow_streams/_manager.py`](../../../sdk-python/temporalio/contrib/external_workflow_streams/_manager.py)
- Core readiness coalescing:
  [`managed_run.rs`](../../crates/sdk-core/src/worker/workflow/managed_run.rs)
- Failing test and poison backend:
  [`test_replay_end_to_end.py`](../../../sdk-python/tests/contrib/external_workflow_streams/test_replay_end_to_end.py)

