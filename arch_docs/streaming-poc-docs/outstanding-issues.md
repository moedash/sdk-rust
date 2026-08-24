# Outstanding issues

Written 2026-08-24; dispositions updated 2026-08-24. **All eight reviewed issues are resolved.**
Issues 2 and 7 were one product defect: follow-up wakes were not tied to the Workflow Task cycle
they caused, so replay could manufacture more work in the closing window. Issue 6 received the
plan's narrowly scoped Core hardening rather than a global `dbg_panic!` policy change.

This is a plain register of what is left. Detail and evidence for items 1-4 live in
[`wft-double-dispatch-flake-handoff.md`](wft-double-dispatch-flake-handoff.md), which is long and
layered because it records three passes of investigation; this file is the short version and says
what state each thing is actually in.

Two things this file names live on the task volume rather than in any repository, and are not
version-controlled: `TASK_STATUS.md` and the `streaming-review-findings/` round that produced
finding 14.

The behavioral resolution commits are `sdk-python` `82b3c7d0`, `86144a0a`, `4eca5b45`, and
`2994f4fa`, plus Core `c4504901`; `sdk-python` `163019c6` points the vendored Core at that hardening.
The old **653 pass, 1 fail** external-stream-suite result is retained only as the pre-fix incident
baseline. Post-fix evidence includes 10 canary and 100 acceptance repetitions of each formerly
failing replay test, all passing, followed by three consecutive complete external-stream-suite
runs at **658 passed** each. Static cleanup is recorded in `sdk-python` `bff2571f`, `552e4f01`, and
`380fe874`. The complete validation record also lives in the task-volume `TASK_STATUS.md`.

## Summary

| # | Issue | Type | Disposition |
|---|---|---|---|
| 1 | Registry test fails during sandbox import | Test flake | **Resolved** |
| 2 | Replay tests time out on rejected Workflow Task reports | Product defect | **Resolved** |
| 3 | Empty-stream replay test rejects a legitimate race | Test bug | **Resolved** |
| 4 | No stateful test for the Core admission fix | Missing coverage | **Resolved** |
| 5 | Five tests create a Core worker and never shut it down | Test hygiene | **Resolved** |
| 6 | `dbg_panic!` behaviour on release builds | Local Core hardening | **Resolved** |
| 7 | Repeated follow-up wakes on `NoOpenWorkflowTask` | Product defect | **Resolved with #2** |
| 8 | `TASK_STATUS.md` says there are no known defects | Stale doc | **Resolved** |

---

## 1. Registry test fails during sandbox import

**Resolved.** `NoOpWorkflow` is now explicitly unsandboxed because these tests exercise Worker
construction rather than sandboxing. The two successful registry constructions also use an
`async with Worker(...)` lifetime, so their Core workers shut down orderly. The focused registry
cases pass, including 32/32 concurrent repetitions of the formerly flaky construction.

The same pytest-rewrite import path later appeared in the handoff and end-to-end replay modules.
Every external-stream test module that defines a sandboxed Workflow and constructs a Worker or
Replayer now declares `PYTEST_DONT_REWRITE`, keeping pytest's assertion-rewriter internals outside
the Workflow sandbox. The handoff Workflow is unsandboxed because that test exercises durable
cross-Worker state, not sandbox behavior. Three consecutive complete suite runs pass after this
broader fix.

At the reviewed baseline,
`tests/contrib/external_workflow_streams/test_registry.py::test_a_conforming_backend_registers_on_a_worker`
(line 114) failed intermittently in full-suite runs only, with `RuntimeError: Failed validating
workflow NoOpWorkflow`.

The chain, captured by the follow-up investigation: constructing the `Worker` validates
`NoOpWorkflow`, which makes the sandbox re-import `test_registry.py`; that module was rewritten by
pytest's assertion rewriter, so the import pulls in `_pytest.assertion.rewrite`, and nested imports
reach CPython's `_ModuleLock.acquire`, whose `finally` does `del _blocking_on[tid]` and raises
`KeyError`.

It happens *before* the Core worker is created, so it is unrelated to items 2 and 6.

**Suggested fix:** the test is not about the sandbox. Stop it depending on a sandbox import of its own
pytest-rewritten module — declare `NoOpWorkflow` unsandboxed, or move it to a helper module pytest
does not rewrite. Whether the general sandbox/import-lock interaction deserves an SDK fix is a
separate question; the importer's own docstring already warns it mutates `sys.modules` and
`builtins.__import__` process-wide.

**Warning:** I earlier declared this cause "disproven" and wrote "do not spend time on this again"
into the handoff. That was wrong — my test imported ordinary modules rather than a pytest-rewritten
workflow module, so it never exercised the path. Ignore that entry.

## 2. Replay tests time out on rejected Workflow Task reports

**Resolved.** The retained trace established that issue 7 sustained the incident. A buffered
readiness report received `NoOpenWorkflowTask`, sent a wake, and then another readiness generation
or replayed subscription sent a distinct wake before Core had accepted the successful completion of
the task the first wake caused. Those Signals repeatedly entered the execution's closing window,
where Workflow Task reporting and signaling rejected one another with `UnhandledCommand` and
`BUSY_WORKFLOW`; eviction reconstructed the readiness and repeated the cycle.

The fix has two boundaries:

- terminal Workflow completions do not re-arm buffered readiness because that Run cannot consume a
  later activation;
- nonterminal readiness is coalesced to one Run-wide wake cycle until a later activation completes
  successfully. The cycle survives eviction/replay, correlates completion to each individual RPC
  retry attempt, and opens again for a genuinely later activation. Cleanup of a stale park intent
  explicitly invalidates the wake it silenced so the required unparked reannouncement is preserved.

The deterministic regressions cover replay reconstruction, an already-running task completing
during a wake send, a failed send attempt followed by a successful retry, genuinely new buffered
ranges, terminal completion, and stale-intent cleanup. Both live replay tests passed 10 canary and
100 acceptance repetitions apiece under four concurrent pytest workers (**220 total**). The timeout
harness remains in the tests and retains complete History plus correlated manager state if the
incident recurs.

At the incident baseline, two tests in `test_worker_integration.py` showed it:
`test_a_replayed_run_re_registers_its_wait_set` (line 632) and
`test_a_replayed_record_is_prepared_off_the_workflow_thread` (line 1033). Both time out at
`wait_for(handle.result(), 30)`.

What happens: the server rejects Core's Workflow Task report with `RESOURCE_EXHAUSTED`,
`ResourceExhaustedFailure` cause 5 = `BUSY_WORKFLOW`, message *"workflow operation can not be applied
because workflow is closing"*. Core evicts the run with "Error reporting WFT to server", replays it,
and hits the same rejection — **exactly 85 times**, in every occurrence observed.

It is one defect, not two flaky tests: runs before and after the Core fix, on the same rebuilt
binary, produced the identical signature and the same 85 iterations in *different* tests of the same
file. Both are replay-after-eviction cases, and it appears to land on whichever one is running.

Each replay re-announced the buffered record, so a failing run fired ~255 wake attempts (85 × 3)
that were rejected the same way. The correlated post-fix investigation established these were not
merely passengers: allowing a failed attempt to claim a completion that occurred before its retry
re-opened the gate and immediately reproduced distinct follow-up wakes. Resetting correlation at
each attempt removed that sequence and passed the bounded stress gate.

**Two of my experiments on this are void. Do not build on either:**

- A harness suppressing only the manager's wake Signals failed 15/15 with **zero** loop iterations
  and counters `{'test': 0, 'manager_suppressed': 1}`. The test's own Signal never reached the gate
  and the one call suppressed was the legitimate delivery wake, so those runs measured a broken
  harness. If you rebuild this, assert the gate sees the test's Signal before trusting any result.
- A baseline-vs-current comparison is confounded: the reproductions had an instrumentation probe
  attached and the baseline could not have it, so probe-vs-no-probe was mixed in with
  changes-vs-baseline. Like-for-like without the probe, both sides were 0 failures.

**Ruled out:** finding 14 (`sdk-python` `7392fbdb`). Measured, not reasoned — a probe over the manager
during two independent reproductions recorded `wake_park_generation_ledger_hit` 0,
`wake_after_cleanup_called` 0, `flush_nonempty` 0 across 256 and 262 wake compositions. The
owed-removal ledger *did* populate (2 entries, via inherited-park reconciliation), so the new code
paths were reachable and simply were not taken.

**Preserved constraint:** do not treat `BUSY_WORKFLOW` as terminal in `_send_owed_wake`.
`BUSY_WORKFLOW` is a transient server signal meaning retry later. Dropping the wake would lose a
record, which is the exact failure the whole external-stream wake mechanism exists to prevent.

## 3. Empty-stream replay test rejects a legitimate race

**Resolved.** The test now proves there was no Workflow Task failure and no workflow-body restart at
the completed cache-eviction boundary. After the deliberate wake it permits only
`WORKFLOW_TASK_FAILED_CAUSE_UNHANDLED_COMMAND`, while continuing to reject all other causes and to
check the marker, cursor, ranges, and offline replays. Because that retry may execute the workflow
body again, the observation assertion checks at least the live run plus two explicit replays and
requires every observation to equal the live result rather than requiring exactly three executions.
The case passed 16/16 concurrent repetitions.

`test_replay_end_to_end.py:874` previously asserted that **no** Workflow Task failed, on the grounds
that a failure would make the second execution ambiguous evidence of the eviction. That rejected an outcome
Temporal considers normal: a generation-0 wake Signal can arrive while the workflow is issuing its
terminal command, the server rejects that task with `UnhandledCommand` so the Signal is not lost, and
it schedules a replay. The workflow then completes correctly with the right records. Core's own
integration tests state `UnhandledCommand` is acceptable when an external event races a command.

Reproduces at roughly 12/32 in focused concurrent batches.

**Suggested fix:** keep the direct cache-eviction assertion and the `starts[0:2] == [False, True]`
check, and permit `UNHANDLED_COMMAND` specifically while still rejecting nondeterminism and other
causes. Keep all the record, marker, range and offline-replay assertions — those are the claims the
test exists to make.

## 4. No stateful test for the Core admission fix

**Resolved at the level required by the review.** A stateful Core regression now constructs a real
`ManagedRun` and two `PermittedWFT`s, clears the first activation while leaving its Workflow Task
outstanding with no pending jobs, verifies that the replacement task is buffered, reports the first
task, and verifies that the replacement drains into the run. The test passes and covers the
production call site in addition to the existing predicate tests. All 21 tests in the
`managed_run` unit-test module passed before defensive recovery was added; all 22 debug-profile
tests now pass. This is the minimum regression the review required. The
lane-controlled integration test it called optional — poll-result lane against post-completion lane —
was **not** added, because the harness does not expose those inputs without sleeps; a sleep-based
race test would not be worth its flakiness.

At the reviewed baseline, `sdk-rust` `57503951` fixed the admission gate (`must_buffer_wft` now
includes `has_wft`) and added three unit tests over the condition, with a mutation check confirming
they cover it. What was *not* covered was the sequence that triggers it: a polled task winning a race against the local
post-completion message that clears `ManagedRun.wft`. That needs a test able to control Core's two
input lanes, which the existing harness does not obviously allow.

Before the stateful regression above, the fix was justified by the invariant it restored rather
than by a reproduction.

## 5. Five tests create a Core worker and never shut it down

**Resolved.** All five successful constructor-only cases now use `async with Worker(...)`; the two
parameter values make six focused executions, all passing. Constructor-failure tests remain direct
constructions because no Worker exists to close when validation raises. The handoff and Worker
integration fixtures also retain and await their worker/shutdown tasks, eliminating the related
"Task was destroyed but it is pending" teardown warnings.

At the reviewed baseline, `test_registry.py:115` and `:144`; `test_continuation.py:634`, `:672`,
`:697` each constructed a `Worker`, which eagerly created a real Core worker, asserted something
about the constructor, and dropped it.

This is **not** the cause of any failure above — I held 400 such workers live in one process with no
failure. It is ordinary hygiene worth cleaning up, and no more than that.

## 6. `dbg_panic!` behaviour on release builds

**Resolved locally.** All production admissions were audited. New-run construction starts with no
task; polled tasks for an existing Run pass through `buffer_wft_if_outstanding_work`; and the task
buffer drains only after `has_any_pending_work` confirms the outstanding WFT is gone. The stateful
item-4 regression mutation-kills removal of the `has_wft` guard.

`_incoming_wft` is nevertheless defensive now. It retains the debug `dbg_panic!`, but the release
path buffers the replacement and returns instead of overwriting the outstanding task and losing its
task token. Fault-injection tests prove the debug build panics at the invariant and the release build
preserves the original token, buffers the replacement, and drains it after the original task is
reported. All 22 debug-profile `managed_run` tests and the release-only recovery test pass. No other
`dbg_panic!` call site or macro behavior changed.

## 7. Repeated follow-up wakes on `NoOpenWorkflowTask`

**Resolved with issue 2.** The mechanism was a correctness/liveness risk, not only an efficiency
concern: distinct follow-up wakes could sustain the Workflow Task rejection/replay loop. Readiness
now has an explicit lifecycle rule:

> With records buffered and no open Workflow Task, one Run-wide wake remains outstanding until an
> activation that began during that exact acknowledged send attempt completes successfully. Replay
> and concurrent waits coalesce behind it. A later completed cycle, a failed send, or retirement of
> a park generation known to have silenced the wake permits the next required wake.

The counter still advances between completed cycles, so genuinely later readiness is not
deduplicated away. Retries within one cycle keep the same counter/request ID.

## 8. `TASK_STATUS.md` says there are no known defects

**Resolved.** `TASK_STATUS.md` now identifies the historical implementation sections as
point-in-time records, reports the current repository heads and remediation state, records all
eight issues as resolved, and distinguishes the complete release gate from earlier focused and
statistical checks.

At the reviewed baseline, line 466 read "None outstanding from any of the four reviews". That
predated the current findings round (`streaming-review-findings/01`–`16`) and everything in this
file. It was not corrected during the investigation because the other fifteen findings had not yet
been verified; the comprehensive status refresh above supersedes it.

---

## One process note

The single most expensive mistake in this work was running a **stale native extension** for the first
half of the investigation. Several results I reported — including a "654 tests pass" figure for the
finding-14 work — were measured against a binary that predated the Core commit under test. The
finding-14 work held up when re-run correctly, but the earlier numbers meant nothing.

This is hazard 1 in `verification-hazards.md`, and checking it costs one
command:

```bash
find temporalio/bridge/sdk-core/crates temporalio/bridge/src -name '*.rs' \
     -newer temporalio/bridge/temporal_sdk_bridge.abi3.so | head
```

Any output means rebuild with `uv run maturin develop --uv` before believing a Python result. Also:
run the suite with `--tb=long`. Plain `-q` prints a truncated table with no traceback and no captured
output, which is why item 1 went twelve runs without a diagnosis.
