# Outstanding issues

Written 2026-08-24; dispositions updated 2026-08-24. Issues **1, 3, 4, 5, and 8 are resolved in the
current working trees**. Issues 2 and 7 remain open. Issue 6 is deferred as a broader Core design
question rather than a confirmed streaming defect.

This is a plain register of what is left. Detail and evidence for items 1-4 live in
[`wft-double-dispatch-flake-handoff.md`](wft-double-dispatch-flake-handoff.md), which is long and
layered because it records three passes of investigation; this file is the short version and says
what state each thing is actually in.

Two things this file names live on the task volume rather than in any repository, and are not
version-controlled: `TASK_STATUS.md` and the `streaming-review-findings/` round that produced
finding 14.

Heads this register was last reconciled against, immediately before the remediation for items 1, 3,
4, 5 and 8 was committed: `sdk-python` `d9ccd4d1`, `sdk-rust` / Core submodule `7b21cd99`. The most
recent full external-stream-suite baseline remains **653 pass, 1 fail** — the 1 is item 2, and that
baseline must not be read as a clean final-suite result. Focused validation of the resolved items is
recorded in the task-volume `TASK_STATUS.md`.

## Summary

| # | Issue | Type | Disposition |
|---|---|---|---|
| 1 | Registry test fails during sandbox import | Test flake | **Resolved** |
| 2 | Replay tests time out on rejected Workflow Task reports | Unknown — possibly product | **Open** |
| 3 | Empty-stream replay test rejects a legitimate race | Test bug | **Resolved** |
| 4 | No stateful test for the Core admission fix | Missing coverage | **Resolved** |
| 5 | Five tests create a Core worker and never shut it down | Test hygiene | **Resolved** |
| 6 | `dbg_panic!` behaviour on release builds | Design question | **Deferred** |
| 7 | Repeated follow-up wakes on `NoOpenWorkflowTask` | Efficiency | **Open with #2** |
| 8 | `TASK_STATUS.md` says there are no known defects | Stale doc | **Resolved** |

---

## 1. Registry test fails during sandbox import

**Resolved.** `NoOpWorkflow` is now explicitly unsandboxed because these tests exercise Worker
construction rather than sandboxing. The two successful registry constructions also use an
`async with Worker(...)` lifetime, so their Core workers shut down orderly. The focused registry
cases pass, including 32/32 concurrent repetitions of the formerly flaky construction.

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

The one currently-failing test. Two tests in `test_worker_integration.py` show it:
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

**The cause is not established.** Each replay re-announces the buffered record, which owes a wake, so
a failing run also fires ~255 wake Signals (85 × 3 attempts) that are rejected the same way. Whether
those Signals *sustain* the loop by contending for the workflow lock or merely ride it is the open
question, and it decides whether any fix belongs in the manager at all.

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

**Suggested next steps, in order:** capture the failing workflow's History (the decisive question is
why the execution is "closing" but never closes — `handle.result()` times out, so it had not); find
what makes the count exactly 85, since a constant across independent occurrences suggests a cap or a
timeout ratio rather than an open-ended loop; then settle sustain-or-ride with a working harness.

**Trap:** do not "fix" this by treating `BUSY_WORKFLOW` as terminal in `_send_owed_wake`.
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
`managed_run` unit-test module pass. This is the minimum regression the review required. The
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
constructions because no Worker exists to close when validation raises.

At the reviewed baseline, `test_registry.py:115` and `:144`; `test_continuation.py:634`, `:672`,
`:697` each constructed a `Worker`, which eagerly created a real Core worker, asserted something
about the constructor, and dropped it.

This is **not** the cause of any failure above — I held 400 such workers live in one process with no
failure. It is ordinary hygiene worth cleaning up, and no more than that.

## 6. `dbg_panic!` behaviour on release builds

`dbg_panic!` is `error!` plus `debug_assert!(false, ...)`. On a debug build (what this container uses)
the admission violation killed the workflow-processing thread. On a release build it logged and
carried on into code not written to hold two Workflow Tasks for one run.

Item 4's fix closes that particular path, but the general question stands: if an invariant like this
can be violated under load, an assertion is the wrong tool and the branch needs a real recovery path.
Worth deciding deliberately rather than per-site.

## 7. Repeated follow-up wakes on `NoOpenWorkflowTask`

Noted during the Failure C investigation: each `NoOpenWorkflowTask` readiness result counts an owed
wake and sends a Signal, and repeated replay registration can re-read and re-announce the same
buffered records. One captured run settled after three follow-ups. If timing keeps placing each
Signal inside a terminal Workflow Task, this can loop through safe-but-wasteful retries — which is
also the mechanism suspected in item 2.

Not a correctness failure on its own. Worth bounding explicitly, and worth a test.

## 8. `TASK_STATUS.md` says there are no known defects

**Resolved.** `TASK_STATUS.md` now identifies the historical implementation sections as
point-in-time records, reports the current repository heads and remediation state, lists #2/#7 as
the remaining open work, classifies #6 as deferred rather than a known defect, and records the
focused validation without presenting it as full CI.

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
