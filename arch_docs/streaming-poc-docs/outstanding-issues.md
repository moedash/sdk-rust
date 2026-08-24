# Outstanding issues

Written 2026-08-24. Everything here is **open**. Nothing in this file is fixed.

This is a plain register of what is left. Detail and evidence for items 1-4 live in
[`wft-double-dispatch-flake-handoff.md`](wft-double-dispatch-flake-handoff.md), which is long and
layered because it records three passes of investigation; this file is the short version and says
what state each thing is actually in.

Two things this file names live on the task volume rather than in any repository, and are not
version-controlled: `TASK_STATUS.md` and the `streaming-review-findings/` round that produced
finding 14.

Current tree: `sdk-python` `85aa5c57`, `sdk-rust` / Core submodule `57503951`, native extension
rebuilt 2026-08-23 20:50 UTC. External stream suite: **653 pass, 1 fail** — the 1 is item 2.

## Summary

| # | Issue | Type | Diagnosed? |
|---|---|---|---|
| 1 | Registry test fails during sandbox import | Test flake | Yes |
| 2 | Replay tests time out on rejected Workflow Task reports | Unknown — possibly product | **No** |
| 3 | Empty-stream replay test rejects a legitimate race | Test bug | Yes |
| 4 | No end-to-end test for the Core admission fix | Missing coverage | n/a |
| 5 | Five tests create a Core worker and never shut it down | Test hygiene | Yes |
| 6 | `dbg_panic!` behaviour on release builds | Design question | Partly |
| 7 | Repeated follow-up wakes on `NoOpenWorkflowTask` | Efficiency | Partly |
| 8 | `TASK_STATUS.md` says there are no known defects | Stale doc | Yes |

---

## 1. Registry test fails during sandbox import

`tests/contrib/external_workflow_streams/test_registry.py::test_a_conforming_backend_registers_on_a_worker`
(line 114). Fails intermittently in full-suite runs only, with `RuntimeError: Failed validating
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

`test_replay_end_to_end.py:874` asserts that **no** Workflow Task failed, on the grounds that a
failure would make the second execution ambiguous evidence of the eviction. That rejects an outcome
Temporal considers normal: a generation-0 wake Signal can arrive while the workflow is issuing its
terminal command, the server rejects that task with `UnhandledCommand` so the Signal is not lost, and
it schedules a replay. The workflow then completes correctly with the right records. Core's own
integration tests state `UnhandledCommand` is acceptable when an external event races a command.

Reproduces at roughly 12/32 in focused concurrent batches.

**Suggested fix:** keep the direct cache-eviction assertion and the `starts[0:2] == [False, True]`
check, and permit `UNHANDLED_COMMAND` specifically while still rejecting nondeterminism and other
causes. Keep all the record, marker, range and offline-replay assertions — those are the claims the
test exists to make.

## 4. No end-to-end test for the Core admission fix

`sdk-rust` `57503951` fixed the admission gate (`must_buffer_wft` now includes `has_wft`) and added
three unit tests over the condition, with a mutation check confirming they cover it. What is *not*
covered is the sequence that triggers it: a polled task winning a race against the local
post-completion message that clears `ManagedRun.wft`. That needs a test able to control Core's two
input lanes, which the existing harness does not obviously allow.

Until that exists, the fix is justified by the invariant it restores rather than by a reproduction.

## 5. Five tests create a Core worker and never shut it down

`test_registry.py:115` and `:144`; `test_continuation.py:634`, `:672`, `:697`. Each constructs a
`Worker`, which eagerly creates a real Core worker, asserts something about the constructor, and
drops it.

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

Line 466 reads "None outstanding from any of the four reviews". That predates the current findings
round (`streaming-review-findings/01`–`16`) and everything in this file. It should not be trusted.

I did not correct it because doing so honestly means stating the status of the other fifteen findings,
which I have not verified.

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
