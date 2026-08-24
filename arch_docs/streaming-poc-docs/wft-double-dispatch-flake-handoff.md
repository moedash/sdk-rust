# Handoff: WFT double admission and two independent full-suite flakes

**Update (2026-08-23, third pass): Failure B is fixed; a fourth failure, D, is open.**

- **B is fixed** in `sdk-rust` `57503951`, pointer moved in `sdk-python` `85aa5c57`, extension
  rebuilt. `must_buffer_wft` now includes `has_wft`, matching the docstring. Three unit tests, and
  the mutation check holds: removing `has_wft` fails exactly the regression case and nothing else.
  Buffering rather than admitting is safe because the drain side already requires
  `!has_any_pending_work(false, true)`, which counts `wft.is_some()` -- the task waits precisely
  until the WFT in flight clears. The end-to-end poll/completion ordering test remains unwritten;
  it needs control of Core's two input lanes.
- **D is new and open.** See "Failure D" below. Do not assume it is B: B's panic is absent from
  every run in which D was observed.

**Earlier status (follow-up investigation, 2026-08-23):** three separate problems are present.
Nothing was fixed and no product source was changed. This update supersedes the conclusions in the
original investigation, which remains below as an audit trail.

The most important correction is verification-related: the original investigation ran a stale
native extension. The loaded bridge predated ADR-041, so its reproduction could not implicate that
change. After rebuilding the bridge, Failure A was reproduced and diagnosed independently, and
Failure C was reproduced at a high rate with its complete History captured. Failure B did not
reproduce against the rebuilt bridge, but reading Core exposed a concrete, longstanding admission
bug that exactly permits the observed panic.

Read `verification-hazards.md` before continuing.

---

## Follow-up conclusions

| Failure | Current conclusion | Severity / ownership |
|---|---|---|
| A — registry `Worker(...)` failure | Pytest assertion-rewriter import fails inside the Workflow sandbox. It occurs before the bridge Worker is created and is unrelated to the Core panic. | Test/sandbox flake; blocks suite reliability, but the observed path is pytest-specific. |
| B — `Trying to send a new WFT for a run which already has one!` | Core's WFT admission gate fails to buffer a new WFT merely because `self.wft` is still present. This permits the exact panic during the completion-response race. | Serious Core correctness defect. Longstanding; not introduced by streaming. |
| C — empty-stream replay `UnhandledCommand` | A generation-0 wake Signal races the Workflow's terminal command. The server safely rejects that WFT and retries; the Workflow then completes correctly. The test rejects an expected race outcome. | Test assertion defect, plus possible redundant-wake efficiency/liveness follow-up. |

Finding 14 is not the cause of any of these failures. Its new behavior is gated on owed park-removal
state, and Failure B creates none. Failure C also reproduces against the current committed finding-14
tree and follows the ordinary `NoOpenWorkflowTask` wake path.

## Failure D -- a replay-after-eviction timeout from rejected WFT reports

| | |
|---|---|
| Tests | `test_worker_integration.py::test_a_replayed_run_re_registers_its_wait_set` (line 722) and `::test_a_replayed_record_is_prepared_off_the_workflow_thread` (line 1108) |
| Symptom | `asyncio.TimeoutError` on `wait_for(handle.result(), 30)` |
| Mechanism | the server rejects Core's Workflow Task report with `RESOURCE_EXHAUSTED`, `ResourceExhaustedFailure` cause 5 = `BUSY_WORKFLOW`, message "workflow operation can not be applied because workflow is closing"; Core evicts with "Error reporting WFT to server", replays, and repeats -- **exactly 85 iterations** in every observed occurrence |
| Cause | **not established** |

It is one defect, not per-test flakiness: the pre- and post-B runs against the same rebuilt bridge
produced the identical signature and the same 85 iterations in two *different* tests of the same
file. Both are replay-after-eviction cases; it appears to land on whichever one is running.

Each replay re-announces the buffered record, which owes a wake, and that Signal is rejected the same
way -- so a failing run carries ~255 wake Signals (85 x 3 attempts) alongside the 85 report
rejections. Whether those Signals *sustain* the loop by contending for the workflow lock, or merely
ride it, is the open question, and it decides whether the fix belongs in the manager at all.

**Finding 14 is ruled out here by measurement, not by reading.** A probe patched over the manager
during two independent reproductions recorded: `owe_removal` 2 (the ledger *does* populate, via
inherited-park reconciliation, so the new paths were reachable), `wake_park_generation_calls` 256 and
262, and `wake_park_generation_ledger_hit` **0**, `wake_after_cleanup_called` **0**, `flush_nonempty`
**0**. Composition was identical to the pre-finding-14 behaviour on every call.

### An invalidated experiment -- do not build on it

An attempt to settle the sustain-or-ride question by suppressing only the manager's wake Signals
(distinguishing them from the test's own by sender identity) **failed 15/15 with 0 loop iterations**
and its counters read `{'test': 0, 'manager_suppressed': 1}`. The test's own Signal never reached the
gate and the single suppressed call was the legitimate delivery wake, so the runs measured a broken
harness, not the livelock. A baseline comparison run earlier the same day is void for a related
reason: the reproductions had the probe attached and the baseline could not have it, so probe-vs-no-probe
was confounded with changes-vs-baseline. Like-for-like without the probe, both sides were 0 failures.

### Recommended next work

1. Settle sustain-or-ride with a harness that keeps the test's own Signal working. Verify the gate
   sees it (`test` counter > 0) *before* trusting any run.
2. Capture the failing Workflow's History, as was done for Failure C. The decisive question is why the
   execution is "closing" while never completing -- `handle.result()` times out, so it had not closed.
3. Find what makes it exactly 85. A constant iteration count across independent occurrences suggests a
   cap or a timeout ratio, not an open-ended loop.
4. Only then decide whether the manager should bound wake attempts per Run across replays. That needs
   per-Run state surviving eviction, like the owed-removal ledger; note `_send_owed_wake` retries
   `BUSY_WORKFLOW` three times, and `BUSY_WORKFLOW` is a *transient* server signal, so treating it as
   terminal would risk dropping a wake and losing a record.

## Verified tree and native bridge

The follow-up ran against:

- sdk-python: `7392fbdb694ecab5d7dd102c203a350c651d247d`
- sdk-core: `0085b7d580c05ebfbaf4399f19e889c6bcb4a6e9`
- container: `python-sdk-streaming`, 4 cores

Before rebuilding, Python loaded:

```
.../sdk-python/temporalio/bridge/temporal_sdk_bridge.abi3.so
mtime: 2026-08-22 02:59:38 UTC
```

ADR-041's Core commit, `80c974f8460dfdef1905f5f2b9ace11802012b02`, was committed at
`2026-08-22 16:02:07 UTC`. The binary was therefore more than thirteen hours older and could not
contain ADR-041. This invalidates the original inference that `prepare_complete_resp`'s new
replacement-WFT request caused the observed panic. It also means the original baseline/current Core
comparison was not a comparison of the stated sources.

The bridge was rebuilt from `sdk-python` inside the container with:

```bash
uv run maturin develop --uv
```

Afterward the loaded extension had mtime `2026-08-23 16:59:05 UTC`. Repository source remained clean;
only the ignored/generated native extension changed.

## Failure B — concrete Core double-admission bug

### The bug

All newly polled WFTs for existing Runs enter
`WFStream::_instantiate_or_update`:

```rust
let pwft = if let Some(rh) = self.runs.get_mut(&pwft.work.execution.run_id) {
    if let Some(w) = rh.buffer_wft_if_outstanding_work(pwft) {
        w
    } else {
        return Ok(None);
    }
} else {
    pwft
};
```

`ManagedRun::buffer_wft_if_outstanding_work` says it buffers when there is an outstanding WFT or
activation, but its condition does not check the WFT:

```rust
/// Stores some work if there is any outstanding WFT or activation for the run.
pub(super) fn buffer_wft_if_outstanding_work(
    &mut self,
    work: PermittedWFT,
) -> Option<PermittedWFT> {
    let about_to_issue_evict = self.trying_to_evict.is_some();
    let has_activation = self.activation().is_some();
    if has_activation || about_to_issue_evict || self.more_pending_work() {
        self.task_buffer.buffer(work);
        None
    } else {
        Some(work)
    }
}
```

`more_pending_work()` is not an equivalent check:

```rust
self.wft.is_some() && self.wfm.machines.has_pending_jobs()
```

Therefore an existing WFT with no activation and no pending machine jobs passes the new task through.
It immediately reaches:

```rust
if self.wft.is_some() {
    dbg_panic!("Trying to send a new WFT for a run which already has one!");
}
```

The relevant locations are:

- `sdk-core/crates/sdk-core/src/worker/workflow/workflow_stream.rs:320-330`
- `sdk-core/crates/sdk-core/src/worker/workflow/managed_run.rs:150-156`
- `sdk-core/crates/sdk-core/src/worker/workflow/managed_run.rs:192-198`
- `sdk-core/crates/sdk-core/src/worker/workflow/managed_run.rs:1986-2002`

### The race it permits

The server can accept the current WFT completion and make a replacement WFT available to a poller
before Core processes the local post-completion message that clears `ManagedRun.wft`. The ordering is:

1. The activation is gone and the old WFT is being reported.
2. The server accepts it and makes the next WFT available.
3. A poll result reaches `WFStream` before `process_post_activation` clears the old WFT.
4. The admission gate sees no activation, eviction, or pending jobs and lets the new WFT through.
5. `_incoming_wft` sees the old WFT and panics.

A WFT returned directly in the completion response is already handled safely: `process_post_activation`
calls `complete_wft`, finishes the activation, and only then applies the returned task. The unsafe path
is a separately polled task winning the ordering race against that local post-completion message.

`git blame` dates the method and its claimed invariant to `7473e802b` (2023-01-10), with the current
condition coming from `2da4d2fa0` (2023-11-17). This is not a streaming regression. No focused test of
this admission window was found.

`dbg_panic!` is an error log plus a debug assertion. Debug builds terminate the workflow-processing
thread; release builds log and continue into code that was not designed to accept two WFTs for one
Run. Do not "fix" only the assertion. The admission path needs to preserve the outstanding WFT and
buffer the replacement.

### Reproduction status after rebuilding

- One focused current-bridge execution passed.
- Eight concurrent focused pairs passed: 16/16 executions.
- Both rebuilt full-suite processes passed Failure B.

This does not disprove the race. It establishes only that the original reproduction was against an
older binary and that the current trigger rate is unknown. The static admission defect is sufficient
to justify a Core regression test without waiting for another full-suite panic.

### Recommended next work

1. Add a Core unit test that leaves `ManagedRun.wft` present while clearing the activation and pending
   jobs, then admits another `PermittedWFT`; assert it enters `task_buffer` and does not reach
   `_incoming_wft`.
2. Make the admission condition match its docstring, most likely by explicitly treating
   `self.wft.is_some()` as outstanding work.
3. Exercise the post-completion/poller ordering in a higher-level test if the harness can control the
   two input lanes.
4. Verify both debug and release behavior. A release-only green result is not sufficient because the
   current macro suppresses the panic there.

## Failure A — sandboxed import of a pytest-rewritten module

Failure A reproduced in one process of a rebuilt concurrent full-suite pair:

```
suite A: 654 passed
suite B: 653 passed, 1 failed
```

There was no `already has one`, Rust panic, or earlier Core failure anywhere in the failing process.
The full exception chain was:

```
Worker(... workflows=[NoOpWorkflow])
  -> _WorkflowWorker validates NoOpWorkflow
  -> SandboxedWorkflowRunner imports test_registry.py
  -> pytest-rewritten module imports _pytest.assertion.rewrite
  -> nested sandbox/importlib imports reach _io
  -> CPython _ModuleLock.acquire finally block
  -> del _blocking_on[tid]
  -> KeyError: <thread id>
  -> RuntimeError: Failed validating workflow NoOpWorkflow
```

The important evidence is that the traceback begins while the sandbox re-imports
`tests.contrib.external_workflow_streams.test_registry`, whose code was transformed by pytest's
assertion rewriter. The final `RuntimeError` is only the wrapper in
`temporalio/worker/_workflow.py:254-262`.

This happens before `temporalio/worker/_worker.py:708` creates the bridge Worker, so it cannot be a
poisoned Core Runtime or an error returned by `new_worker`. Failure A and Failure B are independent.

The original "sandbox importer race disproven" result did not exercise this path. Its synthetic
threads imported ordinary modules while Workers were constructed; they did not reproduce a fresh
sandbox import of a pytest assertion-rewritten Workflow module with the suite's accumulated import
state. That result cannot rule out the now-captured failure.

Eight concurrent fresh-process executions of only the registry test all passed. Raw cross-process CPU
load is therefore insufficient. The flake needs prior in-process importer/sandbox state, a rarer timing
window, or both.

### Recommended next work

The registry test is not testing sandbox behavior. First make it stop depending on sandbox import of
its own pytest-rewritten module, for example by defining `NoOpWorkflow` as unsandboxed or moving the
fixture Workflow into a helper module that pytest does not rewrite. Then decide separately whether the
general sandbox/import-lock behavior warrants an SDK regression: the importer's own docstring already
warns that it mutates `sys.modules` and `builtins.__import__` globally and "should be locked against
other code running at the same time."

Do not pursue the proposed A-to-B poisoned-runtime connection further unless a new traceback shows a
different failure. The captured A traceback rules it out for this occurrence.

## Failure C — expected Signal/terminal-command race rejected by the test

### Current reproduction

With the rebuilt current bridge, four focused eight-process batches produced:

```
batch 1: 4/8 failed
batch 2: 4/8 failed
batch 3: 1/8 failed
batch 4: 3/8 failed
total:  12/32 failed
```

All failures were the same test and signature:

```
test_an_empty_stream_parked_and_evicted_replays_from_the_recorded_cursor
EVENT_TYPE_WORKFLOW_TASK_FAILED
cause: WORKFLOW_TASK_FAILED_CAUSE_UNHANDLED_COMMAND
message: "UnhandledCommand"
```

Both rebuilt full-suite processes happened to pass this test, demonstrating why a small number of
full-suite reruns is a poor way to settle it.

### Captured readiness and History

The failing Workflow still returned the correct `['alpha', 'beta']`. Its execution-start trace was:

```python
[False, True, True]
```

The first replay is the intended cache eviction; the third body start is the recovery from the WFT
failure. The manager's readiness trace was identical in captured failures:

```python
[
    (<run>, 1, 1, "NoOpenWorkflowTask"),
    (<run>, 1, 1, "NoOpenWorkflowTask"),
    (<run>, 1, 1, "NoOpenWorkflowTask"),
]
```

Each `NoOpenWorkflowTask` result enters `_manager.py:1729-1747`, counts one owed wake, and sends a
Signal. The complete failed History contained four distinct generation-0 wake Signals with distinct
request IDs: the test's initial wake plus three manager follow-ups.

The decisive event order was:

```
6   WORKFLOW_EXECUTION_SIGNALED (__temporal_external_stream_wake)
7   WORKFLOW_TASK_SCHEDULED
8   WORKFLOW_TASK_STARTED
9   WORKFLOW_TASK_COMPLETED
10  WORKFLOW_EXECUTION_SIGNALED (__temporal_external_stream_wake)
11  WORKFLOW_TASK_SCHEDULED
12  WORKFLOW_TASK_STARTED
13  WORKFLOW_TASK_FAILED (UNHANDLED_COMMAND)
14  WORKFLOW_EXECUTION_SIGNALED (__temporal_external_stream_wake)
15  WORKFLOW_TASK_SCHEDULED
16  WORKFLOW_TASK_STARTED
17  WORKFLOW_TASK_COMPLETED
18  WORKFLOW_EXECUTION_SIGNALED (__temporal_external_stream_wake)
19  WORKFLOW_TASK_SCHEDULED
20  WORKFLOW_TASK_STARTED
21  WORKFLOW_TASK_COMPLETED
22  MARKER_RECORDED
23  WORKFLOW_EXECUTION_COMPLETED
```

Signal 14 arrived while WFT 11/12 was trying to complete the Workflow. Temporal rejects the terminal
command so the Signal is not lost, records `UnhandledCommand`, and schedules a replay. Core treats
this response as an eviction-and-repoll condition in `worker/workflow/mod.rs:878-892`. Its own
integration tests explicitly state that `UnhandledCommand` is acceptable when an external event races
a command (`tests/integ_tests/workflow_tests/child_workflows.rs:1235-1247`).

The assertion at `test_replay_end_to_end.py:874-879` rejects every WFT failure because it says a WFT
failure would make the second execution ambiguous evidence of eviction. The captured sequence is not
ambiguous:

- the test already waits for the manager's cache-eviction callback to finish before publishing;
- `STARTS[1]` is the replay caused by that eviction;
- the `UnhandledCommand` occurs later and creates `STARTS[2]`;
- the Workflow completes with the expected records and markers.

This is therefore principally a test expectation bug, not evidence that replay or cursor recovery is
wrong.

### Recommended next work

1. Preserve the direct cache-eviction assertion and the `starts[0:2] == [False, True]` assertion.
2. Permit `WorkflowTaskFailedCause.UNHANDLED_COMMAND` after the intended eviction while continuing to
   reject nondeterminism and other WFT failure causes.
3. Keep validating the final records, marker boundary, recorded ranges, and offline replays; those are
   the correctness claims the test exists to prove.
4. Consider a separate efficiency/liveness test for repeated `NoOpenWorkflowTask` follow-up wakes.
   The captured run settled after three follow-ups, but repeated replay registration can re-read and
   reannounce the same buffered records. If timing continually places each Signal inside a terminal
   WFT, this can cause repeated safe retries. That is not the correctness failure asserted here, but it
   is worth bounding explicitly.

## Follow-up commands and retained container output

The rebuilt full-suite pair was run directly inside the container with `RUST_BACKTRACE=1` and
`--tb=long`. Its outputs are:

```
/tmp/wft-full-current/a.txt
/tmp/wft-full-current/b.txt
```

Focused Failure B output is under:

```
/tmp/wft-focused-current/
```

Focused Failure C and diagnostic histories are under:

```
/tmp/wft-empty-replay-current/
/tmp/wft-empty-replay-locals/
/tmp/wft-empty-replay-history2/
/tmp/wft-empty-replay-history3/
```

The temporary pytest diagnostic plugin used to print complete History events was removed. Both
sdk-python and sdk-core source trees were clean at handoff.

---

## Original investigation (archival; conclusions above supersede it)

The text below is retained to show what was initially observed and which hypotheses led to the
follow-up. Where it conflicts with the follow-up conclusions above, the follow-up is authoritative.

---

## 1. What is actually known

There are **three distinct intermittent failures**. B is reproduced and diagnosed, C is reproduced
and confirmed pre-existing, A is neither. They may or may not share an underlying defect; §2 gives the
one mechanism that would connect A to B, and it is unproven.

### Failure A — the one originally reported, NOT reproduced

| | |
|---|---|
| Test | `tests/contrib/external_workflow_streams/test_registry.py::test_a_conforming_backend_registers_on_a_worker` |
| Failing line | `test_registry.py:115`, the `Worker(...)` constructor |
| Error class | `RuntimeError` |
| Message | **unknown** — the run used `-q` with pytest-pretty's compact reporter, which printed only a truncated summary cell and captured no traceback or stdout/stderr |
| Occurrences | 1 |
| Reproduced | **No.** 11 subsequent full-suite runs did not show it |

The test body is three lines: construct a `Worker` with `external_stream_backends`, assert the dict
was stored. It touches none of the code changed for finding 14.

### Failure B — reproduced, with a Core panic

| | |
|---|---|
| Test | `tests/contrib/external_workflow_streams/test_serialization_context.py::test_a_stream_records_conversion_runs_with_the_consuming_workflows_context` |
| Failing line | `test_serialization_context.py:408` |
| Assertion | `assert [] == [WorkflowSerializationContext(namespace='default', workflow_id='wf-…')]` |
| Real cause | a **Rust panic inside Core** kills workflow processing, so the record is never delivered and never converted; the assertion is the symptom |
| Occurrences | 2 (both processes of one concurrent pair, same test) |
| Reproduced | **Yes** — see the recipe in §3 |

The panic, verbatim:

```
ERROR temporalio_sdk_core::worker::workflow::managed_run: Trying to send a new WFT for a run which already has one!

thread 'workflow-processing' panicked at sdk-core/crates/sdk-core/src/worker/workflow/managed_run.rs:197:13:
Trying to send a new WFT for a run which already has one!

thread 'tokio-rt-worker' panicked at sdk-core/crates/sdk-core/src/worker/mod.rs:1192:18:
Workflow processing terminates cleanly: Error joining workflow processing thread: Err(Any { .. })
```

The second panic is a consequence of the first: `mod.rs:1192` is
`.expect("Workflow processing terminates cleanly")` in the shutdown path, which cannot succeed once
the workflow-processing thread is dead.

### Failure C — a third test failed, and this one is confirmed pre-existing

`test_replay_end_to_end.py:874` —
`test_an_empty_stream_parked_and_evicted_replays_from_the_recorded_cursor` failed with
`EVENT_TYPE_WORKFLOW_TASK_FAILED` / `message: "UnhandledCommand"`. This is the test whose stall was
root-caused and fixed on 2026-08-22 (`empty-stream-replay-flake-handoff.md`,
ADR-041). **The failure signature here is different from the one that document describes** — that one
was a silent stall from a readiness reported against a task Core was about to report; this one is a
server-side `UnhandledCommand`. Treat it as a possibly-new manifestation, not as the fixed defect
recurring. It occurred in the same process as Failure B but in an earlier test, and pytest attached
no panic output to it.

**This one is settled: it reproduces on the baseline.** With the finding-14 changes stashed, six runs
(three concurrent pairs) produced this same failure once — same test, same
`test_replay_end_to_end.py:874`, same `UnhandledCommand`. It is independent of finding 14 and of the
Core panic (that baseline run showed no panic at all).

---

## 2. Where the panic comes from

`crates/sdk-core/src/worker/workflow/managed_run.rs`, in `_incoming_wft`:

```rust
fn _incoming_wft(&mut self, pwft: PermittedWFT) -> Result<Option<ActivationOrAuto>, RunUpdateErr> {
    if self.wft.is_some() {
        dbg_panic!("Trying to send a new WFT for a run which already has one!");
    }
```

`incoming_wft` is called when Core takes a **new Workflow Task from the server** for a run that
already has an outstanding one. So the question to answer is: *what makes the server dispatch, or
Core accept, a second concurrent WFT for one run?*

**`dbg_panic!` is `error!` + `debug_assert!(false, …)`** (`crates/common/src/lib.rs:27`). The native
extension in this container is a **debug build** (`temporalio/bridge/target/debug/`), so the assert is
live and the process panics. In a release build this is an ERROR log and execution continues — which
means:

- this invariant violation may have been happening silently in release-mode runs all along;
- and conversely, a CI or user environment on a release build would see corruption or a stall rather
  than this panic.

**Consequence worth chasing:** the Core `Runtime` is a process-global singleton shared by every
Worker in a pytest session. Once workflow processing has panicked, every later Worker in that process
is operating against a poisoned runtime. `Worker.__init__` eagerly creates a Core worker
(`temporalio/worker/_worker.py:708` → `new_worker`, `temporalio/bridge/src/worker.rs:490`), and that
path returns `anyhow` errors that pyo3 surfaces as **`RuntimeError`**
(`.context("Failed creating worker")`).

That is a **credible mechanism linking Failure A to Failure B**, and it is the first thing to test.
It is *not* established. The obstacle: collection is alphabetical, so `test_registry.py` runs
**before** `test_serialization_context.py`. For the link to hold, a panic would have to have occurred
in one of the 16 test files preceding `test_registry.py`. The original run's output does not say
whether one did, because it captured nothing.

---

## 3. How to reproduce Failure B

What reproduced it was **two full suites running concurrently**, twice through the loop. Each pytest
session starts its **own** dev server (`--workflow-environment` defaults to `local`, see
`tests/conftest.py:55`), so a concurrent pair means two Temporal servers and two Core runtimes on
4 cores.

```bash
cd .../sdk-python
S=/tmp/scratch   # anywhere writable
for i in 1 2 3 4; do
  timeout 1800 uv run --frozen pytest tests/contrib/external_workflow_streams/ -q --tb=long -rf > $S/hunt-${i}a.txt 2>&1 &
  A=$!
  timeout 1800 uv run --frozen pytest tests/contrib/external_workflow_streams/ -q --tb=long -rf > $S/hunt-${i}b.txt 2>&1 &
  B=$!
  wait $A; wait $B
  echo "iteration $i"
  grep -l 'already has one' $S/hunt-${i}a.txt $S/hunt-${i}b.txt && break
done
```

**`--tb=long` is not optional.** With plain `-q`, pytest-pretty prints a truncated table and no
captured output — which is exactly why Failure A is still undiagnosed after 12 runs. Always grep the
output for `already has one` and `panicked at`, not just for `failed`; a release-build run would show
the ERROR line with no failure at all.

### Observed rates

| Condition | Runs | Result |
|---|---|---|
| Single suite, idle machine | 4 | 654 passed every time |
| Single suite, 4 busy-loop processes saturating the CPU | 1 | 654 passed |
| Two concurrent suites, finding-14 changes applied | 4 pairs | 1 pair panicked in **both** processes; 1 pair produced Failure A; 2 pairs clean |
| Two concurrent suites, **baseline** (changes stashed) | 3 pairs | **0 panics**; 1 pair produced Failure C |

Both panics in the reproducing pair landed 43 ms apart (`22:25:23.453421Z` and `22:25:23.496722Z`) in
two *separate processes with separate servers*. That is not shared state — the two suites start
together and run the same order at the same speed, so they reach the same heavy test simultaneously.
The trigger is load at that specific test, not cross-process interference.

---

## 4. Hypotheses already disproven

Both of these looked convincing and were wrong. Do not spend time on them again.

**Sandbox importer race — disproven.** The theory was that `Worker.__init__` → `prepare_workflow`
runs the sandbox importer, which mutates `sys.modules` and `builtins.__import__` process-wide (its
own docstring warns it "should be locked against other code running at the same time"), so a
concurrent import would break it and surface as `RuntimeError("Failed validating workflow …")`. A
script driving that collision directly — 6 threads importing while the main thread built Workers in a
loop — **never failed a single Worker construction in 300 attempts**. The `KeyError` the script did
produce came from its own threads calling `sys.modules.pop()` on the *same* keys concurrently, i.e. a
bug in the script, not in the SDK.

**Accumulating never-shut-down Core workers — disproven.** Five constructor-only tests build a real
Core worker and never shut it down: `test_registry.py:115` and `:144`,
`test_continuation.py:634`, `:672`, `:697`. Theory: the accumulation exhausts something and
`new_worker` starts failing. **400 such workers were constructed and held live in one process with no
failure.** The leak is real test hygiene worth fixing on its own merits, but it is not this.

---

## 5. Next steps, in order

1. **Extend the baseline comparison — it is not yet conclusive.** Three baseline pairs produced
   0 panics; four pairs with the finding-14 changes produced 1 panicking pair. **1-of-4 versus 0-of-3
   is too small to implicate or exonerate anything**, and it would be a mistake to read the clean
   baseline as proof either way. Run considerably more pairs on both sides before drawing a
   conclusion from run counts.

   There is, however, a much sharper argument available than counting runs, and it should be checked
   first because it is decidable by reading: **the finding-14 code paths are unreachable on the
   failing test's path.** Every new behaviour is gated on a non-empty owed-removal ledger —
   `_wake_after_cleanup` fires only from `_drain_owed_removals`, which requires a ledger entry;
   `wake_park_generation` returns the old `current_park_generation` read unless the key is in the
   ledger; `_flush_cleanup_wakes` returns immediately on an empty set; `_note_swept_handoff` returns
   immediately when the Run owes nothing. A ledger entry requires a **failed** park-intent removal,
   and `test_a_stream_records_conversion_runs_with_the_consuming_workflows_context` runs against an
   unmodified `MemoryStreamBackend` that fails nothing. Confirm that reading (assert the ledger is
   empty throughout that test) and the changes are ruled out on this path by construction rather than
   by sample size.
2. **Establish or kill the Failure A ↔ Failure B link.** Re-run with `--tb=long` and grep the *whole*
   output for `already has one`, then check whether any occurrence precedes `test_registry.py` in
   collection order. If yes, the poisoned-runtime mechanism in §2 is confirmed and Failure A needs no
   separate fix. If Failure A recurs with no preceding panic, it is a separate defect and the
   traceback is finally in hand.
3. **Find the double dispatch.** With a reproduction in hand, set `RUST_BACKTRACE=1` and capture the
   backtrace at `managed_run.rs:197`. The question is which of the two WFT sources raced: a server
   dispatch (sticky vs. normal queue), or something on the external-stream side that asks for a
   replacement task — `prepare_complete_resp` was changed by the ADR-041 fix specifically to *request
   a replacement Workflow Task* when a completion is reported with readiness still pending, which is
   a new way for a run to acquire a second task and is the first place to look.
4. **Decide what a release build should do here.** If this invariant can be violated legitimately
   under load, `dbg_panic!` is the wrong tool and the branch needs a real recovery path, because on a
   release build it currently logs and carries on with corrupt state.

---

## What was in the tree

sdk-python at `e73ee2cc`, Core submodule at `34f0a190`, plus uncommitted finding-14 changes:
`_manager.py`, `worker/_workflow.py`, and three test files. Backed up as
`finding-14.patch` / `finding-14-docs.patch` in the scratchpad.

Finding 14's own verification: 654 tests passed on four separate clean full-suite runs with those
changes in place, and each of its four regressions was confirmed to fail without its own fix. The
changes do alter when unparked wakes are sent — a wake naming an intent the manager has decided to
remove is now composed as generation 0, which Core *accepts* where it previously *discarded* it — so
they are not a priori irrelevant to a WFT-dispatch panic and step 1 above exists to settle it. Note
though that the failing test in Failure B parks nothing and owes no removal, so on that path
`wake_park_generation` reads the backend, finds no intent, and returns 0 exactly as the old code did.
