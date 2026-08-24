# Remaining issues plan

This plan closes the two open issues in `outstanding-issues.md` and gives the deferred issue a
bounded disposition:

- **#2:** replay-after-eviction runs can loop on rejected Workflow Task reports and time out;
- **#7:** replayed readiness can produce repeated follow-up wakes for the same buffered range;
- **#6:** a local Core invariant uses `dbg_panic!`, whose release-build behavior logs and continues.

Issues 2 and 7 are one investigation until evidence separates them. Issue 6 is a separate Core
hardening decision; it must not turn this work into a global redesign of `dbg_panic!`.

## Result

Completed 2026-08-24.

- **#2 and #7:** the correlated trace proved follow-up wakes sustained the closing-window replay
  loop. Python now suppresses terminal re-arms and coalesces nonterminal readiness behind one
  Run-wide, per-send-attempt wake cycle. Deterministic retry/eviction/cleanup regressions pass, as do
  10 canary and 100 acceptance repetitions of each formerly failing live test.
- **#6:** all production admission paths are guarded. `_incoming_wft` now also buffers the second
  task in release builds after logging the invariant, preserving the debug panic and the original
  task token. Debug- and release-profile fault-injection tests pass.
- The timeout artifact collector remains in the two replay integrations. It writes complete History,
  Workflow Task timing, subscription state, and correlated wake events before cleanup on recurrence.
- **Release gate:** the two formerly failing live tests each passed 10 canary and 100 acceptance
  repetitions; the rebuilt native extension then passed three consecutive complete
  external-stream suites at **658 passed** each. Core passed all 22 debug `managed_run` tests, the
  release-only recovery test, all 496 Core library tests (one ignored), compile-fail suites, and
  documentation tests. Rust formatting/lint and Python Ruff, Pyright, Mypy, BasedPyright, and
  pydocstyle checks all pass.

## Completion criteria

The work is complete only when all of the following are true:

1. A failing #2 run retains enough evidence to explain why the server says the execution is
   closing while the result never becomes terminal.
2. A controlled experiment establishes whether #7's wakes sustain that loop or merely accompany
   it. The experiment must prove that the test's original wake reached the server before changing
   follow-up behavior.
3. The fix is made in the layer that owns the failure—Python manager, Core, or server—and a
   deterministic regression fails without it. A blind retry cap or dropping `BUSY_WORKFLOW` is not
   an acceptable fix because either can strand a record.
4. Repeated announcements for one buffered range have an explicit liveness and deduplication rule,
   with tests distinguishing them from genuinely new readiness.
5. Issue 6 ends in either a local recovery implementation with tests or a documented decision to
   retain the assertion because the post-fix path is proven unreachable. No global macro change is
   in scope.
6. The targeted stress runs, the complete external-stream suite, relevant Core tests, formatting,
   and the native-freshness gate all pass on the same source revision.

## Phase 0 — establish a trustworthy baseline

Before changing behavior:

1. Record the Python, Core, vendored-submodule, native-extension, and Temporal dev-server versions.
   Rebuild the bridge if any vendored Rust source is newer than the extension.
2. Run each #2 test alone, then together, with long tracebacks. Record the run count and ordering so
   later comparisons use the identical harness.
3. Add timeout diagnostics to the two tests, without changing runtime behavior. Before cleanup they
   must retain:

   - complete Workflow History and execution status;
   - Workflow Task scheduled/started/completed/failed event IDs and failure causes;
   - Core report attempts and eviction reasons, ordered by timestamp;
   - workflow-body replay starts;
   - readiness answers and the manager's subscription state;
   - every wake's request ID, wait ID, park generation, sender identity, and wake counter;
   - whether the workflow becomes terminal if observation continues beyond the current 30-second
     client timeout.

4. Give every trace one workflow/run correlation ID and emit one machine-readable summary. Avoid a
   probe that changes scheduling in only one side of a comparison.

Exit: a passing run produces a coherent trace, and a timeout preserves its History before the test
terminates the workflow.

## Phase 1 — reproduce and explain issue 2

Use the diagnostic harness in bounded batches:

1. Run each affected test independently and alternate their order. Then run the pair concurrently.
2. Continue until either three independent failures are captured or 200 attempts per test complete
   without failure. The latter does not close the issue; it leaves the diagnostic harness in place
   and records that the incident was not reproducible on this environment revision.
3. For every failure, reconstruct one timeline from the final successful Workflow Task through the
   first `BUSY_WORKFLOW` rejection, all evictions/replays, and cleanup.
4. Explain the repeated count of 85. Check configured RPC retry policy, per-attempt delay, Workflow
   Task timeout, the test's 30-second timeout, and any server retry/deadline limit against the trace;
   do not infer a cap from the count alone.
5. Answer these ownership questions from evidence:

   - Does History already contain a terminal command/event when the first report is rejected?
   - Is Core retrying one report, or receiving/replaying distinct Workflow Tasks?
   - Does the server remain in closing state after all client wake traffic stops?
   - Does the workflow eventually close when the client waits longer?
   - Is the same task token or a sequence of new tokens involved?

Exit: the loop has a demonstrated trigger and an owning layer, or the bounded non-reproduction and
new permanent diagnostics are documented.

## Phase 2 — settle issue 7's relationship to issue 2

Build one test-only gate around wake delivery. It must count the test's original wake separately
from manager follow-ups and assert that the original reached the server before an experiment is
accepted.

Run these like-for-like variants:

1. **Baseline:** all wakes enabled.
2. **Follow-up isolation:** preserve the original wake and first required delivery wake, then hold
   only replay-generated follow-ups while retaining their state for later release.
3. **Ordered release:** release held follow-ups after the Workflow Task report leaves the closing
   window.
4. **No-record control:** force the same eviction/replay ordering without buffered stream data, so
   no manager wake is owed.

Compare completion, Workflow Task rejection count, replay count, and unique wake request IDs. The
result classifies #7:

- **Sustains #2:** holding only follow-ups breaks or materially shortens the loop.
- **Rides #2:** the report/replay loop continues with follow-ups held.
- **Independent:** repeated wakes remain after #2 is fixed but cause bounded excess work.

Regardless of classification, define the required invariant before changing code:

> When records are buffered and no Workflow Task is open, at least one server-visible wake remains
> capable of creating a future task. Replaying the same readiness epoch must not manufacture
> unbounded distinct wakes, while a new append or readiness generation must still be able to wake
> the run.

The logical readiness identity must be derived from durable/replay-stable facts. Candidate inputs
include workflow/run/stream identity, wait ID, readiness generation, and the buffered cursor range;
the wake counter alone is not an identity because replay creates a new manager.

Exit: sustain-versus-ride is measured, and the liveness identity is written before implementation.

## Phase 3 — implement the owning fix

Choose the branch supported by phases 1 and 2:

### Python manager ownership

Coalesce wakes by the defined readiness identity. Preserve one owed/in-flight wake across retries,
clear it only on a state transition that proves another wake is unnecessary, and allow new data or
a new readiness generation to create a new wake. Test eviction/replay, failed sends, worker
shutdown, and inherited park-intent reconciliation.

### Core ownership

Correct Workflow Task report/eviction ordering or readiness admission at the violated state
transition. The regression must control the involved input lanes directly; do not use sleeps to
win a race. Verify task-token ownership and that a rejected report cannot recursively recreate the
same invalid state.

### Server ownership

Reduce the trace to the smallest SDK-independent reproduction and file it upstream with History and
server logs. Add only an SDK-side mitigation whose liveness is proven; otherwise keep the issue open
and pin the affected server range in the status document.

For every branch, retain `BUSY_WORKFLOW` as transient. Never convert it into silent wake loss.

Exit: the deterministic regression fails on the baseline, passes with the fix, and no test-only
instrumentation is required for correctness.

## Phase 4 — dispose of issue 6 narrowly

Audit only the `_incoming_wft` double-admission invariant that motivated this issue:

1. Enumerate every call path and verify that all production paths pass through
   `buffer_wft_if_outstanding_work` or an equivalent guard.
2. Add a fault-injection test that bypasses the guard and observes debug and release behavior.
3. Decide among three local outcomes:

   - **Keep and close:** all production paths are guarded, the stateful regression mutation-kills
     guard removal, and continuing after an impossible internal violation is existing Core policy.
   - **Defensive buffer:** the callee can safely retain the second task without obscuring ownership
     or losing the newer task.
   - **Typed failure and eviction:** buffering is unsafe, but the run can preserve the task token,
     fail/evict deterministically, and keep the workflow-processing thread alive.

4. Record the decision beside the invariant. If recovery is implemented, add both debug and
   release-profile coverage for that site.

Do not alter `dbg_panic!` globally; its other call sites have different recovery semantics and need
a separate project-wide policy.

Exit: issue 6 is closed with a local test-backed decision, not left as an unbounded design question.

## Phase 5 — regression and release validation

Run in this order so a stale native extension cannot invalidate Python results:

1. Nightly Rust formatting and the relevant Core unit/integration modules.
2. Full `temporalio-sdk-core` package tests.
3. Rebuild the Python native bridge and rerun the freshness check.
4. Focused #2/#7 deterministic regressions.
5. At least 100 concurrent repetitions of each formerly failing #2 test, followed by three
   consecutive full external-stream-suite passes with long tracebacks.
6. Python format, import, type, and documentation checks required by the repository.
7. Update `outstanding-issues.md`, `TASK_STATUS.md`, and any wake-protocol ADR affected by a changed
   readiness identity.

If a statistical run fails, retain its artifact bundle and return to the owning phase; do not rerun
until green and discard the first failure.

## Planned deliverables

- timeout artifact collector and correlated trace summary;
- root-cause record for #2 and sustain-versus-ride result for #7;
- deterministic regression(s) and the owning runtime/Core fix, or an SDK-independent server
  reproduction;
- explicit wake liveness/deduplication invariant and ADR update if protocol identity changes;
- local, test-backed disposition for #6;
- final validation record and updated issue/status documents.
