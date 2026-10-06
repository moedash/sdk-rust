# Verification hazards

Seven ways a test result in this repository can be confidently wrong. Each is a current constraint
on trustworthy validation and is cheap to check once it is named.

A result is evidence about the system *plus* its harness. Broken apparatus can produce a plausible,
repeatable failure against correct code; a gate that never armed its case can produce a clean pass
while checking nothing.

## 1. A stale native extension

**Symptom.** Rust tests pass. Python end-to-end tests fail with a nondeterminism error naming a
mechanism that was just implemented — as though Core were missing the feature entirely. Which it
was: the process had loaded a Core built before it.

**Why it happens.** `pyproject.toml` sets

```toml
[tool.maturin]
manifest-path = "temporalio/bridge/Cargo.toml"
module-name = "temporalio.bridge.temporal_sdk_bridge"
```

so the extension must land at `temporalio/bridge/temporal_sdk_bridge.abi3.so`, which is what
`import temporalio.bridge.temporal_sdk_bridge` resolves. Running `maturin develop` **from
`temporalio/bridge/`** instead of the repo root uses that crate's own packaging and installs a
top-level `temporal_sdk_bridge` package into the virtualenv. The command succeeds. It prints
`Installed`. Nothing imports the result.

**Why it is insidious.** A stale extension does not error — it is quietly correct for the *old*
code. Every test that does not exercise the new Core keeps passing, so the suite stays green and
only the newest tests disagree. The natural reading is that the new tests found a bug.

**The check.** Ask the process what it actually loaded, rather than trusting the build:

```python
import temporalio.bridge.temporal_sdk_bridge as b
import os, time
print(b.__file__, time.ctime(os.path.getmtime(b.__file__)))
```

If the timestamp predates the Rust change, nothing since then has been tested.

**The fix.** Build from the repository root: `uv run maturin develop --uv`.

**When to check.** Before believing any Python-side result about Core behaviour, and always before
filing a Core defect from a Python test.

## 2. A test helper that restarts a producer sequence

**Symptom.** A Workflow appears to hang, the server accumulates tens of thousands of history
events, and the client's own event loop looks starved. It reads like a livelock between retention
and quiescence.

**Why it happens.** `(session_id, sequence)` is the append idempotency key, and it is idempotent on
*identity*, not on the key alone. A helper like

```python
async def publish(backend, key, values):
    for i, value in enumerate(values):          # restarts at 0 every call
        await backend.append(key, StreamRecord(DATA, encode(value), "producer", i))
```

re-uses `producer/0` on its second call with different content. The backend rejects it —
correctly; that rejection is a required conformance case. The record is never appended, the
Workflow waits for data that will never arrive, and the exception surfaces somewhere unrelated to
the Workflow that is now stuck.

**Why it is insidious.** The visible symptom is on the Workflow side and looks like a coordination
bug, while the cause is an exception in the test's own producer several lines earlier. Two separate
test modules grew the same helper independently, and both were wrong the same way.

**The fix.** Carry the sequence across calls, per producer session:

```python
_sequences: dict[StreamKey, int] = {}

async def publish(backend, key, values):
    start = _sequences.get(key, 0)
    for i, value in enumerate(values, start=start):
        await backend.append(key, StreamRecord(DATA, encode(value), "producer", i))
    _sequences[key] = start + len(values)
```

**Related contract.** See `spec/backend-contract.md` on idempotent append, and ADR-020 on why
identity rather than the key alone.

## 3. A required-test gate reading a stale copy of its own list

**Symptom.** `test_m1_gate.py` passes, and reports the milestone fully covered, immediately after a
case was added to a required-test list and mapped to a new test. It would report the same thing if
the case had never been added.

**Why it happens.** `m1_gate.py` resolves `PLAN_DIR` into the **vendored** Core checkout under
`temporalio/bridge/sdk-core`, not into the working tree these documents live in. That is deliberate —
reading the list from a copy inside the Python repo would let the two drift silently — but it means
the list the gate parses is whatever commit the submodule pointer names. Edit `required-tests/` in
the Core working tree and the gate does not see the edit at all.

**Why it is insidious.** The failure the gate exists to produce is an *unmapped* case. The reverse,
a mapping for a case the parsed list does not contain, is caught by nothing: the count check compares
the parsed list against that same list's own heading, so both sides move together, and the coverage
check only walks cases the list contains. So a case mapped ahead of the pointer is not a pending
gate that will fail until the pointer moves — it is invisible, and the gate stays green without ever
arming the case. Mapping ahead is the normal order of work here, since the two repos commit
separately, which is exactly why the green result cannot be read as coverage.

**The check.** Ask the gate what it counted, rather than whether it passed:

```python
from tests.contrib.external_workflow_streams.m1_gate import PLAN_DIR, declared_count
print(PLAN_DIR, declared_count("tests-m1.md"))
```

If the count predates the case you just added, that case is not armed. Confirm the same way for the
Core-side lists before trusting a milestone figure.

**The fix.** Move the submodule pointer to the Core commit that carries the list, then re-run the
gate and check the coverage figures changed. Checking the new lists out into the vendored tree first
is a cheap way to see what the pointer will arm before committing to it.

**When to check.** Whenever a required-test list and its mapping are edited — which is every fix
that adds a case.

## 4. A pytest-rewritten module imported into the Workflow sandbox

**Symptom.** Worker construction fails intermittently during Workflow validation with a `KeyError`
inside CPython module-lock bookkeeping. It is more likely in a concurrent or complete suite than in
the focused test and occurs before the Core worker exists.

**Why it happens.** A Workflow defined in a test module is re-imported by the sandbox. If pytest's
assertion rewriter transformed that module, the sandbox import also pulls `_pytest` implementation
modules and their process-global import machinery into a path whose isolation assumptions they do
not satisfy. The stack points at real sandbox and import-lock code, but the product behavior the test
intended to exercise has not started.

**The fix.** A test module that defines sandboxed Workflows and constructs a `Worker` or `Replayer`
declares `PYTEST_DONT_REWRITE` in its module docstring, or moves the Workflow into a helper pytest
does not rewrite. A Workflow may instead be unsandboxed only when the test is not about sandbox
behavior. Do not diagnose a general sandbox defect from the pytest-specific path.

## 5. Rejecting every Workflow Task failure in a terminal-race test

**Symptom.** The Workflow returns the expected stream observations, yet a History assertion fails
because one Workflow Task was rejected with `UnhandledCommand`; replay may also add another
workflow-body execution.

**Why it happens.** A wake Signal can race the terminal command. Temporal rejects the task so it can
preserve the external event, then replays. This is the lifecycle described in
`spec/wft-lifecycle.md`, not evidence that stream replay failed.

**The fix.** Tests that deliberately create this race permit `UnhandledCommand` specifically while
rejecting every other cause. They compare every execution's observations with the live result and do
not require an exact body-start count. Tests that do not create the race should retain stricter
assertions.

## 6. A controlled experiment that never observes its control stimulus

**Symptom.** A fault-injection or wake-suppression variant changes the failure rate dramatically, but
its counters show that the event it was meant to isolate never reached the gate. The apparatus may
have intercepted the legitimate delivery wake or changed scheduling before the target condition.

**The check.** Assert that the original stimulus reached the server or the exact production boundary
before accepting any experimental run. Count original inputs separately from follow-ups. Apply the
same tracing to control and treatment; instrumentation on only one side makes scheduling part of the
comparison. On timeout, retain History and correlated runtime state before cleanup, and use long
tracebacks so the first failure is not reduced to a one-line table.

**The rule.** A treatment that did not observe its control stimulus is void, no matter how stable its
result looks.

## 7. Worker tasks that outlive their test

**Symptom.** A later test reports pending-task destruction, shutdown warnings, or scheduling-sensitive
failures whose originating Worker was constructed by an earlier case.

**Why it happens.** Constructing a `Worker` creates a real Core worker and integration fixtures often
start worker and shutdown coroutines separately. Dropping the Python objects does not make those
lifetimes orderly.

**The fix.** Successful constructor tests use `async with Worker(...)`. Fixtures retain every worker
and shutdown task and await them during teardown. Constructor-failure tests remain direct because no
Worker exists to close when validation raises.

## Before reporting a defect found by a test

1. Confirm the binary under test is the one you built (hazard 1).
2. Confirm the test's own setup succeeded — an exception in a producer or fixture can present as a
   hang in the system under test (hazard 2).
3. Confirm required-test lists and their mappings come from the same submodule revision (hazard 3).
4. Keep pytest rewriting and leaked Worker lifetimes out of Workflow tests (hazards 4 and 7).
5. Classify `UnhandledCommand` against the event race the test created (hazard 5).
6. Prove a controlled experiment observed its control stimulus (hazard 6).
7. Reproduce at the smallest layer that shows it. If a Core-level test cannot reproduce what an
   end-to-end test shows, suspect the apparatus before concluding the layers disagree.
8. Mutation-test the claim: break the code path you believe is at fault and confirm *that* test
   fails. If nothing fails, the test was not covering what you thought — and if the test already
   passes without the mechanism, the defect is not where you think it is.

The last step also covers hazard 3, and it is worth stating the general form: **a green
result is a claim about what ran, and nothing checks that for you.** Before reading a pass as
coverage, make the covering mechanism fail once.
