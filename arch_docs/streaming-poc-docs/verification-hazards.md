# Verification hazards

Two ways a test result in this repository can be confidently wrong. Both were hit during
implementation, both produced a written defect report against code that turned out to be correct,
and both are cheap to check for once you know they exist.

They share a shape worth naming: **the apparatus was broken, not the code**. A failing test is
evidence about the system *plus* the harness, and when the harness is wrong the failure still looks
exactly like a real bug — with a plausible mechanism, a reproducible symptom, and a stack trace
pointing at real code.

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

## Before reporting a defect found by a test

1. Confirm the binary under test is the one you built (hazard 1).
2. Confirm the test's own setup succeeded — an exception in a producer or fixture can present as a
   hang in the system under test (hazard 2).
3. Reproduce at the smallest layer that shows it. If a Core-level test cannot reproduce what an
   end-to-end test shows, suspect the apparatus before concluding the layers disagree.
4. Mutation-test the claim: break the code path you believe is at fault and confirm *that* test
   fails. If nothing fails, the test was not covering what you thought — and if the test already
   passes without the mechanism, the defect is not where you think it is.
