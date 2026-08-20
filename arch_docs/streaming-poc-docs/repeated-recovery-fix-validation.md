# Validating the repeated-recovery fix

**This is a review artifact, not part of the design.** It records how one uncommitted fix was checked
and what the check found. The design statements it confirms live in `spec/wake-signal.md` and
`decisions/ADR-038-an-append-with-no-answer-is-an-unknown-outcome.md`, which are the authority on what
the code now does; the defect and the fix are recorded in `fourth-review.md` as case 79.

Date of validation: 2026-08-20 · Baseline: `sdk-python` `88e3af7b` with the case-79 change uncommitted

Subject: the change to `ExternalStreamProducerTopic._remember` in
`temporalio/contrib/external_workflow_streams/_producer.py`, its `AppendNotAcknowledgedError.cancelled`
docstring, and `test_wake.py::test_a_reinterrupted_append_recovery_preserves_its_latest_wake`.

**Verdict: the fix is correct.** The defect it claims to close is real and was reproduced; the fix
closes it; nothing else in the producer depends on the behaviour it changed. Two residual items are
recorded at the end, neither of which is a defect.

## The defect, reproduced

`_append` built a fresh `_UnresolvedAppend` and raised from **its own local object**, while `_remember`
returned early whenever a matching record was already stored. So the raised error and the producer's
retained state described two different operations, and the next defaulted recovery read the retained
one.

Mutation-testing the claim — step 4 of "before reporting a defect found by a test" in
`verification-hazards.md` — means putting the pre-fix body back and re-running the scenario. Rather
than reverting the file, `_remember` was monkeypatched to its pre-fix body: store the first entry,
never replace it, and return the caller's own `pending`, which is exactly what the old caller raised
from. Case 79's scenario against that patch:

| Error raised | `wake` | `lease` | `cancelled` |
|---|---|---|---|
| First `publish(wake=False)`, committed and lost its response | `False` | 30s | `False` |
| `resolve_append(wake=True, lease=97s)`, committed then cancelled | `True` | 97s | `True` |
| Next defaulted `resolve_append()`, response also lost | `False` | 30s | `False` |
| The intervening refused `publish()` | `False` | 30s | `False` |

Settling from that last error produced `SIGNALS=0` against `RECORDS=[(0, DATA)]`: a durable record on a
stream with a parked Workflow that was never told about it. Both halves of the write-up in
`fourth-review.md` are therefore accurate — the defaulted recovery silently used the first publish's
`wake=False`, **and** the later `ConnectionError` erased a cancellation the caller still had to honour.
The same scenario with the fix in place reports `wake=True`, `lease=97s`, `cancelled=True` on every
raise after the override, and sends exactly one Signal.

Note what the third row shows about the shape of the bug: pre-fix the *error* was internally consistent
with itself and still wrong, because `resolve_append` had already defaulted its `wake` from the stale
entry before `_append` built the error from those defaults. A reader checking only that one error
against only its own recovery instructions would have found nothing.

## Why the fix is sound

- **One object, one source of truth.** `_remember` now returns the canonical operation and `_append`
  raises from *that* return value. Storage and error cannot disagree again, because they are no longer
  two objects — which is the only structural reason the defect was possible.
- **Replace wake and lease, accumulate cancellation, is the right asymmetry.** A deliberate
  `wake=False` on a recovery means the caller has taken the wake for itself, exactly as
  `publish(wake=False)` does, so honouring the newest instruction is what keeps the two consistent;
  reverting to the older policy is what made the error's own instructions unfollowable. Cancellation
  cannot be discharged before the append is settled, and settling calls `_forget`, so the sticky flag
  never outlives the obligation it stands for.
- **The single caller is threaded correctly.** `_remember` has one call site, in `_append`. The three
  `_append` call sites are `resolve_append`, `publish` and `finish_writing`; the latter two pass
  through `_refuse_while_unresolved` first and draw a fresh sequence number, so neither can reach the
  replacement branch with a record that matches a stored entry.
- **Nothing branches on `.cancelled`.** It is read nowhere in the package outside its own docstrings, so
  widening its meaning from "cancellation ended this attempt" to "cancellation ended any attempt to
  settle this operation" changes no control flow. Had any recovery path branched on it, stickiness
  would have made a later transport failure take the cancellation branch.
- **Concurrency holds.** Two publishes already past `_refuse_while_unresolved` hold distinct sequence
  numbers and therefore distinct idempotency keys, so the second one appends a new entry rather than
  replacing the first, and each is settled and forgotten independently.

## What was run

- The full `tests/contrib/external_workflow_streams/` suite: green.
- `test_wake.py`: 60 passed, including the new case.
- `test_m1_gate.py`: 11 passed — but read what that did and did not check. `m1_gate.py` parses the
  required-test lists out of the **vendored** Core checkout under `temporalio/bridge/sdk-core`, not out
  of the working tree these documents live in, so while the submodule pointer still named the case-78
  commit the gate was reading the 78-case list and case 79 was a mapping beyond the declared count.
  No assertion catches an extra mapping — the count check compares the list against its own heading,
  and the coverage check only walks cases the list contains — so the gate stayed green without arming
  the case. Moving the pointer is what arms it: against these lists it reports 79/79 and 12/12 covered,
  confirmed before the pointer moved by checking the new lists out into the vendored tree, and again
  after.
- The case-count arithmetic across `required-tests/`: `tests-m1.md` 79 plus `tests-m2.md` 12 partitions
  91, both headings agree with the bullets the parser finds, and both prose references agree. The
  pre-existing disagreement in `tests-m2.md`, which said 76 where `tests-m1.md` said 78, is gone.
- `ruff check` and `mypy` on the changed producer and test: clean, including the `_remember` signature
  change from `None` to `_UnresolvedAppend`.

## Residual items

- **Addressed after validation: `_UnresolvedAppend.error()`'s docstring was stale.** It said every
  raise reproduced the *same* recovery, which was true when the retained operation could never
  change. It now says the error reports the *current canonical* recovery, which a deliberate override
  is allowed to change between attempts — the invariant implemented by the fix and recorded in
  `spec/wake-signal.md` and ADR-038.
- **`_forget` matches on `idempotency_key` where `_remember` and `_outstanding` match on the whole
  record.** Not reachable today: a record with a stored key but different bytes cannot be stored,
  because `_outstanding` refuses it with a `ValueError` before the backend is touched and `publish`
  always draws an unused key. If that ever stops holding, `_forget` drops both entries for the key
  while the other two treat them as distinct, and one unsettled append disappears without being
  settled.

## A note on the plan-graph tools

`tools/check_plan_graph.py` and its self-test cannot run against this documentation set: they default
to `arch_docs/streaming-poc-docs/plan`, which does not exist and never has in this vendored copy —
confirmed with `git ls-tree HEAD` rather than by looking at the working tree, so it is not an
uncommitted deletion. The plan lives outside this set, and the machine-checked part of what is here is
`required-tests/`, which the Python gate parses. This is stale guidance in `CLAUDE.md`, not a
consequence of the change validated above, and it means the two commands that guidance asks for after a
plan edit are not available as a check here.
