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
4. [`decisions/README.md`](decisions/README.md) — 27 records, one per decision,
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
