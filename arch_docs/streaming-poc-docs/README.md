# External Workflow Streams — documentation map

Adding External Workflow Streams to the Temporal Python SDK: high-volume stream payloads live in a
pluggable external backend, never in Temporal History, while deterministic replay is preserved with
compact markers.

**These documents describe the current design.** They carry no revision narrative. Where an
alternative was considered and rejected, the reasoning is in `decisions/`, not inline.

## Start here

| If you want to… | Read |
|---|---|
| Understand what the feature is | [`overview.md`](overview.md) |
| Know why something is shaped the way it is | [`decisions/`](decisions/README.md) |
| Build a piece of it | [`plan/`](plan/README.md) — find your deliverable, follow its spec links |
| Find a file-and-line anchor in Core or the SDK | [`spec/code-anchors.md`](spec/code-anchors.md) |

## Specs — one subsystem per file

| File | Answers |
|---|---|
| [`spec/backend-contract.md`](spec/backend-contract.md) | What a provider must implement, cursor semantics, the immutability precondition, producer binding, write fences, retention |
| [`spec/wft-lifecycle.md`](spec/wft-lifecycle.md) | When a Workflow Task is retained, the idle timeout, rollover, parking, the three generations, shutdown and eviction, the durability boundary |
| [`spec/annotation-format.md`](spec/annotation-format.md) | The replay annotation grammar, runs, segments, the byte budget, replay validation, cursor origin |
| [`spec/core-lang-protocol.md`](spec/core-lang-protocol.md) | Every proto message, activation job, readiness call, and piece of Core state |
| [`spec/python-runtime.md`](spec/python-runtime.md) | The out-of-sandbox manager, three cursors, why no I/O touches the Workflow thread |
| [`spec/wake-signal.md`](spec/wake-signal.md) | The reserved Signal's name, envelope, request-ID derivation, and interception |
| [`spec/failure-taxonomy.md`](spec/failure-taxonomy.md) | The four failure classes, their error types, metrics, and operator responses |
| [`spec/code-anchors.md`](spec/code-anchors.md) | **Every file-and-line reference**, in one table |

## Which spec covers which deliverable

| Working on | Read |
|---|---|
| P1–P3b, P6, P6a, P6b, P17 | `spec/backend-contract.md` |
| P5, P10b, P13, C14a, C14b | `spec/annotation-format.md` |
| C1–C15b, P7 | `spec/core-lang-protocol.md` + `spec/wft-lifecycle.md` |
| P8, P9, P11, P19 | `spec/python-runtime.md` |
| C11, P14, P20 | `spec/wake-signal.md` |
| P18 | `spec/failure-taxonomy.md` |

## Conventions these documents hold to

- **One fact, one home.** Each concept is specified in exactly one file; everything else links to it.
  If you find the same rule stated in two places, one of them is a bug.
- **Line numbers live only in `spec/code-anchors.md`.** Specs name files and symbols. A Core rebase
  updates one table.
- **The plan's graph is machine-checked.** Deliverable dependencies, milestone membership, and the
  stage schedule are declared in a parseable form and validated by `tools/check_plan_graph.py`. Prose
  never introduces a dependency edge.
- **Rejected alternatives live in `decisions/`.** A spec states what is true now.

## Checks to run after editing

The checker lives in the task directory, outside this repo, and defaults to this plan:

```bash
cd <task-dir>                            # .../tasks/python-sdk-streaming
python3 tools/check_plan_graph.py        # graph, milestones, stage order
python3 tools/test_check_plan_graph.py   # the checker's own failure-class tests
```

Two obligations no tool covers, both described in [`plan/README.md`](plan/README.md): prose must not
introduce a dependency edge, and every `Done when` must be satisfiable from its deliverable's declared
transitive closure.
