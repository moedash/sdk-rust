# Implementation plan

The plan is split so that a machine can check it and a person can read only the part
they are working on.

| File | Holds |
|---|---|
| `deliverables-x.md` | Phase 0 foundations — X1, X2, X3 |
| `deliverables-p.md` | Track P, Python with no Core dependency — 12 deliverables |
| `deliverables-c.md` | Track C, Core — 18 deliverables |
| `deliverables-integration.md` | Needs both tracks — 14 deliverables |
| `milestones.md` | Milestone membership and acceptance criteria |
| `schedule.md` | The machine-readable stage schedule and why it is what it is |
| `tests-m1.md`, `tests-m2.md` | The required-test lists the milestone gates run |

## The declaration format is load-bearing

Every deliverable is declared as a header line:

```text
**P3 — Redis Streams backend, core operations** ⇢ P2, X2
```

`⇢` is omitted when there are no dependencies. Every milestone declares its members
on a `**Members:** ID, ID, …` line. The schedule is a fenced ` ```plan-order ` block
of `N: ID, ID` stage lines. Those three forms are the graph — prose is not.

## The checker

`tools/check_plan_graph.py` reads every `.md` file in this directory and fails on
exactly seven conditions:

| # | Condition |
|---|---|
| 1 | an ID declared twice |
| 2 | a dependency naming an ID with no deliverable section |
| 3 | a milestone member with no deliverable section |
| 4 | a deliverable in no milestone, or in two |
| 5 | a cycle |
| 6 | a milestone not closed over its dependencies |
| 7 | a deliverable missing from the stage schedule, listed twice, or scheduled in the same stage as — or earlier than — one of its dependencies |

It lives in the task directory, outside this repo, and defaults to this plan. Run it after any
edit here:

```bash
cd <task-dir>                            # .../tasks/python-sdk-streaming
python3 tools/check_plan_graph.py
python3 tools/test_check_plan_graph.py
```

## What the checker does not do

It does not read prose, `Done when` criteria, or rationale. It cannot tell whether a
dependency list is *semantically* complete, only that it is structurally sound. Two
obligations therefore stay with reviewers:

- **Prose must not introduce an edge.** Rationale inside a deliverable body explains
  declared edges. Anything argued there but not declared on the header line is a bug
  in the declaration. `--audit-references` lists IDs mentioned in a body but absent
  from its dependencies, as an advisory aid with known false positives; it never
  affects the exit code.
- **Every `Done when` must be satisfiable from that deliverable's declared transitive
  closure.** A criterion needing a capability owned by a later deliverable is
  unmeetable at the point the graph says the work is finished. Re-check by hand
  whenever a `Done when` changes; no tool does it.

The recurring failure this guards against is an end-to-end assertion attached to the
component that *causes* a behavior rather than to the gate that can *observe* it.
Cross-cutting claims belong to P16a and P16b, which is why those depend on everything
in their milestone.

## IDs

IDs are stable names, not an ordering. These are not in use and must not be assigned
to anything new: **P10**, **P12**, **P16**, **C12**, **C14**, **C15**.
