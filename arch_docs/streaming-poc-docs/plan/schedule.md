# Schedule

**The order is declared as numbered stages and is machine-checked against the graph.**
Everything in one stage may run in parallel; everything a stage member depends on sits in a
strictly earlier stage. X3 fails if any deliverable is missing from the schedule, listed twice,
or scheduled beside or before one of its dependencies.

```plan-order
1: X1, X2, X3
2: P1, C1, C2, C5
3: P2, P4, P5, P18, C3, C4, C13
4: P2b, P3, P6a, P17, P7, C6
5: P3b, P6, P8, P9, C7, C12a, C14a
6: P10a, P10b, P19, C9, C11
7: P11, P14, C14b
8: P6b, P15, C8, C10, C15a
9: P13, P21, C12b, C15b
10: P20
11: P16a
12: P16b
```

Read as a narrative: Phase 0 first; then the two tracks run in parallel — Core through
C1 → C2/C3/C4/C5 → C13/C6 → C7 → C12a/C14a, Python through P1 → P2/P4/P5/P18 →
P3/P2b/P6a/P17 → P3b/P6/P8/P9 — joining at P7 → P8 → P9 → P10a → P11.

## Stages are not milestone boundaries

The two partition the plan differently and overlap on purpose:

| Milestone | Earliest member | Latest member |
|---|---|---|
| Milestone 0 | stage 1 (X1, X2, X3) | stage 7 (P11) |
| Milestone 1 | stage 3 (P5, P18) | stage 11 (P16a) |
| Milestone 2 | stage 8 (P15) | stage 12 (P16b) |

Several Milestone 1 deliverables — P5, P18, P2b, P3b, P6a, P6 — have no Milestone 0
prerequisites and may start in stages 3–5, well before Milestone 0 is accepted. A milestone
gates acceptance; a stage gates the earliest legal start.

## Why some deliverables sit later than the graph requires

The schedule is one legal topological order, not the only one. Three placements are
deliberate, for reasons the graph cannot express:

- **C5 is the schedule risk.** It is the highest-risk mechanical refactor, so it sits in stage 2
  with the roots even though only C6 and C13 need it. C13 follows immediately because it touches
  the same struct and nothing that retains a Workflow Task is safe without it.
- **C11 could be as early as stage 5** — it needs only C1, C2, C7. It is in stage 6 because the
  only things waiting on it, P14 and P6b, are Python-side and later anyway.
- **P6 is in stage 5, after P6a in stage 4**, because `P6 ⇢ P6a`: the append API is built on the
  bound producer handle. The alternative was splitting P6 so an unbound append could land
  earlier, which buys a stage and costs an ID.

## Why the Milestone 1 Core sequence is what it is, edge by edge

- **C14a → C9 → C14b**: accumulation before the state machine that holds it, and emission last,
  because emission is the machine's job.
- **C14b → C8, C15a, C10**: the marker-emission primitive precedes every path that writes a
  marker (park in C8, Core-decided boundaries in C15a) and precedes replay lookahead, which has
  nothing to find until markers exist. Refusing to emit a terminal-less annotation belongs to
  emission itself; obtaining a terminal for a Core-decided boundary belongs to finalization and
  needs an emitter to hand it to.
- **C15a → C12b, C15b**: rollover and shutdown are the two boundaries that go through the
  finalization round trip. Park does not, which is why C8 sits beside C15a rather than after it.
- **C12a → C12b, C15b**: both extend rollover transport rather than reinventing it.
- **C8 after C7**: the park handshake resolves against the same readiness lane C7 establishes,
  and its abort case issues C7's resolve activation.

## Dependencies worth stating explicitly

The formal `⇢` lists in the deliverable files are the graph. This section is rationale only and
**cannot introduce an edge**: anything argued here that is not declared there is a bug in the
declaration. X3 cannot detect that — it never reads prose — so this stays a review rule,
assisted by `check_plan_graph.py --audit-references`.

- P10b ⇢ C1, C14a, P5, P8, P9 — it cannot emit an observation delta it has no codec for (P5), no
  registration state to describe (P8), and no Core handler to accept (C14a).
- Anything claiming production readiness depends on rollover (C12a/C12b, C13), on observation
  transport (C14a/C14b), and on finalization (C15a/C15b), not only on retention.
- Producer wake signaling (P14) depends on the `WakeSignal` envelope and the derived stable
  request ID (C1, C11), not only on the backend; and `publish()`'s acknowledged-wake contract
  (P6b) depends on P14, C11, P2b, and P3b.
- Continue-As-New (P15) depends on the observation delta being committed on the terminal-command
  path (C14b), not only on header propagation — a Continue-As-New that drops the final segment
  restarts the new Run at a stale cursor.
- C15a is on the Milestone 1 critical path in both directions: without it, a rollover deadline
  that expires with no Python activation outstanding writes a marker with no terminal, and a
  shutting-down Worker with a retained Workflow Task leaves it to time out.
- C15b and P20 are two halves of one behavior, declared separately on purpose: they cover
  mutually exclusive Run states, and P20 ⇢ C15b because the sweep must know which state a Run is
  in before it acts. Testing them together would hide the case where the wrong mechanism is
  applied to the wrong state.
- P16a depends on every Milestone 1 deliverable, and P16b on P15, P21, and P16a. A required-test
  list is not runnable ahead of the capabilities it exercises, and stating that as an edge is what
  keeps a milestone's acceptance gate meetable.
- Marker *emission* (C14b) and marker *path integration* are different things. C8, C12b, and C15b
  each depend on C14b and own their own path's integration and tests; the assertion that **every**
  path in the finalization-ownership table produces a complete marker is P16a's, which depends on
  all of them.
