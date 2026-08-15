# Milestones

A milestone gates *acceptance*, not the earliest legal start of its members. See
`schedule.md` for when work may begin.

## Milestone 0 — plumbing spike (throwaway)

**Not mergeable to a release, and no public API is exported** (ADR-024). Its only purpose is
to prove the Core/Python round trip and de-risk C5 before the real slice is built on it.

**Members:** X1, X2, X3, P1, P2, P3, P4, P17, C1, C2, C3, C4, C5, C6, C7, C12a, C13, P7, P8, P9, P10a, P11

The spike gets rollover *transport* and state preservation (C12a), not marker-integrated
rollover, because it emits no annotation to write. X3 is a member because a plan whose graph
is only checked by hand is what the checker exists to prevent.

**No progress emission in the spike.** P5, P10b, C14a, C14b, C15a, and C15b are deliberately
excluded, and with them the marker half of rollover (C12b): emitting observation deltas
requires a codec to encode them and a Core handler to accept them. The spike therefore tests
the readiness/rollover transport only, on **disposable histories with no replay guarantee** —
a spike run evicted mid-stream is expected to lose its position, which is acceptable precisely
because nothing here is merged.

*Acceptance:* a Workflow subscribes to one topic; a producer publishes 100 records with
inter-record delays below the idle timeout; the Workflow receives all 100, History shows no
per-item events, and the run crosses at least one Workflow Task rollover without failing. 100
records at sub-second gaps exceeds a default 10s Workflow Task timeout, which is why C12a and
C13 are members rather than deferred.
Also gated on `tools/check_plan_graph.py` exiting 0 against this plan directory (X3), so the milestone
that first commits to a sequence is the one that starts enforcing the graph it claims to follow.

## Milestone 1 — first mergeable slice

One stream, end to end, durable. This is the first thing that can carry a public API.

**Members:** C8, C9, C10, C11, C12b, C14a, C14b, C15a, C15b, P2b, P3b, P5, P6, P6a, P6b, P10b, P13, P14, P18, P19, P20, P16a

Every member has a deliverable section with formal dependencies, and every one of those
dependencies is in this milestone or Milestone 0 — X3 checks that structurally rather than it
being asserted here.

**P16a is the cross-path gate.** Assertions that span deliverables — every completion path
producing a complete marker, a second Worker reconstructing a subscription after shutdown in
either Run state, a slow provider not failing a Workflow Task — live there and nowhere else,
because it is the only member that depends on all of them.

*Acceptance*, which is what "production-ready for one stream" means here:

- backend registration and an explicitly bound, verified producer handle;
- idempotent append with exact, inclusive replay offsets;
- asynchronous prefetch outside the Workflow thread;
- Workflow Task retention, readiness, idle timeout, and rollover;
- an observation delta on every activation that changed replay-visible state, including one
  that observed no records at all;
- a complete marker, terminal included, on every Workflow Task completion path — including the
  boundaries Core decides with no Python activation outstanding;
- replay after cache eviction and after Worker restart;
- Workflow Task failure and retry before marker commit;
- normal completion after a consumed record;
- a server-bound command produced from a consumed record;
- parking and a wake Signal using the `WakeSignal` envelope;
- a shutdown or eviction in either Run state that either finalizes and forces a replacement task
  or hands the Run off by acknowledged wake, and never commits an annotation without its
  terminal; and
- History verification showing no stream payloads and bounded marker metadata.

## Milestone 2 — breadth

Only after Milestone 1 passes.

**Members:** P15, P21, P16b

P21 is multiple streams, `merge`, and same-stream subscriptions; P15 is the Continue-As-New
cursor via the reserved internal header; P16b is the Milestone 2 required-test list.

Additional providers and the optional backend outbox for durable wake delivery are **future
work, not members of this milestone** — they have no deliverable sections because nothing here
depends on them.
