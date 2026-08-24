# External Workflow Streams — documentation map

Adding External Workflow Streams to the Temporal Python SDK: high-volume stream payloads live in a
pluggable external backend, never in Temporal History, while deterministic replay is preserved with
compact markers.

**These documents describe the design, not the code.** They hold what a reader cannot get from the
implementation itself: invariants that span Core, the Python runtime, and the provider; rules that no
single file owns; and the shape of the lifecycle as a whole. Anything a function's own comment
already explains is deliberately absent — read the code for that. These documents carry no revision
narrative, and where an alternative was considered and rejected the reasoning is in `decisions/`.

## Start here

| If you want to… | Read |
|---|---|
| Understand what the feature is | [`overview.md`](overview.md) |
| Know why something is shaped the way it is | [`decisions/`](decisions/README.md) |
| Know what a Workflow Task does from open to marker | [`spec/wft-lifecycle.md`](spec/wft-lifecycle.md) |
| Implement a backend provider | [`spec/backend-contract.md`](spec/backend-contract.md) |
| Trust a test result before acting on it | [`verification-hazards.md`](verification-hazards.md) |
| Review the feature, or know which commits it is | [`review-guide.md`](review-guide.md) — a dated snapshot, not design |
| Know what past reviews found and what was done | [`follow-up-review.md`](follow-up-review.md), then [`third-review.md`](third-review.md), [`fourth-review.md`](fourth-review.md), [`fifth-review.md`](fifth-review.md) — review records, not design |
| Chase the empty-stream replay flake | [`empty-stream-replay-flake-handoff.md`](empty-stream-replay-flake-handoff.md) — a resolved investigation, with what it got wrong |
| Know what is still broken | [`outstanding-issues.md`](outstanding-issues.md) — current dispositions, including which experiments are void |
| Know how the remaining issues will be closed | [`remaining-issues-plan.md`](remaining-issues-plan.md) — evidence gates, ownership branches, and completion criteria |
| Chase the suite flakes, or the WFT admission window | [`wft-double-dispatch-flake-handoff.md`](wft-double-dispatch-flake-handoff.md) — three passes of investigation; the conclusions at the top supersede the archive below them |

## Specs — one subsystem per file

| File | Answers |
|---|---|
| [`spec/backend-contract.md`](spec/backend-contract.md) | What a provider must implement, cursor semantics, the immutability precondition, producer binding, park intents, write fences, retention |
| [`spec/wft-lifecycle.md`](spec/wft-lifecycle.md) | When a Workflow Task is retained and when its wait set is merely registered, the idle timeout, rollover, parking, the three generations, shutdown and eviction, the durability boundary |
| [`spec/annotation-format.md`](spec/annotation-format.md) | The replay annotation grammar, runs, segments, the byte budget and the four rules that make it a bound, replay validation, cursor origin |
| [`spec/core-lang-protocol.md`](spec/core-lang-protocol.md) | Every proto message, activation job, readiness call, and piece of Core state |
| [`spec/python-runtime.md`](spec/python-runtime.md) | The out-of-sandbox manager, the split decode path and the serialization context both halves carry, the four cursor positions, why the reposition after a replay is synchronous, the two delivery budgets, how `merge()` stays fair across activations, what closing a subscription ends and what it keeps, which side answers which activation job |
| [`spec/wake-signal.md`](spec/wake-signal.md) | The reserved Signal's name, envelope, request-ID derivation, and interception |
| [`spec/failure-taxonomy.md`](spec/failure-taxonomy.md) | The four failure classes, their error types, metrics, and operator responses |

## The rest of the set

- [`decisions/`](decisions/README.md) — 40 records, one per decision, each with the alternatives that
  were rejected. A spec states what is true; a decision record states why the other shape was not
  taken.
- [`required-tests/`](required-tests/) — the two required-test lists. These are not prose: the Python
  suite parses them, checks the declared case counts, and requires every case to map to a test that
  exists. Editing a bullet or a heading count changes what that gate demands.
- [`verification-hazards.md`](verification-hazards.md) — three ways a test result here can be
  confidently wrong: two that produced a written defect report against correct code, and one that
  lets a required-test gate pass while checking nothing.
- [`outstanding-issues.md`](outstanding-issues.md),
  [`remaining-issues-plan.md`](remaining-issues-plan.md), and
  [`wft-double-dispatch-flake-handoff.md`](wft-double-dispatch-flake-handoff.md) — current
  dispositions, the closure plan, and the investigations behind them. Records, not design: they
  carry dates and evidence state, and they say which experiments turned out to be invalid.

## Conventions these documents hold to

- **One fact, one home.** Each concept is specified in exactly one file; everything else links to it.
  If you find the same rule stated in two places, one of them is a bug.
- **No line numbers, and no restatement of code.** Specs name files and symbols; `grep` resolves them
  and cannot go stale. A claim that a single function's own comment already carries does not belong
  here.
- **Every claim is paired with what breaks without it.** A rule with no failure attached to it is
  either obvious or unverified, and neither is worth a reader's time.
- **Rejected alternatives live in `decisions/`.** A spec states what is true.
