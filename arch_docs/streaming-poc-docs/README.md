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

## Specs — one subsystem per file

| File | Answers |
|---|---|
| [`spec/backend-contract.md`](spec/backend-contract.md) | What a provider must implement, cursor semantics, the immutability precondition, producer binding, park intents, write fences, retention |
| [`spec/wft-lifecycle.md`](spec/wft-lifecycle.md) | When a Workflow Task is retained and when its wait set is merely registered, the idle timeout, rollover, parking, the three generations, shutdown and eviction, the durability boundary |
| [`spec/annotation-format.md`](spec/annotation-format.md) | The replay annotation grammar, runs, segments, the byte budget, replay validation, cursor origin |
| [`spec/core-lang-protocol.md`](spec/core-lang-protocol.md) | Every proto message, activation job, readiness call, and piece of Core state |
| [`spec/python-runtime.md`](spec/python-runtime.md) | The out-of-sandbox manager, the four cursor positions, the delivery budget, which side answers which activation job |
| [`spec/wake-signal.md`](spec/wake-signal.md) | The reserved Signal's name, envelope, request-ID derivation, and interception |
| [`spec/failure-taxonomy.md`](spec/failure-taxonomy.md) | The four failure classes, their error types, metrics, and operator responses |

## The rest of the set

- [`decisions/`](decisions/README.md) — 26 records, one per decision, each with the alternatives that
  were rejected. A spec states what is true; a decision record states why the other shape was not
  taken.
- [`required-tests/`](required-tests/) — the two required-test lists. These are not prose: the Python
  suite parses them, checks the declared case counts, and requires every case to map to a test that
  exists. Editing a bullet or a heading count changes what that gate demands.
- [`verification-hazards.md`](verification-hazards.md) — two ways a test result here can be
  confidently wrong, both of which produced a written defect report against correct code.

## Conventions these documents hold to

- **One fact, one home.** Each concept is specified in exactly one file; everything else links to it.
  If you find the same rule stated in two places, one of them is a bug.
- **No line numbers, and no restatement of code.** Specs name files and symbols; `grep` resolves them
  and cannot go stale. A claim that a single function's own comment already carries does not belong
  here.
- **Every claim is paired with what breaks without it.** A rule with no failure attached to it is
  either obvious or unverified, and neither is worth a reader's time.
- **Rejected alternatives live in `decisions/`.** A spec states what is true.
