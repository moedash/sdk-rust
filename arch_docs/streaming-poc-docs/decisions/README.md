# Decision records

One file per decision. Each states the context, the options that were on the table, the choice, and
what follows from it. **The specs state current truth only; the reasoning for why an alternative was
not taken lives here.**

Read one of these when you are about to change a spec and want to know whether the current shape is
load-bearing. Adding a decision means adding a file and a row below.

| ADR | Decision | Primary spec |
|---|---|---|
| [001](ADR-001-coexist-with-contrib-workflow-streams.md) | Coexist with `contrib.workflow_streams` rather than replacing it | `overview.md` |
| [002](ADR-002-cursor-is-a-position-boundary.md) | A cursor is a position boundary, not a record identity | `spec/backend-contract.md` |
| [003](ADR-003-structural-immutability-is-required.md) | Structural immutability is required of every provider | `spec/backend-contract.md` |
| [004](ADR-004-progress-and-quiescence-are-separate-commands.md) | Progress and quiescence are separate commands | `spec/wft-lifecycle.md` |
| [005](ADR-005-progress-is-an-observation-delta.md) | Progress is an observation delta, emitted on every completion path | `spec/annotation-format.md` |
| [006](ADR-006-runs-record-both-endpoints.md) | A run records both endpoints, not a start plus a count | `spec/annotation-format.md` |
| [007](ADR-007-byte-budget-forces-rollover.md) | A hard byte budget forces rollover rather than growing a marker | `spec/annotation-format.md` |
| [008](ADR-008-no-marker-without-a-terminal.md) | Core never writes a marker without a terminal from Python | `spec/core-lang-protocol.md` |
| [009](ADR-009-shutdown-is-two-transitions.md) | Shutdown and eviction are two transitions | `spec/wft-lifecycle.md` |
| [010](ADR-010-finalization-is-manager-state-only.md) | `FinalizeExternalStreams` is manager-state-only | `spec/python-runtime.md` |
| [011](ADR-011-runtime-only-jobs-run-outside-the-workflow-thread.md) | Runtime-only jobs are handled in `_handle_activation`, not `_apply` | `spec/python-runtime.md` |
| [012](ADR-012-park-intents-are-keyed-per-subscription.md) | Park intents are keyed per subscription, not per stream | `spec/backend-contract.md` |
| [013](ADR-013-readiness-result-distinguishes-five-states.md) | The readiness result distinguishes a cached Run from a missing one | `spec/core-lang-protocol.md` |
| [014](ADR-014-integrity-loss-blocks-rather-than-fails.md) | Integrity loss blocks the Workflow; no terminal-failure opt-in | `spec/failure-taxonomy.md` |
| [015](ADR-015-decode-failure-is-not-integrity-failure.md) | A decode failure is a separate class from an integrity failure | `spec/failure-taxonomy.md` |
| [016](ADR-016-idle-timeout-reduces-by-min.md) | The idle timeout is a Workflow-Task policy reduced by `min` | `spec/wft-lifecycle.md` |
| [017](ADR-017-rollover-is-mandatory-and-needs-its-own-timer.md) | Rollover is mandatory and needs a sink-independent timer | `spec/wft-lifecycle.md` |
| [018](ADR-018-replay-segmentation-is-reproduced.md) | Replay reproduces activation segmentation rather than collapsing it | `spec/annotation-format.md` |
| [019](ADR-019-producer-binding-is-fully-explicit.md) | Producer binding is fully explicit, and the stream name appears once | `spec/backend-contract.md` |
| [020](ADR-020-append-idempotency-is-on-identity.md) | Append is idempotent on identity, not on key alone | `spec/backend-contract.md` |
| [021](ADR-021-delivery-is-broadcast.md) | Delivery to multiple subscriptions is broadcast, not work-sharing | `spec/wft-lifecycle.md` |
| [022](ADR-022-continue-as-new-cursor-in-a-reserved-header.md) | The Continue-As-New cursor travels in a reserved internal header | `spec/annotation-format.md` |
| [023](ADR-023-park-generation-zero-is-the-unparked-wake.md) | `park_generation = 0` is the unparked wake | `spec/wake-signal.md` |
| [025](ADR-025-wake-signal-bypasses-the-data-converter.md) | The wake Signal bypasses the user's `DataConverter` | `spec/wake-signal.md` |
| [026](ADR-026-delivery-is-bounded-by-a-record-count.md) | Delivery within one activation is bounded by a record count, not by time | `spec/python-runtime.md` |
| [027](ADR-027-a-late-binding-rides-its-own-frame.md) | A subscription created after the header is bound by its own frame | `spec/annotation-format.md` |
| [028](ADR-028-decoding-is-split-across-the-boundary.md) | Decoding a record is split across the Worker/Workflow boundary | `spec/python-runtime.md` |
| [029](ADR-029-a-closed-subscription-keeps-its-state.md) | A closed subscription keeps its recorded state | `spec/python-runtime.md` |
| [030](ADR-030-cancellation-removes-its-own-park-intent.md) | Cancelling a subscription removes its park intent itself | `spec/wft-lifecycle.md` |
| [031](ADR-031-an-owed-removal-belongs-to-the-run.md) | An owed park-intent removal belongs to the Run, not to the subscription | `spec/wft-lifecycle.md` |
| [032](ADR-032-park-rollback-catches-cancellation-and-is-not-shielded.md) | Park rollback catches cancellation, and is not shielded | `spec/wft-lifecycle.md` |
| [033](ADR-033-replay-checks-bound-waits-one-directionally.md) | Replay checks that every bound wait was recreated, in one direction only | `spec/annotation-format.md` |
| [034](ADR-034-merge-rotates-its-start-position.md) | `merge()` rotates its start position across passes | `spec/python-runtime.md` |
| [035](ADR-035-both-sides-bind-the-workflows-serialization-context.md) | Both sides of a stream bind the Workflow's serialization context | `spec/python-runtime.md` |
| [036](ADR-036-cancellation-after-a-durable-append-is-an-unacknowledged-wake.md) | Cancellation after a durable append is reported as an unacknowledged wake | `spec/wake-signal.md` |
| [037](ADR-037-a-subscription-has-one-consumer.md) | A subscription has one consumer, and the refusal is on the waiter | `spec/python-runtime.md` |
| [038](ADR-038-an-append-with-no-answer-is-an-unknown-outcome.md) | An append that reports no outcome is unknown, not failed | `spec/wake-signal.md` |
| [039](ADR-039-the-continuation-carries-the-binding-and-it-is-must-understand.md) | The continuation carries the provider binding, and it is must-understand | `spec/annotation-format.md` |
| [040](ADR-040-a-fence-is-ordered-behind-data-writes-only.md) | A fence is ordered behind data writes, and behind nothing else | `spec/backend-contract.md` |
| [041](ADR-041-a-reported-task-keeps-the-readiness-promise.md) | A task on its way to the server keeps the readiness promise it made | `spec/core-lang-protocol.md` |
| [042](ADR-042-one-run-has-one-outstanding-wake-cycle.md) | One Run has one outstanding wake cycle | `spec/wake-signal.md` |
| [043](ADR-043-a-second-workflow-task-is-buffered.md) | A second Workflow Task is buffered, never substituted | `spec/wft-lifecycle.md` |

## The decisions that constrain the most

If you read only a few, read these — the rest of the design leans on them:

- **ADR-003** (structural immutability) is what makes every other size and validation claim true.
- **ADR-008** (no marker without a terminal) governs every completion path in Core.
- **ADR-011** (no I/O on the Workflow thread) is a hard property of the Python Worker, not a
  preference.
- **ADR-002** (cursor as boundary) is the shape every provider operation is built around.
- **ADR-042** (one outstanding wake cycle per Run) is the liveness and deduplication boundary for
  every readiness result that Core cannot accept locally.
- **ADR-043** (buffer a second Workflow Task) is the task-token ownership invariant at Core
  admission.
