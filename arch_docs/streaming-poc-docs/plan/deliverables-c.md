# Track C — Core (critical path)

Line references are to `spec/code-anchors.md`, which carries the file-and-line table for
Core `6e90e6d5` and sdk-python `ec200384`.

**C1 — Protos only**
`workflow_commands.proto`: `WorkflowStreamProgress` (tag 23, carrying `observation_delta`),
`WorkflowStreamQuiescent` (tag 24), `ExternalStreamParkResult` (tag 25, carrying
`final_observation_delta`), `ExternalStreamFinalized` (tag 26), plus the shared
`ExternalStreamWait` message — added to the `WorkflowCommand.variant` oneof, where tags 1–22
are in use.
`external_stream.proto`: `WakeSignal`, the reserved wake Signal envelope — a Signal payload
wire format, not a command or activation variant, and readable by Core without a
`DataConverter` (ADR-025).
`workflow_activation.proto`: `ResolveExternalStreamWaits`, `PrepareExternalStreamPark`,
`ReplayExternalStreams`, `FinalizeExternalStreams` — added to the
`WorkflowActivationJob.variant` oneof, next free tag 17 (tag 3 is retired and not reused; 50
is `RemoveFromCache`).
`external_data.proto`: `ExternalStreamMarkerData`, `ExternalWaitMarker`, `ParkReason`.
`ParkReason` appears here only — it is deliberately not duplicated inside the opaque
annotation.
Full message definitions are in `spec/core-lang-protocol.md`.
*Done when:* `cargo check` passes and `scripts/gen_protos.py` regenerates Python cleanly.
Zero behavior — fully independent and separately mergeable.

**C2 — `ExternalWaitSet` / `ExternalWaitState` types**
Plain Rust types plus transition logic: `BlockedWftOpen → Ready → Parking → Parked`,
generation bookkeeping, staleness rules, and the readiness-vs-park race resolution expressed
as a pure function.
*Done when:* unit tests cover stale-generation rejection and both orderings of the
park/readiness race. Independent of C1 if kept as plain types.

**C3 — `LocalInputs` variants + routing** ⇢ C1
Add `ExternalStreamReady`, `ExternalStreamIdleTimeout`, `ExternalStreamParkResult` to the
enum in `workflow_stream.rs` and to the `run_id()` match; route to `ManagedRun` with no-op
handlers, using the same prioritized local-input lane as local-activity completions.
*Done when:* compiles, existing tests green, handlers demonstrably reached.

**C4 — `notify_external_stream_ready()` on `Worker`** ⇢ C2
Public acknowledged entry point returning
`Accepted | Stale | Parked | NoOpenWorkflowTask | RunNotFound`, shaped like
`record_activity_heartbeat` and routed like `notify_local_result`, but returning an
acknowledgement rather than being fire-and-forget.
`NoOpenWorkflowTask` and `RunNotFound` are separate results: a Run cached between Workflow
Tasks is the healthy post-completion state, and reporting it as a missing Run both corrupts
the metric and tells the watcher to tear itself down while it is still needed (ADR-013).
Also adds the **read-only** companion `external_stream_run_status(run_id) -> WftOpen | Parked
| NoOpenWorkflowTask | RunNotFound`, answered on the same serialized lane, which P20's
shutdown sweep uses. It is deliberately not the readiness call: readiness means "a record is
buffered", so probing with it would assert something false and manufacture a spurious
activation during shutdown.
*Done when:* callable; unit tests cover a confirmed park, a cached Run with no open WFT, and
an unknown run, each returning its own result from both calls, and the status probe provably
leaves the Run's state untouched.

**C5 — Generalize `WaitingOnLAs` into `WaitingOnLocalWork`**
Rename and extend the struct in `managed_run.rs` so WFT retention can be driven by local
activities *or* an external wait set: the retention predicate stops keying directly off
`outstanding_local_activity_count()`. All readers move with it. **Pure refactor — no new
behavior.**
*Done when:* all existing LA tests green with no semantic change. Kept as its own deliverable
specifically so the diff stays reviewable; this is the highest-risk mechanical change in the
plan.

**C6 — `WorkflowStreamQuiescent` retains the WFT and starts the idle timer** ⇢ C1, C2, C3, C5
One timer for the whole wait set, monotonic clock, separate from the WFT rollover deadline
(which derives from the same `wft_timeout` as `sink_heartbeat_timeout_start`).
Retention applies only when no server-bound command accompanies the completion.
*Done when:* a Core integration test shows the WFT held open and the idle timer firing.

**C7 — Readiness cancels the timer and issues `ResolveExternalStreamWaits`** ⇢ C4, C6
Coalesce all wait IDs known ready before shipping the activation; accumulate notifications
arriving while an activation is outstanding.
*Done when:* Core tests show (a) one coalesced activation for simultaneous readiness, (b)
stale generations rejected, (c) readiness before timer expiry cancels the timer.

**C8 — Complete-set park handshake, including its marker integration** ⇢ C1, C2, C6, C7, C14b
Core marks the complete wait set `Parking`, issues `PrepareExternalStreamPark` on idle-timer
expiry or when every wait in the quiescent snapshot is `immediately_parkable`, and resolves
`ExternalStreamParkResult` — `ParkSetConfirmed` carrying the terminal
`final_observation_delta`, or `StreamSetBecameReady` aborting that parking generation. Both
orderings of the readiness/park race resolve to the pure function specified in C2.
**Park owns its own marker path.** The terminal arrives on the park result, not from a
finalization job, so C8 hands the completed annotation to C14b's emitter and completes the
Workflow Task. This is why C8 depends on C14b and why C15a does not own idle park.
*Done when:* Core tests cover readiness accepted before confirmation (park aborted, resolve
issued), confirmation first (park wins, one marker written carrying the park result's
terminal), a stale confirmation for an aborted generation, and a recheck-became-ready that
issues a normal resolve activation rather than running user code from inside the park path —
and that an aborted park writes no marker.

**C9 — External stream state machine** ⇢ C1, C2, C14a
`machines/external_stream_state_machine.rs`, modeled on `local_activity_state_machine.rs`,
registered in `machines/workflow_machines.rs`. Registers waits, holds the accumulated
annotation for the current Workflow Task, and owns the marker command the completion paths
emit. Depends on C14a because accumulation must exist before there is anything for the
machine to emit.
*Done when:* the machine registers and resolves waits under unit test, and a Workflow Task
with several progress reports holds exactly one pending marker.

**C10 — Replay marker lookahead and `ReplayExternalStreams` emission** ⇢ C9, C14b
Marker lookahead before resolving the matching wait set, following the local-activity state
machine's `Replaying → WaitingResolveFromMarkerLookAhead →
ResolvedFromMarkerLookAheadWaitingMarkerEvent` sequence, settled by `MarkerRecorded`. Emits
one `ReplayExternalStreams` per marker carrying the opaque annotation and the Core-readable
`terminal_boundary`. Depends on C14b because a lookahead has nothing to find until markers are
written.
*Done when:* a replayed history resolves its wait set from markers with no timers started and
no readiness path entered, and a marker with no matching state machine is handled the way
local activities already handle that case.

**C11 — Reserved wake-Signal interception** ⇢ C1, C2, C7
Intercept `__temporal_external_stream_wake` before user Signal dispatch, decode the
`WakeSignal` envelope **without a `DataConverter`**, validate chain identity and generation,
and issue `ResolveExternalStreamWaits`. Suppress the Signal from user handlers whether or not
it validates. `park_generation = 0` is the unparked wake and is accepted as a recheck request;
a non-zero unrecognized generation is ignored as stale; an unknown envelope version is ignored
harmlessly (ADR-023).
*Done when:* an unparked wake resumes the Run; a wake naming a park generation Core recognizes
resumes it, with that generation injected into the wait set directly since the handshake that
would produce it live is C8, outside this closure; and unknown-version, stale-generation, and
foreign-chain Signals neither resume the Workflow nor reach a user handler.

**C13 — Run-level timer facility independent of the local-activity sink** ⇢ C5
The Workflow Task rollover deadline is scheduled by `sink_heartbeat_timeout_start`, which
pushes a `StartHeartbeatTimeout` into the local-activity request sink inside
`if let Some(la_sink)`. With no sink it silently returns a handle to a timer that was never
started. The sink exists only when `enable_local_activities`, and Python sets
`enable_local_activities = self._activity_worker is not None` — so a Worker registering
Workflows and no Activities has **no rollover timer at all**, which is exactly the Worker
external streams must support (ADR-017).
Add a per-Run timer facility on `ManagedRun` that does not route through the sink, and make
the local-activity heartbeat one caller of it rather than its owner. The `force_new_wft`
plumbing already exists and is unchanged.
*Done when:* a Core test on a workflow-only Worker shows the rollover deadline firing and
`force_new_wft` set; all existing local-activity heartbeat tests stay green.

**C12a — WFT rollover transport and state preservation** ⇢ C6, C13
On rollover-deadline expiry the task completes with `force_new_wft = true`, and every active
subscription, cursor, and readiness generation survives onto the replacement task. The idle
timeout is clamped below the rollover deadline. **No marker and no finalization**, which is
what makes this the half that can exist in the Milestone 0 spike: the spike carries no
annotation to write.
*Done when:* a Core test with a mock lang worker — no Python and no backend, neither of which
is in this deliverable's closure — shows a retained Workflow Task rolling over at the deadline
with `force_new_wft` set and the wait set, cursors, and readiness generations intact on the
replacement task, on a Worker configured with `enable_local_activities = false`. The
end-to-end "continuously fed stream survives a rollover without losing a record" assertion is
Milestone 0's acceptance criterion, which is where the Python consumer stack exists.

**C12b — Rollover integrated with finalization and marker emission** ⇢ C12a, C14b, C15a
The durable half, and the owner of both rollover paths' marker integration. A rollover
deadline that expires with no Python activation outstanding issues
`FinalizeExternalStreams{ROLLOVER}` through C15a, writes exactly one marker carrying the
returned terminal through C14b, and only then completes with `force_new_wft = true`. The
accumulated annotation is cleared on that completion, per the unwritten-annotation invariant.
Budget-driven rollover (`request_rollover`) takes the same path minus the finalization round
trip, because the triggering `WorkflowStreamProgress` already carried the terminal.
*Done when:* Core tests show a deadline rollover producing one complete marker only after the
finalization response, a budget rollover producing one complete marker with no finalization
job issued, and two consecutive rollovers producing two markers whose annotations reassemble
in Workflow Task order. That replay then delivers every record exactly once across the split
is P13/P16a's assertion — replay reading is not in this closure.

**C14a — Accumulate opaque observation deltas** ⇢ C1, C6
Accept `WorkflowStreamProgress` on every completion path and accumulate `observation_delta`
into `ExternalWaitSet.replay_annotation`, honoring `request_rollover`. Enforce that the
command is ordered before any command whose value could depend on the consumed data.
Accumulation only — this deliverable does not emit markers, so it can land before the marker
state machine exists.
*Done when:* unit tests cover progress-with-terminal-command,
progress-with-server-bound-command, progress-without-retention, and an empty observation delta
accumulating like any other.

**C14b — Marker emission primitive** ⇢ C9, C14a
The generic primitive, **not** the per-path integration: from the accumulated annotation, emit
exactly one marker per Workflow Task however many progress reports it carried, and **refuse to
emit an annotation with no terminal** — a refusal that belongs to emission itself and needs no
finalization job to state (ADR-008). Integrated here only on the completion paths where Python
supplies the terminal in its own `WorkflowStreamProgress`: normal completion, command-producing
completion, and terminal command.
The Core-decided paths integrate marker emission themselves, each against this primitive — park
in C8, rollover in C12b, shutdown and eviction in C15b — and the cross-path assertion that
*every* completion path in the finalization-ownership table produces a complete marker belongs
to P16a, which depends on all of them.
*Done when:* several progress reports collapse into one marker; the three Python-terminal paths
each produce a complete marker; and an attempt to emit an annotation whose encoding has no
terminal is rejected in a unit test rather than written.

**C15a — Annotation finalization protocol for Core-decided boundaries** ⇢ C1, C6, C14a, C14b
The protocol primitive, independent of which boundary triggers it: when Core decides a boundary
with no Python activation outstanding, it issues `FinalizeExternalStreams`, accepts
`ExternalStreamFinalized`, and hands the finalized annotation to C14b's emitter. It never
manufactures a terminal. If the terminal cannot be obtained — Python fails the activation, or
the Run's manager state is gone — Core writes no marker and the Workflow Task fails for retry;
there is no best-effort path (ADR-008).
Also enforces the unwritten-annotation invariant: `ExternalWaitSet.replay_annotation` is
non-empty only while a Workflow Task is open, asserted where it is cleared.
Park is deliberately **not** here: a park obtains its terminal from `ExternalStreamParkResult`,
not from a finalization job, so idle-park integration is C8's.
*Done when:* driving a Core-decided boundary directly at the `ManagedRun` level, with no
activation outstanding, issues the job and writes the marker only after the response arrives; a
finalization failure yields **no** marker and a failed Workflow Task; and the invariant
assertion fires when a non-empty annotation is observed with no open Workflow Task. All three
are Core unit tests against a mock lang — no real timer, park handshake, or Python Worker is
required, and none is available at this point in the graph.

**C15b — Core-owned shutdown and eviction transitions** ⇢ C12a, C13, C14b, C15a
The two transitions, kept separate because they use different mechanisms and only one of them
exists in each Run state (ADR-009):
- **Workflow Task open** (retained by the wait set, or open with an unfinished activation):
  issue `FinalizeExternalStreams{SHUTDOWN}` through C15a's protocol, write the marker through
  C14b's emitter, complete with `force_new_wft = true`. The finalization activation is issued
  before the eviction activation, which `_check_more_activations` already guarantees; the
  marker rides the finalization completion, never the eviction completion, which reports
  nothing and may carry no commands.
- **No open Workflow Task**: nothing is accumulated, so no marker is written and none is
  missing. The server-visible replacement is P20's wake sweep, because `force_new_wft` needs a
  task token this Run does not have.
*Done when:* Core tests with a mock lang show shutdown in each state doing the right one of
those two things — marker plus `force_new_wft` in the first, no marker and no completion in the
second — and eviction with no open Workflow Task provably writing no marker. The end-to-end
assertion that a *second Worker* reconstructs the subscription needs a Python Worker and belongs
to P20 and P16a.
