# Core/lang protocol

Every message, call, and piece of Core state the feature adds. Names are internal to the Core
bridge. Line anchors are in `code-anchors.md`.

Owned by C1 (protos), C2–C15b (Core), P7 (bridge).

## Responsibility split

**Python owns:** backend connection, authentication, reads, retries, and subscriptions; payload
serialization and delivery to Workflow code; stable stream offsets and write-fence interpretation;
background readiness watchers and readiness coalescing; the backend side of the race-free
park/wakeup handshake; creation and interpretation of opaque replay annotations, including
cross-stream delivery order; exact-offset reads and integrity checks during replay.

**Core owns:** serializing stream readiness with all other Workflow inputs; tracking deterministic
external wait IDs and generations; holding a Workflow Task open while Workflow code is quiescent;
one global quiescence timeout per open Workflow Task and Workflow Task rollover timing; issuing
activations; coordinating atomic parking of the complete external wait set, marker creation, and
Workflow Task completion; returning recorded annotations to Python during replay; intercepting the
reserved wake Signal and suppressing it from user Signal handlers.

**Core treats the replay annotation as opaque bytes.** It does not understand stream offsets,
payloads, backend types, or codecs. Stream records never pass through `sdk-core`, Signals, or
Temporal History.

## Completion commands

Added to the `WorkflowCommand.variant` oneof in `workflow_commands.proto`. Tags 1–22 are in use;
new variants take 23–26.

```protobuf
// Tag 23. Commits an observation delta for external streams. Emitted on every
// completion path where replay-visible stream state changed -- which includes
// an activation that observed no records at all -- independent of whether the
// WFT is retained. Ordered before any command whose value depends on consumed
// data.
message WorkflowStreamProgress {
  bytes observation_delta = 1;
  // Set when the encoder is approaching the annotation byte budget and the
  // runtime wants the WFT rolled over rather than the marker grown further.
  bool request_rollover = 2;
}

// Tag 24. Asks Core to retain the open WFT. Carries no annotation data.
message WorkflowStreamQuiescent {
  uint64 quiescence_generation = 1;
  repeated ExternalStreamWait waits = 2;
  google.protobuf.Duration idle_timeout = 3;
}

message ExternalStreamWait {
  uint32 wait_id = 1;
  uint64 generation = 2;
  bool immediately_parkable = 3;
}

// Tag 25. Python's answer to PrepareExternalStreamPark.
message ExternalStreamParkResult {
  uint64 quiescence_generation = 1;
  oneof outcome {
    ParkSetConfirmed confirmed = 2;
    StreamSetBecameReady became_ready = 3;
  }
  // Terminal observation delta for the marker Core is about to write. Present
  // on `confirmed`: only Python can encode the boundary, and Core must not
  // write a marker whose annotation has no terminal.
  bytes final_observation_delta = 4;
}

// Tag 26. Python's answer to FinalizeExternalStreams, for the paths where Core
// decides the boundary and no park handshake runs.
message ExternalStreamFinalized {
  uint64 quiescence_generation = 1;
  bytes final_observation_delta = 2;
}
```

`ExternalStreamWait` is a shared message, not a oneof variant, and takes no command tag.
`quiescence_generation` identifies the complete blocked snapshot.

`immediately_parkable` is normally set after a write fence. A single fenced stream does not park the
Workflow Task; Core bypasses the idle delay only when **every** active external wait in the quiescent
snapshot is immediately parkable.

New commands must be added to the `WorkflowCommand` conversion (`TryFrom<WorkflowCommand> for
WFCommand`) and accepted by `validate_completion`.

## Activation jobs

Added to the `WorkflowActivationJob.variant` oneof in `workflow_activation.proto`. New variants
start at 17.

```protobuf
message ResolveExternalStreamWaits {
  uint64 quiescence_generation = 1;
  repeated ExternalStreamWait ready_hints = 2;
}

message PrepareExternalStreamPark {
  uint64 quiescence_generation = 1;
  repeated ExternalStreamWait waits = 2;
  ParkReason reason = 3; // IDLE, ALL_WRITE_FENCED, SHUTDOWN
}

message ReplayExternalStreams {
  uint64 quiescence_generation = 1;
  repeated ExternalStreamWait waits = 2;
  bytes replay_annotation = 3;
  ParkReason terminal_boundary = 4;
}

// Issued before Core writes a marker for a boundary Core itself decided and no
// park handshake will run: rollover-deadline expiry, and shutdown or eviction
// with active waits. Runs no user Workflow code.
message FinalizeExternalStreams {
  uint64 quiescence_generation = 1;
  repeated ExternalStreamWait waits = 2;
  ParkReason reason = 3; // ROLLOVER, SHUTDOWN
}
```

`ResolveExternalStreamWaits` contains no records. The listed waits are readiness **hints**, not an
exhaustive availability claim. On receipt, Python probes every active wait, drains all currently
available inputs, and then resumes Workflow futures.

`PrepareExternalStreamPark` and `FinalizeExternalStreams` are runtime-internal and always cover the
complete active wait set. Python handles both without invoking user Workflow code —
`PrepareExternalStreamPark` may find new records on its final recheck, in which case it returns
`StreamSetBecameReady` and Core issues a normal resolve activation rather than running user code
from inside the park path.

**Two** of these four jobs — `ReplayExternalStreams` and `PrepareExternalStreamPark` — require
backend I/O. `FinalizeExternalStreams` requires **none** (ADR-010). All three are still dispatched
outside the synchronous Workflow thread; see `python-runtime.md`.

## Marker envelope

In `external_data.proto`, alongside the existing `LocalActivityMarkerData` and `PatchedMarkerData`:

```protobuf
message ExternalStreamMarkerData {
  uint32 schema_version = 1;
  uint64 quiescence_generation = 2;
  repeated ExternalWaitMarker waits = 3;
  bytes replay_annotation = 4;
  ParkReason terminal_boundary = 5;
}

message ExternalWaitMarker {
  uint32 wait_id = 1;
  uint64 generation = 2;
}
```

`ParkReason` lives here and only here — see "Who finalizes the annotation" below.

During replay, Core performs marker lookahead before resolving the matching external wait set,
analogous to local-activity marker lookahead: `Replaying --(Schedule)-->
WaitingResolveFromMarkerLookAhead`, then `--(HandleKnownResult)-->
ResolvedFromMarkerLookAheadWaitingMarkerEvent`, settled by `MarkerRecorded`.

## Who finalizes the annotation

The annotation ends with a terminal — the blocked cursor snapshot — and **only Python can encode
it**. Core is annotation-blind by design, so Core cannot manufacture a terminal, and Python cannot
encode a boundary it was never asked to finalize. Several completion paths are decided *inside
Core*, with no Python activation outstanding at the moment of decision:

| Path | Who decides | How the terminal is obtained |
|---|---|---|
| Idle-timeout park | Core timer | `PrepareExternalStreamPark` → `ExternalStreamParkResult.final_observation_delta` |
| All-fenced immediate park | Core, from the quiescence snapshot | same |
| Rollover-deadline expiry | Core timer | `FinalizeExternalStreams{ROLLOVER}` → `ExternalStreamFinalized` |
| Byte-budget rollover | Python, via `request_rollover` | already carried by the triggering `WorkflowStreamProgress` |
| Command-producing completion | Python | already carried by the completion's `WorkflowStreamProgress` |
| Terminal Workflow completion | Python | same, ordered before the terminal command |
| Worker shutdown / eviction, Workflow Task open | Core | `FinalizeExternalStreams{SHUTDOWN}` → `ExternalStreamFinalized`. If Python cannot answer, **no marker is written** and the Workflow Task fails for retry |
| Worker shutdown / eviction, no open Workflow Task | — | No marker exists to write; see `wft-lifecycle.md` |

**The rule that makes this coherent, and that has no exceptions:** Core never writes a marker for a
boundary it decided without first receiving a terminal from Python (ADR-008). If a terminal cannot
be obtained, Core writes nothing and the Workflow Task is retried, because an abandoned Workflow
Task commits no cursor and loses no record, while a truncated annotation is durable and wrong.

`FinalizeExternalStreams` is a runtime-only activation job in the same class as
`PrepareExternalStreamPark` — it runs no user Workflow code, it cannot resolve futures, and its only
legal responses are `ExternalStreamFinalized` or an activation failure.

`ParkReason` therefore lives in exactly one place: the Core-readable
`ExternalStreamMarkerData.terminal_boundary`. Core knows the reason in every row above — it either
decided it or received it — so duplicating it inside the opaque annotation's terminal would add a
second copy that could disagree with the first.

## Readiness and status calls

The bridge exposes two acknowledged, thread-safe calls:

```text
notify_external_stream_ready(run_id, wait_id, wait_generation)
    -> Accepted | Stale | Parked | NoOpenWorkflowTask | RunNotFound

external_stream_run_status(run_id)
    -> WftOpen | Parked | NoOpenWorkflowTask | RunNotFound
```

The second is **read-only** and has no effect on the Run: it exists so the shutdown sweep can ask
what state a Run is in without claiming a record is buffered, which is what `notify_…_ready` means.
Both are answered on the same serialized local-input lane, so a status answer is as authoritative as
a readiness acknowledgement. Core already answers a read-only question on that lane through
`GetStateInfoMsg`; `external_stream_run_status` is the same shape, scoped to one Run's external wait
set.

`Accepted` means Core serialized readiness while the Workflow Task was still open. It cancels the
global quiescence timer or logically aborts an in-progress park, then issues or augments a readiness
activation. Core coalesces all wait IDs known to be ready before shipping that activation;
notifications received while an activation is outstanding are accumulated for the next one. **There
is never more than one outstanding activation per Run.**

The other four results all mean local readiness could not be delivered, and Python sends the wake
Signal for the last three (ADR-013):

| Result | Meaning | Watcher action | Metric |
|---|---|---|---|
| `Accepted` | Readiness was serialized into an open WFT | Nothing further; Core will activate | local wakeup |
| `Stale` | The wait exists but its `wait_generation` moved on | Re-probe; do not signal | stale notification |
| `Parked` | A confirmed `park_generation` exists for this wait | Send the reserved wake Signal | signal wakeup, parked |
| `NoOpenWorkflowTask` | The Run is cached and its waits are registered, but no WFT is open | Send the wake Signal; **keep** the watcher | signal wakeup, unparked |
| `RunNotFound` | The Run is absent from this Core worker's cache | Send the wake Signal, then tear the watcher down | signal wakeup, evicted |

`NoOpenWorkflowTask` is the healthy state between Workflow Tasks after a command-producing
completion or a rollover, and is not an error. The three signal-sending results are distinguished by
what the watcher does *afterwards* and by what an operator should conclude, not by whether a Signal
is sent.

The public entry point goes on `Worker`, following the shape of `record_activity_heartbeat`. Routing
into the workflow stream follows `notify_local_result` → `notify_of_local_result`. Unlike
`notify_local_result`, the stream call must return an acknowledgement rather than being
fire-and-forget.

## Core state

`ManagedRun` tracks multiple external waits as one quiescent set rather than adding stream state to
`LocalActivityManager`:

```rust
struct ExternalWaitState {
    wait_id: u32,
    wait_generation: u64,
    status: ExternalWaitStatus,
    immediately_parkable: bool,
}

enum ExternalWaitStatus {
    BlockedWftOpen,
    Ready,
    Parking,
    Parked,
}

struct ExternalWaitSet {
    quiescence_generation: u64,
    waits: HashMap<u32, ExternalWaitState>,
    ready_wait_ids: HashSet<u32>,
    idle_timeout: Duration,
    idle_timer: AbortHandle,
    replay_annotation: Vec<u8>,
}
```

### Generalizing WFT retention

Retention of an open Workflow Task is currently driven solely by outstanding local activities, via
`ManagedRun`'s `waiting_on_la: Option<WaitingOnLAs>`. Retention is expressed by completing with
`ActivationCompleteOutcome::DoNothing` instead of reporting to the server, keyed off
`outstanding_local_activity_count() == 0`.

The broader per-Run concept is *local work that may retain the Workflow Task*. `WaitingOnLAs` is
renamed and extended into it (C5):

```rust
struct WaitingOnLocalWork {
    /// Present when local activities are outstanding; carries the existing
    /// wft_timeout / hb_timeout_handle / heartbeat_timeout_pending fields.
    local_activities: Option<LocalActivityHeartbeatState>,
    external_wait_set: Option<ExternalWaitSet>,
    wft_rollover_timer: AbortHandle,
}
```

The retention predicate becomes "local activities outstanding **or** the external wait set retains".
This is a pure refactor with no behavior change for local activities.

External waits remain logically pending after parking, but only a wait set containing
`BlockedWftOpen`, `Ready`, or `Parking` states retains the current Workflow Task. Core must not
complete the task merely because one member becomes idle or parkable.

### Local input routing

`enum LocalInputs` gains `ExternalStreamReady`, `ExternalStreamIdleTimeout`, and
`ExternalStreamParkResult`. Each carries a run ID and must be added to the `LocalInputs::run_id()`
match. These use the same prioritized local-input lane as local-activity completions
(`LocalInputs::LocalResolution`).

## Idle timer

The idle timer belongs to Core because its outcome determines whether the Workflow Task is retained
or completed. It measures **global** quiescence across all input streams, rather than the emptiness
duration of each stream independently.

1. Python reports any `WorkflowStreamProgress` delta, then `WorkflowStreamQuiescent` with the
   complete active wait set.
2. Core accumulates the delta and starts one timer using a monotonic clock.
3. Readiness accepted for any current wait cancels the timer and ends that quiescent snapshot.
4. Python drains available inputs and reports a new complete snapshot if the Workflow becomes
   quiescent again; only then does Core start a fresh timer.
5. Timer expiry enters `Parking` for every wait and queues one `PrepareExternalStreamPark`
   activation.
6. Core completes the Workflow Task only after Python confirms the complete backend park handshake.

The timer is operational wall-clock state, not Workflow time. On replay, Core does not run it; the
marker reproduces the recorded idle boundary.

The idle deadline and the Workflow Task rollover deadline are **separate**. The rollover deadline
derives from the same `wft_timeout` that drives the existing local-activity heartbeat, but it cannot
reuse that mechanism — see ADR-017 and C13.

## Implementation locations

Files to touch, with line anchors in `code-anchors.md`:

**Core protobufs** (`crates/protos/protos/local/temporal/sdk/core/`):
`workflow_commands/workflow_commands.proto`, `external_stream/external_stream.proto` (the
`WakeSignal` envelope), `workflow_activation/workflow_activation.proto`,
`external_data/external_data.proto`.

`crates/sdk-core-c-bridge` carries the C-ABI surface used by non-Rust, non-Python SDKs; the
acknowledged readiness call is added there when those SDKs adopt the feature. Python does not go
through it — it uses its own pyo3 bridge.

**Core orchestration:** `workflow_stream.rs` (LocalInputs), `managed_run.rs` (wait set, retention,
timers, finalization, shutdown transitions, the sink-independent timer facility), `workflow/mod.rs`
(command validation and conversion, readiness routing), `worker/mod.rs` (public entry points).

**Core machines:** add `machines/external_stream_state_machine.rs` modeled on
`local_activity_state_machine.rs`; extend `machines/workflow_machines.rs` to register waits,
accumulate annotations, issue markers, and perform replay lookahead. Keep the stream provider out of
`LocalActivityManager`; only the scheduling pattern is shared.

**Python SDK:** see `python-runtime.md`.
