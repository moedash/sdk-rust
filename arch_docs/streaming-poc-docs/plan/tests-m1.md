# Milestone 1 required tests — 55 cases

One stream, end to end. This list is P16a's gate. Milestone 2's 12 cases are in
`tests-m2.md`; the two partition the 67 required cases exactly. A Milestone 1 gate that
required the whole list would be unmeetable, because a fifth of it exercises capabilities
Milestone 1 deliberately does not ship.

Every bullet is one case. If you add or remove one, update the count in this heading and in
P16a's title and `Done when`.

## Progress, observation, and marker transport

- A consumed record followed by a normal completion commits its marker.
- A consumed record followed by an Activity command commits its marker, ordered before the
  Activity command, and replay delivers the record exactly once.
- A consumed record followed by a terminal command (complete / fail / continue-as-new) commits
  its marker before the terminal command.
- A retained Workflow Task spanning several progress reports produces exactly one marker.
- Every completion path in the finalization-ownership table produces exactly one marker whose
  annotation ends with a terminal — normal, command-producing, terminal command, park
  confirmation, all-fenced immediate park, deadline rollover, budget rollover, and shutdown with
  a Workflow Task open — while an aborted park and a shutdown with no open Workflow Task produce
  none. This is the cross-path assertion; the per-path integrations are separately owned, so
  only this gate can make it.
- A subscription to an empty stream that never receives a record still emits an observation
  delta, and its marker carries provider identity, stream key, and explicit start cursor.
- An activation that drains and observes nothing encodes an empty segment, and the codec
  round-trips an annotation with zero runs in a segment and with zero segments overall.
- Annotation exceeding the byte-budget high-water mark forces rollover instead of growing the
  marker, and replay reassembles the two markers in Workflow Task order.
- A single-stream batch of many records encodes as one run.
- Encoded marker bytes are asserted directly, not inferred from run count: a single-stream batch
  with sparse control records stays flat in bytes as record count grows by three orders of
  magnitude.

## Retention, rollover, and timers

- Readiness wins immediately before idle timeout.
- Idle timeout wins immediately before readiness.
- Backend recheck aborts complete-set parking and removes every park intent.
- Append after confirmed park produces a Signal wakeup.
- Duplicate/stale readiness and Signals are harmless.
- With one active wait, its write fence permits immediate parking without closing the stream.
- WFT rollover preserves every active subscription, cursor, observation delta, and readiness
  generation.
- A rollover deadline that expires with no Python activation outstanding still produces a
  complete marker: Core issues `FinalizeExternalStreams`, Python returns the terminal, and only
  then is the marker written.
- A continuously fed stream with inter-record gaps below the idle timeout survives at least one
  rollover and never exceeds the Workflow Task timeout.
- Rollover fires on a Worker registering Workflows and **no** Activities, i.e. with
  `enable_local_activities = false`.
- A Signal sent while a Workflow Task is retained is delivered no later than the rollover
  deadline.

## Wakeup after unparked completions

- An append after a completion that carried server-bound commands wakes the subscription: the
  watcher observes `NoOpenWorkflowTask` and sends the wake Signal.
- The same, after a rollover completion.
- The three non-local acknowledgements are distinct: a confirmed park returns `Parked`, a cached
  Run between Workflow Tasks returns `NoOpenWorkflowTask`, and an evicted Run returns
  `RunNotFound` — with the corresponding watcher teardown behavior for each.
- A wake Signal with an unknown envelope version, a stale generation, or a foreign
  `first_execution_run_id` is suppressed from user handlers and does not resume the Workflow.
- Two producers retrying the same wake produce the identical Temporal request ID and the server
  deduplicates them.
- An unparked wake carries `park_generation = 0` and is accepted as a recheck, while a non-zero
  generation the Run does not recognize is ignored as stale; two Workers shutting down at
  different times derive **different** request IDs and both wakes are delivered.

## Shutdown, eviction, and finalization

- Shutdown with a Workflow Task open issues `FinalizeExternalStreams{SHUTDOWN}`, writes a marker
  carrying the returned terminal, and completes with `force_new_wft = true`; a second Worker
  receives the replacement task and reconstructs the subscription from that marker.
- Shutdown in the `NoOpenWorkflowTask` window writes **no** marker, resolves the Run's state
  through the read-only status probe rather than a false readiness notification, sends an
  acknowledged unparked wake Signal, and a second Worker receives a Workflow Task and
  reconstructs the subscription — without waiting for an unrelated Workflow event and without
  completing a nonexistent Workflow Task.
- Teardown racing finalization never commits an annotation without its terminal: the Python
  manager's Run state survives until `RemoveFromCache`, and when finalization is forced to fail —
  the manager's Run entry removed underneath it — Core writes no marker, the Workflow Task fails,
  and the retry replays from the previous marker with no record lost and no cursor moved.
- Eviction with no open Workflow Task writes no marker at all, and the invariant that an
  unwritten annotation exists only while a Workflow Task is open holds across both eviction
  states.
- An unacknowledged shutdown wake follows the documented policy: retried within the grace period
  under the same request ID, then reported through the `external_stream_shutdown_wake_failed`
  metric without blocking shutdown and without being reported as delivered.
- `FinalizeExternalStreams` calls no provider method: with a provider that raises on every call,
  finalization still produces a marker with a terminal, and provider code runs on neither the
  Workflow thread nor the finalization path.

## Replay

- Replay performs no live waiting and reproduces recorded boundaries.
- Replay reproduces activation segmentation: a `wait_condition` registered mid-stream fires on
  the same delivery under replay as it did live, for a marker spanning several activations.
- A Workflow that subscribes to an empty stream, parks, is evicted, and replays reproduces the
  same empty boundary and starts from the marker-recorded cursor without reading live backend
  state.
- Deleted or trimmed replay records fail as `StreamIntegrityError`; a backend that is merely
  unreachable during replay fails the Workflow Task with the transient error type instead.
- Deleting the first, the middle, and the last record of a recorded range each fails as integrity
  loss rather than substituting a later record — the middle case is the one a start-plus-count
  encoding would have passed.
- A record whose bytes are intact but whose DataConverter configuration does not match fails as
  `StreamDecodeError`, with a different metric from `StreamIntegrityError`.
- An annotation that does not match the subscriptions Workflow code creates fails as
  nondeterminism, not as a storage failure.
- Worker crash before marker commit re-reads the same offsets.
- The first Run of a chain records an explicit start cursor rather than resolving one from live
  backend state.
- A pending timer or activity in the same activation suppresses retention; subscriptions survive
  to the next Workflow Task.

## Provider conformance and the Python runtime

- A provider whose range read is implemented with exclusive (`XREAD`-style) semantics fails the
  conformance suite; the inclusive range read returns the record at `first_offset` itself.
- A consumer parked at the current tail resumes correctly after an append whose ID could not have
  been predicted — a provider requiring a nameable next ID fails the suite.
- Offsets compare numerically as `(milliseconds, sequence)`, including across a millisecond
  boundary where lexical comparison would be wrong.
- Reusing a `(session_id, sequence)` pair with byte-identical content is a no-op returning the
  original offset; reusing it with different bytes is rejected as an error.
- A backend read slower than the 2-second activation deadlock timeout delays readiness and does
  not fail the Workflow Task.
- A replay range read and a park transaction that each take longer than the deadlock timeout both
  complete, without running provider code on the Workflow thread and without a `_DeadlockError`.
- `_apply` performs no I/O: an activation drains only from the buffer, verified by a provider that
  raises if called from the Workflow thread.
- A full subscription buffer stops prefetch without dropping records or blocking the Workflow
  thread.
- Eviction discards speculative prefetch state: a subscription whose `prefetch_cursor` ran ahead
  of `committed_cursor` restarts from `committed_cursor` and re-delivers the same records.
- Run eviction and Worker shutdown tear down every watcher, buffer, and backend connection for the
  Run.
- A provider that does not declare structural immutability is rejected at Worker registration,
  before any Workflow can name it, rather than failing later at replay.
- A trimmed range (Redis `XTRIM`/`MAXLEN` removing the head of a recorded range) and a deleted
  write fence both fail as `StreamIntegrityError`, distinctly from an unreachable backend.
