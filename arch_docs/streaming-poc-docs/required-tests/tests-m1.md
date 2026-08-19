# Milestone 1 required tests — 78 cases

One stream, end to end. Milestone 2's 12 cases are in `tests-m2.md`; the two partition the
88 required cases exactly.

This list is read at test time, not by a human: `tests/contrib/external_workflow_streams/
m1_gate.py` parses the count in the heading and every bullet below it, and maps each case
to the test that covers it. A case added here with no mapping fails that gate rather than
passing unnoticed.

Every bullet is one case. If you add or remove one, update the count in this heading; the
gate checks that the two agree.

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
  under the same request ID, then reported through the
  `temporal_external_stream_shutdown_wake_failed` metric without blocking shutdown and without being reported as delivered.
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

## Boundaries an implementation review found unprotected

New cases are appended here rather than filed under the sections they belong to, because the gate
maps cases by position: inserting one renumbers every case after it and silently repoints every
mapping. Each of these covers an invariant that was stated in a spec and held by nothing.

- The first live drain after a replay sees only records past the marker's boundary **with no
  event-loop turn in between**: the reposition has to have happened by the time the replay returns,
  not merely be scheduled onto the manager's loop. A test that yields before draining passes either
  way and proves only that the loop won a race.
- A marker with *k* segments produces exactly *k* condition-checking drains **counted across the
  whole activation**, driver plus the trailing `_run_once` the job set always runs, with one empty
  segment among the *k*. Counted inside the replay driver alone the number was always right and the
  extra drain was invisible.
- The completion carrying the segment that crosses the byte-budget high-water mark is the one that
  requests rollover, and it carries a decodable terminal — the flag is read after the segment is
  closed, not before.
- A frame larger than the slack a fractional high-water mark leaves ends the activation with
  `BUDGET_ROLLOVER` and requests rollover, rather than raising; the annotation still closes with a
  terminal. A subscription set whose header and terminal alone cannot fit an empty annotation is
  refused at `subscribe()`, leaving no half-registered wait behind.
- Every coordination step after a durable append is inside `publish()`'s acknowledged-wake contract:
  a provider failure in the parked-set read, the generation read, or the claim raises the
  durable-but-unacknowledged error carrying the offset and an explicit recovery, and recovering the
  wake alone leaves exactly one record — and, for `finish_writing()`, exactly one fence.
- A shutdown grace period that expires inside a hanging wake counts **every** subscription it
  abandoned, including those the serial loop never reached, and a Run whose status probe cannot
  answer is counted rather than passed over; a Run that is parked or has an open Workflow Task is
  not counted, or the metric fires on every clean shutdown.
- An external payload store that cannot be reached is row one, not row three, on both delivery
  paths: a storage driver whose `retrieve()` raises an ordinary provider exception reaches the
  consumer as `StreamStorageError` rather than as a converter mismatch, and the classification rule
  leaves it alone rather than relabelling it.
- An activation the annotation byte budget stopped is followed by one that can deliver again: the
  same completion requests rollover and carries a terminal, with a frame larger than the slack the
  fractional high-water mark leaves but not large enough to reach the mark, so the mark cannot stand
  in for the condition. Stopping without asking wedges the Workflow permanently.
- A manager with no run-status probe wired reports no abandoned wakes at shutdown, since it has no
  sweep obligation to have failed at; a metric that fires where the mechanism is unconfigured is not
  alertable.
- A marker with **no segments**, replayed on a Run whose watcher has already buffered records
  published while it was evicted, closes before the activation's own drain: that drain hands nothing
  over, the cursor sits at the marker's boundary, and the records arrive on the activation the
  watcher's re-read announces rather than twice.
- A record is priced by the largest run the **annotation** has encoded, not the largest in the open
  segment: the measurement survives the segment being closed, so the first record of an activation is
  not priced at the bare floor and then found unrecordable.
- A run larger than the spill margin is refused as a capacity limit naming the provider's offsets,
  non-retryably, rather than surfacing as an internal byte-budget error.
- The `subscribe()` capacity floor covers everything an empty annotation carries -- header, terminal,
  one segment frame, and the margin -- so a set that clears header-plus-terminal alone is still
  refused rather than failing on its first completion.
- Abandoning a replay leaves replay mode without running the consumed check or the cursor move, so a
  failing activation reports its own error rather than a nondeterminism error blaming a `subscribe()`
  nobody touched, and commits nothing.
- An append carrying a prefetch epoch a reposition has retracted is refused rather than buffered, so
  a watcher read that began before the reposition cannot undo it -- asserted as a contract, because a
  single-threaded loop cannot place the reposition between a check and an append with no await
  between them.
- A replay activation delivers nothing the annotation does not name, with a poison record already in
  the live buffer and pending waits resolved on every drain as a coalesced readiness job resolves
  them, and records no run of its own.

- One activation hands Workflow code at most `MAX_RECORDS_PER_ACTIVATION` records **across
  subscriptions consumed independently of each other**, with no `merge()` involved: two never-empty
  subscriptions drained by two coroutines that yield after every value receive one budget between
  them, both end blocked on a readiness future, and the segment records exactly what was handed over.
  A ready list carried across an activation boundary is charged to the activation that inherits it, so
  the carry-over plus what is newly delivered is still one budget.
- Two concurrent publishes take their sequence numbers in **invocation** order, not in the order their
  payload codecs finish: a retry that reuses the session id, makes the same calls in the same order,
  and releases the encodes the other way round is idempotent and leaves two records — and the
  cross-topic version, where the swapped key cannot collide and nothing would raise, appends no
  duplicate on either topic.
- Cancellation delivered after a durable append leaves `publish()` as the durable-but-unacknowledged
  error carrying the offset and exactly one recovery, for each of the four post-append stages, and
  performing that recovery leaves one record — one fence, for `finish_writing()`. Cancellation before
  the backend is called at all — while the payload is still encoding — is still a cancellation, with
  nothing appended, no Signal sent, and nothing left owed.
- A second coroutine blocking on one subscription is refused deterministically, on the single-wait path
  and inside `merge()` — which refuses before registering anything — while the consumer already
  blocked still receives the next record and is left resolvable by `close()`. Iterating one
  subscription again after breaking out of a previous `async for` is still allowed, and resumes.
- A `Stale` readiness report whose retry discovers `RunNotFound` sends the owed wake exactly once
  **and** tears the watcher down, with the retry position parameterized, and stops retrying at the
  answer that cannot change.

- An append interrupted **after the backend committed and before it answered** is an unknown outcome
  carrying the exact record, not a failure and not a bare cancellation: settling it with
  `resolve_append()` recovers the original offset and leaves one record — one fence, for
  `finish_writing()` — and the same holds when a `ConnectionError` replaces the cancellation. While it
  is unsettled the stream refuses the `publish()` that would duplicate it, and accepts one again once
  it is settled. Settling an append that never landed appends it once, under the sequence the
  interrupted call drew. `AppendConflictError` stays a refusal, since re-appending cannot change it.

- The unknown-outcome recovery is bound to the operation, the stream and the producer instance. A
  refusal issued while an append is unsettled reports **that** append's wake, lease and cancellation
  rather than the refused call's, and recovering by the refusal's own fields sends the one wake it
  owed. `resolve_append()` refuses a record whose unsettled append is on another topic, refuses
  different bytes under the outstanding key without clearing it, and refuses a replacement producer
  that shares the session id — whose own recovery, re-running the same calls in the same order,
  deduplicates the appends and leaves its sequence and unparked-wake request IDs correct. A recovery
  given no wake policy uses the interrupted call's, so a `wake=False` fence gains no Signal.
