<!--
High-level release notes.
Loosely based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).

This file serves users of the other Temporal SDKs, whose workers and clients run on
Core. The repository-root CHANGELOG.md serves users of the Rust SDK.

Log a change here only if a user of one of those SDKs can observe it: different behavior,
an option surfaced to them, a new log or metric, a different interaction with the server.
The question is what the user sees, not which crate or which files your PR touched.

The Rust SDK runs on Core too, so a user-observable change in Core behavior normally
belongs in the root changelog as well, worded for each audience. What belongs only here is
what only the other SDKs' users can see — a Core capability the Rust SDK does not surface,
or a C-bridge change. A Rust-level API change that a language SDK absorbs inside its own
bridge, without its users noticing, belongs in neither file.

When your PR includes a user-facing change, add an entry below under the
appropriate heading (create the heading if it does not yet exist) in the
Unreleased section — never under a released version. Within each heading content
can be free-form. Feel free to include examples, links to docs, or any other
relevant information.

### Added            — new features
### Changed          — changes in existing functionality
### Deprecated       — soon-to-be-removed features
### Breaking Changes — removed or backwards-incompatible features
### Fixed            — notable bug fixes
### Security         — notable security fixes
-->

# Changelog

## Unreleased

### Fixed
* Task-poll targets no longer decrease after cancelled or timed-out polls. Affected pollers still
  retain their slot during backoff, while resource-exhaustion errors still reduce the target.
* Workflow poll balancing now lets non-sticky pollers use capacity after sticky pollers reach their
  configured or autoscaled polling limit.
* Every path that fails a workflow task now only reports the failure to server
  on the task's first attempt, and later attempts are left to time out. Previously `PayloadsTooLarge`
  failures and history fetch failures were re-reported on every attempt.
* The `workflow_task_execution_failed` metric is now recorded for every failed workflow task
  attempt, including attempts whose failure was not sent to the server, and its `failure_reason`
  tag distinguishes `GrpcMessageTooLarge`, `PayloadsTooLarge`, and `RequestTooLarge` on every path.

## [0.9.0] - 2026-09-04

## [0.8.0] - 2026-09-02

### Added
* Added the Core protocol for replay-safe Workflow-originated external stream output, including
  exact Workflow Task History floors, compact staged-output marker proofs, and shared input/output
  replay segmentation.
* External workflow signal and cancellation resolution activations now include the typed server
  failure cause alongside the existing failure.
* Language SDKs can opt in to recording local activity arguments in the local activity marker's
  `input` detail.
* Core console logs can now be emitted as newline-delimited JSON when an SDK selects the JSON log
  format. Configured log filters continue to apply to JSON output.
* Workflow completion-as-cancelled commands can now carry details for recording on the terminal
  history event.
* Worker heartbeats now report the SDK runtime, hosting environments, operating system, and
  architecture once per worker, retrying until the first successful delivery. Runtime options can
  disable the reporting.
* Workers now log a `[TMPRL1104]` warning when a workflow task takes longer than 5 seconds. Set
  `TEMPORAL_WORKFLOW_TASK_DURATION_WARN_SECONDS` to change the threshold.
* Core now supports attaching `EventGroupMarker`s to most workflow commands.
* The `temporal_activity_execution_failed` and `temporal_local_activity_execution_failed` worker
  metrics now carry a `failure_reason` attribute. Each is now split into one time series per
  reason, which may affect existing dashboards.
* Workflow task completions larger than the gRPC request size limit are now paginated automatically when the namespace supports it. Paginated workflow task completions require Temporal Server 1.32.0 or later.
* Workflows can subscribe to server-side streams and append batches of records to them with the
  `SubscribeStream` and `AppendStreamRecords` commands. Consumed ranges reach the workflow as
  `DeliverStreamRecords` activation jobs, and replay hands each recorded range back in the
  activation of the task that consumed it.
* A history fed to a replay worker can carry the stream records its tasks consumed
  (`HistoryForReplay::with_stream_slices`), so a language replayer that fetched them from the
  stream service can replay a consuming workflow. History alone holds only the offsets.
* A task whose history records a consumed range with content that the response carried no
  records for fails before the workflow runs, rather than after it ran on less input. A legacy
  query dispatched that way to a worker that no longer holds the run goes unanswered, so the
  server retries it on the normal task queue, where the records travel with it.

### Breaking Changes :boom:
* The following types are now non-exhaustive: `Priority`, `WorkerDeploymentVersion`,
  `WorkerCallbacks`, `WorkflowExecutionInfo`, `ActivityCloseTimeouts`,
  `ActivityExecutionDecodeHint`, child-workflow and signal decode hints,
  `SerializationContext`, `SerializationContextData`, `PayloadConverter`, `IncomingError`,
  `ScheduleSpec`, and `ScheduleOverlapPolicy`. Construct structs using their respective builders
  or constructors (`WorkerCallbacks::new`, `ActivityExecutionDecodeHint::new`, or
  `SerializationContext::new`); use `Default` for `PayloadConverter`; and add wildcard branches
  when matching enums.
* Renamed `ActivityCloseTimeouts::Both` to `ActivityCloseTimeouts::ScheduleAndStartToClose`.
* Removed the unused `ActExitValue` type. Use `ActivityError::WillCompleteAsync` to mark an
  activity for asynchronous completion.
* Removed the test-only `FailOnNondeterminismInterceptor` from the public API.
* `TaskToken` no longer exposes its underlying bytes directly. Use `TaskToken::into_inner()` to
  consume a token into its bytes.
* Activity failures now include the latest heartbeat details atomically instead of force-flushing a
  throttled heartbeat first. Temporal Server 1.16.0 or newer is required to guarantee those details
  are preserved on failure; workers warn when the server does not advertise support.

### Fixed
* Workers with caching disabled now keep an incomplete retained external stream task until its
  normal boundary, as they do for local Activities. This prevents repeated shutdown markers and
  replacement tasks from starving asynchronous input readiness.
* External stream wake Signals encountered while replay advances through a History page now
  resume reconstructed subscriptions. Workers with caching disabled no longer complete repeated
  empty tasks while unread records remain in the external store.
* Workflow-originated external output no longer forces an empty replacement task from an old
  stream wait after Workflow code has resumed and is awaiting an Activity or timer. This avoids
  delaying that result behind an unnecessary task timeout. A completion that stages a commit and
  reports output still buffered does keep forcing one, since the max publish latency it asked for
  can only be honored while a task is held.
* Workers now defensively buffer a replacement workflow task if it reaches a run that still owns
  one, preserving the outstanding task token in release builds.
* Worker shutdown now drains activity completions that are still flushing their result to the
  server before finishing. Previously such a completion — typically one whose final heartbeat RPC
  was still in flight — could be permanently stranded by shutdown: the activity's result was
  never reported (the server had to time the attempt out before retrying it), and workers missed
  shutdown's slot-permit release deadline, panicking in debug builds.
* The Prometheus exporter now appends `_total` to counter metric names when an SDK enables the
  counter suffix option.
* Update-with-start `ExecuteMultiOperation` calls now use Core's long-poll timeout instead of the
  normal RPC timeout, avoiding premature failures while waiting for an update to reach its
  requested stage.
* An activity failure caused by oversized final heartbeat details is now counted in the
  `temporal_activity_execution_failed` metric as `failure_reason="PayloadsTooLarge"`. Previously it
  was counted under the reason for the failure the activity itself reported, and was not counted at
  all when that failure was benign, even though a payload-limit failure was reported instead.
* Workers now warn when autoscaling task polling encounters errors continuously for one minute.
  Repeated warnings use exponential backoff up to 15-minute intervals and stop after polling
  recovers.
* Workers no longer send worker heartbeats or appear in centralized heartbeat reports before they
  begin polling.
* Ephemeral server processes no longer leak on failed start.
* Local activity resolutions are now delivered to workflows as each activity completes instead of
  waiting for every local activity in the workflow task. This allows sequences of short local
  activities to make progress while a long-running local activity executes in parallel, while
  preserving the resolution ordering recorded in existing histories during replay.
* Try-cancel child workflows no longer cause nondeterminism when they complete or fail after their
  cancellation was requested.
* Nexus tasks are now timed out locally even when the server sends a `request-timeout` header that
  falls outside the Nexus duration grammar, such as a negative value for a task whose deadline has
  already elapsed, a sub-millisecond unit, or a multi-unit value like `1m30s`. Previously such a
  header was ignored entirely, so the handler was never told the task had timed out, and a task
  left unanswered could block worker shutdown indefinitely.
* Workers with a small workflow cache no longer briefly stop accepting new workflows. Sticky
  workflow-task pollers could consume every workflow-cache permit and starve the non-sticky poller,
  so the worker would stop picking up new workflows until a poll timed out (up to ~60s). The poll
  balancer now reserves a non-sticky slot against the workflow cache size rather than the slot
  supplier size.
