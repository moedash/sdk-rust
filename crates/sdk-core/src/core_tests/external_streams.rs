//! External stream output commit: the marker a completion writes for staged output, the history
//! floor the manifest is checked against, and how replay hands the recorded manifest back and
//! checks a recomputed one against it.

use crate::{
    TaskToken,
    replay::{TestHistoryBuilder, canned_histories},
    test_help::{
        MockPollCfg, ResponseType, WorkerExt, WorkerTestHelpers, build_mock_pollers, mock_worker,
        schedule_local_activity_cmd, start_timer_cmd,
    },
    worker::client::{WorkflowTaskCompletion, mocks::mock_worker_client},
};
use parking_lot::Mutex;
use std::{sync::Arc, time::Duration};
use temporalio_common::{
    protos::{
        constants::{EXTERNAL_STREAM_MARKER_NAME, LOCAL_ACTIVITY_MARKER_NAME},
        coresdk::{
            ActivityTaskCompletion,
            activity_result::ActivityExecutionResult,
            common::extract_local_activity_marker_data,
            external_data::{
                ExternalOutputSegmentManifest, ExternalOutputStreamManifest,
                ExternalOutputTopicManifest, ExternalStreamBoundary, ExternalStreamMarkerData,
                extract_external_stream_marker_data,
            },
            workflow_activation::{WorkflowActivation, workflow_activation_job},
            workflow_commands::{
                ActivityCancellationType, CompleteWorkflowExecution, WorkflowOutputStreamCommit,
                workflow_command,
            },
            workflow_completion::WorkflowActivationCompletion,
        },
        temporal::api::{
            command::v1::{Command, command},
            common::v1::Payload,
            enums::v1::{CommandType, EventType, WorkflowTaskFailedCause},
            failure::v1::Failure,
            workflowservice::v1::RespondWorkflowTaskCompletedResponse,
        },
    },
    worker::WorkerTaskTypes,
};
use tokio::sync::Notify;

/// Every completion's external stream markers, in the order the completions were reported.
type RecordedMarkers = Arc<Mutex<Vec<Vec<ExternalStreamMarkerData>>>>;

fn stream_marker_data(wft: &WorkflowTaskCompletion) -> Vec<ExternalStreamMarkerData> {
    wft.commands
        .iter()
        .filter_map(|c| match &c.attributes {
            Some(command::Attributes::RecordMarkerCommandAttributes(m))
                if m.marker_name == EXTERNAL_STREAM_MARKER_NAME =>
            {
                extract_external_stream_marker_data(&m.details)
            }
            _ => None,
        })
        .collect()
}

fn output_manifest(
    run_id: &str,
    history_floor_event_id: i64,
    stage_token: &str,
) -> ExternalOutputStreamManifest {
    ExternalOutputStreamManifest {
        schema_version: 1,
        fingerprint_version: 1,
        stage_token: stage_token.to_string(),
        history_floor_event_id,
        run_id: run_id.to_string(),
        topics: vec![ExternalOutputTopicManifest {
            topic: "results".to_string(),
            record_count: 2,
            logical_byte_count: 7,
            logical_fingerprint: vec![b'f'; 32],
            finished: false,
        }],
        segments: vec![ExternalOutputSegmentManifest {
            record_counts_by_topic: vec![2],
        }],
        provider_id: "test-provider".to_string(),
        provider_format_version: 1,
    }
}

/// The manifest lang re-sends while replaying: recomputed, so it has no stage token.
fn unstaged(mut manifest: ExternalOutputStreamManifest) -> ExternalOutputStreamManifest {
    manifest.stage_token.clear();
    manifest
}

fn output_commit_command(manifest: ExternalOutputStreamManifest) -> workflow_command::Variant {
    workflow_command::Variant::WorkflowOutputStreamCommit(WorkflowOutputStreamCommit {
        manifest: Some(manifest),
    })
}

fn output_marker(
    terminal: ExternalStreamBoundary,
    manifest: ExternalOutputStreamManifest,
) -> ExternalStreamMarkerData {
    ExternalStreamMarkerData {
        schema_version: 1,
        terminal_boundary: terminal as i32,
        output: Some(manifest),
    }
}

/// Start, a task that commits output and starts a timer, the timer firing, and the next task.
fn output_then_timer_history() -> (TestHistoryBuilder, ExternalOutputStreamManifest) {
    let mut t = TestHistoryBuilder::default();
    t.add_by_type(EventType::WorkflowExecutionStarted);
    let manifest = output_manifest(t.get_orig_run_id(), 1, "stage-token");
    t.add_full_wf_task();
    t.add_external_stream_marker_data(output_marker(
        ExternalStreamBoundary::CommandsProduced,
        manifest.clone(),
    ));
    let timer_started = t.add_by_type(EventType::TimerStarted);
    t.add_timer_fired(timer_started, "1".to_string());
    t.add_workflow_task_scheduled_and_started();
    (t, manifest)
}

fn replay_outputs(activation: &WorkflowActivation) -> Vec<ExternalOutputStreamManifest> {
    activation
        .jobs
        .iter()
        .filter_map(|job| match &job.variant {
            Some(workflow_activation_job::Variant::ReplayExternalStreams(replay)) => {
                replay.output.clone()
            }
            _ => None,
        })
        .collect()
}

fn has_fire_timer(activation: &WorkflowActivation) -> bool {
    activation.jobs.iter().any(|job| {
        matches!(
            job.variant,
            Some(workflow_activation_job::Variant::FireTimer(_))
        )
    })
}

/// A worker that records the external stream markers and command types of every completion.
fn worker_recording(
    history: TestHistoryBuilder,
    batches: Vec<ResponseType>,
    markers: RecordedMarkers,
    command_types: Arc<Mutex<Vec<Vec<CommandType>>>>,
    num_expected_fails: usize,
) -> crate::Worker {
    let mut mock_cfg =
        MockPollCfg::from_resp_batches("fakeid", history, batches, mock_worker_client());
    mock_cfg.num_expected_fails = num_expected_fails;
    mock_cfg.completion_mock_fn = Some(Box::new(move |wft| {
        markers.lock().push(stream_marker_data(wft));
        command_types
            .lock()
            .push(wft.commands.iter().map(|c| c.command_type()).collect());
        Ok(RespondWorkflowTaskCompletedResponse::default())
    }));
    let mut mock = build_mock_pollers(mock_cfg);
    mock.worker_cfg(|w| {
        w.task_types = WorkerTaskTypes::workflow_only();
        w.max_cached_workflows = 1;
    });
    mock_worker(mock)
}

#[tokio::test]
async fn activation_carries_the_exact_history_floor_before_its_scheduled_event() {
    let mut mock = build_mock_pollers(MockPollCfg::from_resp_batches(
        "fake_wf_id",
        canned_histories::single_timer("1"),
        [1, 2],
        mock_worker_client(),
    ));
    mock.worker_cfg(|w| {
        w.task_types = WorkerTaskTypes::workflow_only();
        w.max_cached_workflows = 1;
    });
    let worker = mock_worker(mock);

    let first = worker.poll_workflow_activation().await.unwrap();
    let run_id = first.run_id.clone();
    assert_eq!(
        first.history_floor_event_id, 1,
        "event 1 immediately precedes the first WorkflowTaskScheduled event"
    );
    worker
        .complete_workflow_activation(WorkflowActivationCompletion::from_cmds(
            run_id.clone(),
            vec![start_timer_cmd(1, Duration::from_secs(10))],
        ))
        .await
        .unwrap();

    let second = worker.poll_workflow_activation().await.unwrap();
    assert_eq!(
        second.history_floor_event_id, 6,
        "TimerFired event 6 immediately precedes the second WorkflowTaskScheduled event"
    );
    worker.complete_execution(&run_id).await;
    worker.drain_pollers_and_shutdown().await;
}

#[tokio::test]
async fn a_manifest_below_the_exact_floor_fails_the_task() {
    let mut history = TestHistoryBuilder::default();
    history.add_by_type(EventType::WorkflowExecutionStarted);
    history.add_full_wf_task();
    let previous_wft_close = history.current_event_id();
    history.add_we_signaled("deciding-event", vec![]);
    let exact_floor = history.current_event_id();
    history.add_workflow_task_scheduled_and_started();

    let mut mock_cfg = MockPollCfg::from_resp_batches("fakeid", history, [2], mock_worker_client());
    mock_cfg.num_expected_fails = 1;
    mock_cfg.expect_fail_wft_matcher = Box::new(|_, _, failure| {
        failure
            .as_ref()
            .is_some_and(|f| f.message.contains("history floor"))
    });
    let mut mock = build_mock_pollers(mock_cfg);
    mock.worker_cfg(|w| {
        w.task_types = WorkerTaskTypes::workflow_only();
        w.max_cached_workflows = 1;
    });
    let worker = mock_worker(mock);

    let replayed = worker.poll_workflow_activation().await.unwrap();
    let run_id = replayed.run_id.clone();
    worker
        .complete_workflow_activation(WorkflowActivationCompletion::empty(run_id.clone()))
        .await
        .unwrap();

    let producing = worker.poll_workflow_activation().await.unwrap();
    assert_eq!(producing.history_floor_event_id, exact_floor);
    assert!(
        producing.history_floor_event_id > previous_wft_close,
        "the previous Workflow Task close must be below, not inside, the deciding interval"
    );

    worker
        .complete_workflow_activation(WorkflowActivationCompletion::from_cmd(
            run_id,
            output_commit_command(output_manifest(
                &producing.run_id,
                previous_wft_close - 1,
                "false-floor-token",
            )),
        ))
        .await
        .unwrap();

    worker.shutdown().await;
    worker.finalize_shutdown().await;
}

#[tokio::test]
async fn two_commits_in_one_completion_fail_the_task() {
    let mut mock_cfg = MockPollCfg::from_resp_batches(
        "fakeid",
        canned_histories::single_timer("1"),
        [1],
        mock_worker_client(),
    );
    mock_cfg.num_expected_fails = 1;
    mock_cfg.expect_fail_wft_matcher = Box::new(|_, _, failure| {
        failure.as_ref().is_some_and(|f| {
            f.message
                .contains("more than one WorkflowOutputStreamCommit")
        })
    });
    let mut mock = build_mock_pollers(mock_cfg);
    mock.worker_cfg(|w| {
        w.task_types = WorkerTaskTypes::workflow_only();
        w.max_cached_workflows = 1;
    });
    let worker = mock_worker(mock);

    let first = worker.poll_workflow_activation().await.unwrap();
    let manifest = output_manifest(&first.run_id, first.history_floor_event_id, "token");
    worker
        .complete_workflow_activation(WorkflowActivationCompletion::from_cmds(
            first.run_id,
            vec![
                output_commit_command(manifest.clone()),
                output_commit_command(manifest),
            ],
        ))
        .await
        .unwrap();

    worker.shutdown().await;
    worker.finalize_shutdown().await;
}

#[tokio::test]
async fn a_commit_on_an_accepted_task_writes_one_marker() {
    let (history, manifest) = output_then_timer_history();
    let markers: RecordedMarkers = Default::default();
    let worker = worker_recording(
        history,
        vec![1.into(), 2.into()],
        markers.clone(),
        Default::default(),
        0,
    );

    let first = worker.poll_workflow_activation().await.unwrap();
    assert_eq!(
        first.history_floor_event_id,
        manifest.history_floor_event_id
    );
    worker
        .complete_workflow_activation(WorkflowActivationCompletion::from_cmds(
            first.run_id.clone(),
            vec![
                output_commit_command(manifest.clone()),
                start_timer_cmd(1, Duration::from_secs(10)),
            ],
        ))
        .await
        .unwrap();
    let fired = worker.poll_workflow_activation().await.unwrap();
    assert!(has_fire_timer(&fired));
    worker.complete_execution(&fired.run_id).await;
    worker.drain_pollers_and_shutdown().await;

    {
        let written = markers.lock();
        assert_eq!(written.len(), 2);
        assert_eq!(
            written[0],
            vec![output_marker(
                ExternalStreamBoundary::CommandsProduced,
                manifest.clone()
            )]
        );
        assert!(written[1].is_empty());
    }
}

#[tokio::test]
async fn a_commit_with_nothing_else_completes_the_task_with_its_marker() {
    let mut history = TestHistoryBuilder::default();
    history.add_by_type(EventType::WorkflowExecutionStarted);
    let manifest = output_manifest(history.get_orig_run_id(), 1, "commit-only-token");
    history.add_full_wf_task();
    history.add_external_stream_marker_data(output_marker(
        ExternalStreamBoundary::TaskCompleted,
        manifest.clone(),
    ));
    history.add_workflow_execution_completed();

    let markers: RecordedMarkers = Default::default();
    let command_types: Arc<Mutex<Vec<Vec<CommandType>>>> = Default::default();
    let worker = worker_recording(
        history,
        vec![1.into()],
        markers.clone(),
        command_types.clone(),
        0,
    );
    let first = worker.poll_workflow_activation().await.unwrap();
    worker
        .complete_workflow_activation(WorkflowActivationCompletion::from_cmd(
            first.run_id,
            output_commit_command(manifest.clone()),
        ))
        .await
        .unwrap();
    worker.drain_pollers_and_shutdown().await;

    assert_eq!(
        *markers.lock(),
        vec![vec![output_marker(
            ExternalStreamBoundary::TaskCompleted,
            manifest
        )]]
    );
    assert_eq!(*command_types.lock(), vec![vec![CommandType::RecordMarker]]);
}

#[tokio::test]
async fn a_terminal_command_writes_the_output_marker_ordered_before_it() {
    let mut history = TestHistoryBuilder::default();
    history.add_by_type(EventType::WorkflowExecutionStarted);
    let manifest = output_manifest(history.get_orig_run_id(), 1, "terminal-token");
    history.add_full_wf_task();
    history.add_external_stream_marker_data(output_marker(
        ExternalStreamBoundary::WorkflowCompleted,
        manifest.clone(),
    ));
    history.add_workflow_execution_completed();

    let markers: RecordedMarkers = Default::default();
    let command_types: Arc<Mutex<Vec<Vec<CommandType>>>> = Default::default();
    let worker = worker_recording(
        history,
        vec![1.into()],
        markers.clone(),
        command_types.clone(),
        0,
    );
    let first = worker.poll_workflow_activation().await.unwrap();
    // Lang puts the commit last; Core still orders the marker first.
    worker
        .complete_workflow_activation(WorkflowActivationCompletion::from_cmds(
            first.run_id,
            vec![
                CompleteWorkflowExecution::default().into(),
                output_commit_command(manifest.clone()),
            ],
        ))
        .await
        .unwrap();
    worker.drain_pollers_and_shutdown().await;

    assert_eq!(
        *markers.lock(),
        vec![vec![output_marker(
            ExternalStreamBoundary::WorkflowCompleted,
            manifest
        )]]
    );
    assert_eq!(
        *command_types.lock(),
        vec![vec![
            CommandType::RecordMarker,
            CommandType::CompleteWorkflowExecution
        ]]
    );
}

#[tokio::test]
async fn a_marker_in_history_with_a_different_manifest_is_nondeterministic() {
    let (history, recorded) = output_then_timer_history();
    let mut committed = recorded.clone();
    committed.stage_token = "a-different-stage-token".to_string();

    let mut mock_cfg =
        MockPollCfg::from_resp_batches("fakeid", history, [1, 2], mock_worker_client());
    mock_cfg.num_expected_fails = 1;
    mock_cfg.expect_fail_wft_matcher = Box::new(|_, cause, failure| {
        *cause == WorkflowTaskFailedCause::NonDeterministicError
            && failure
                .as_ref()
                .is_some_and(|f| f.message.contains("different external output manifest"))
    });
    let mut mock = build_mock_pollers(mock_cfg);
    mock.worker_cfg(|w| {
        w.task_types = WorkerTaskTypes::workflow_only();
        w.max_cached_workflows = 1;
    });
    let worker = mock_worker(mock);

    let first = worker.poll_workflow_activation().await.unwrap();
    worker
        .complete_workflow_activation(WorkflowActivationCompletion::from_cmds(
            first.run_id,
            vec![
                output_commit_command(committed),
                start_timer_cmd(1, Duration::from_secs(10)),
            ],
        ))
        .await
        .unwrap();
    // Applying the next task reconciles the written marker against History and fails it.
    worker.handle_eviction().await;
    worker.drain_pollers_and_shutdown().await;
}

#[tokio::test]
async fn replay_hands_the_recorded_manifest_back_and_writes_nothing() {
    let (history, manifest) = output_then_timer_history();
    let replay_markers: RecordedMarkers = Default::default();
    let worker = worker_recording(
        history,
        vec![2.into()],
        replay_markers.clone(),
        Default::default(),
        0,
    );
    let replayed = worker.poll_workflow_activation().await.unwrap();
    assert!(replayed.is_replaying);
    assert_eq!(replay_outputs(&replayed), vec![manifest.clone()]);
    worker
        .complete_workflow_activation(WorkflowActivationCompletion::from_cmds(
            replayed.run_id.clone(),
            vec![
                output_commit_command(unstaged(manifest)),
                start_timer_cmd(1, Duration::from_secs(10)),
            ],
        ))
        .await
        .unwrap();
    let fired = worker.poll_workflow_activation().await.unwrap();
    assert!(has_fire_timer(&fired));
    worker.complete_execution(&fired.run_id).await;
    worker.drain_pollers_and_shutdown().await;
    assert_eq!(
        *replay_markers.lock(),
        vec![Vec::<ExternalStreamMarkerData>::new()],
        "the marker found by replay lookahead must not be written again"
    );
}

#[tokio::test]
async fn the_output_marker_survives_a_cache_eviction() {
    let (history, manifest) = output_then_timer_history();
    let markers: RecordedMarkers = Default::default();
    let worker = worker_recording(
        history,
        vec![1.into(), ResponseType::AllHistory],
        markers.clone(),
        Default::default(),
        0,
    );

    let first = worker.poll_workflow_activation().await.unwrap();
    let run_id = first.run_id.clone();
    worker
        .complete_workflow_activation(WorkflowActivationCompletion::from_cmds(
            run_id.clone(),
            vec![
                output_commit_command(manifest.clone()),
                start_timer_cmd(1, Duration::from_secs(10)),
            ],
        ))
        .await
        .unwrap();
    worker.request_workflow_eviction(&run_id);
    worker.handle_eviction().await;

    let replayed = worker.poll_workflow_activation().await.unwrap();
    assert!(replayed.is_replaying);
    assert_eq!(
        replay_outputs(&replayed),
        vec![manifest.clone()],
        "the rebuilt run must receive the recorded manifest instead of staging again"
    );
    worker
        .complete_workflow_activation(WorkflowActivationCompletion::from_cmds(
            run_id.clone(),
            vec![
                output_commit_command(unstaged(manifest.clone())),
                start_timer_cmd(1, Duration::from_secs(10)),
            ],
        ))
        .await
        .unwrap();
    let fired = worker.poll_workflow_activation().await.unwrap();
    assert!(!fired.is_replaying);
    assert!(has_fire_timer(&fired));
    worker.complete_execution(&run_id).await;
    worker.drain_pollers_and_shutdown().await;

    let written = markers.lock();
    assert_eq!(
        written.as_slice(),
        &[
            vec![output_marker(
                ExternalStreamBoundary::CommandsProduced,
                manifest
            )],
            vec![],
        ],
        "only the live task writes the marker; the replayed one does not"
    );
}

/// Replays `history` and answers its first activation with `commit`.
async fn replay_with_commit(
    history: TestHistoryBuilder,
    commit: ExternalOutputStreamManifest,
    num_expected_fails: usize,
) {
    let markers: RecordedMarkers = Default::default();
    let mut mock_cfg = MockPollCfg::from_resp_batches("fakeid", history, [2], mock_worker_client());
    mock_cfg.num_expected_fails = num_expected_fails;
    mock_cfg.expect_fail_wft_matcher =
        Box::new(|_, cause, _| *cause == WorkflowTaskFailedCause::NonDeterministicError);
    if num_expected_fails == 0 {
        let recorded = markers.clone();
        mock_cfg.completion_mock_fn = Some(Box::new(move |wft| {
            recorded.lock().push(stream_marker_data(wft));
            Ok(RespondWorkflowTaskCompletedResponse::default())
        }));
    }
    let mut mock = build_mock_pollers(mock_cfg);
    mock.worker_cfg(|w| {
        w.task_types = WorkerTaskTypes::workflow_only();
        w.max_cached_workflows = 1;
    });
    let worker = mock_worker(mock);

    let replayed = worker.poll_workflow_activation().await.unwrap();
    assert!(replayed.is_replaying);
    worker
        .complete_workflow_activation(WorkflowActivationCompletion::from_cmds(
            replayed.run_id.clone(),
            vec![
                output_commit_command(commit),
                start_timer_cmd(1, Duration::from_secs(10)),
            ],
        ))
        .await
        .unwrap();
    if num_expected_fails == 0 {
        let fired = worker.poll_workflow_activation().await.unwrap();
        assert!(has_fire_timer(&fired));
        worker.complete_execution(&fired.run_id).await;
        worker.drain_pollers_and_shutdown().await;
    } else {
        worker.shutdown().await;
        worker.finalize_shutdown().await;
    }
    assert!(
        markers.lock().iter().all(Vec::is_empty),
        "replay never writes an output marker"
    );
}

#[tokio::test]
async fn a_replayed_commit_matching_history_is_accepted_without_a_stage_token() {
    let (history, manifest) = output_then_timer_history();
    replay_with_commit(history, unstaged(manifest), 0).await;
}

#[tokio::test]
async fn a_replayed_commit_matches_a_marker_a_reset_copied_from_the_base_run() {
    // A reset forks the base run's History, so its markers keep naming the base run while lang
    // recomputes the manifest with the new run's id.
    let mut history = TestHistoryBuilder::default();
    history.add_by_type(EventType::WorkflowExecutionStarted);
    let replayed = output_manifest(history.get_orig_run_id(), 1, "");
    history.add_full_wf_task();
    history.add_external_stream_marker_data(output_marker(
        ExternalStreamBoundary::CommandsProduced,
        output_manifest("reset-base-run", 1, "base-run-stage-token"),
    ));
    let timer_started = history.add_by_type(EventType::TimerStarted);
    history.add_timer_fired(timer_started, "1".to_string());
    history.add_workflow_task_scheduled_and_started();
    replay_with_commit(history, replayed, 0).await;
}

#[tokio::test]
async fn a_replayed_commit_that_differs_from_history_is_nondeterministic() {
    let (history, mut manifest) = output_then_timer_history();
    manifest.topics[0].record_count = 3;
    manifest.segments[0].record_counts_by_topic = vec![3];
    replay_with_commit(history, manifest, 1).await;
}

#[tokio::test]
async fn a_replayed_commit_where_history_recorded_none_is_nondeterministic() {
    let mut history = TestHistoryBuilder::default();
    history.add_by_type(EventType::WorkflowExecutionStarted);
    history.add_full_wf_task();
    let timer_started = history.add_by_type(EventType::TimerStarted);
    history.add_timer_fired(timer_started, "1".to_string());
    history.add_workflow_task_scheduled_and_started();

    let mut mock_cfg = MockPollCfg::from_resp_batches("fakeid", history, [2], mock_worker_client());
    mock_cfg.num_expected_fails = 1;
    mock_cfg.expect_fail_wft_matcher = Box::new(|_, cause, failure| {
        *cause == WorkflowTaskFailedCause::NonDeterministicError
            && failure
                .as_ref()
                .is_some_and(|f| f.message.contains("recorded none"))
    });
    let mut mock = build_mock_pollers(mock_cfg);
    mock.worker_cfg(|w| {
        w.task_types = WorkerTaskTypes::workflow_only();
        w.max_cached_workflows = 1;
    });
    let worker = mock_worker(mock);

    let replayed = worker.poll_workflow_activation().await.unwrap();
    assert!(replayed.is_replaying);
    assert!(replay_outputs(&replayed).is_empty());
    worker
        .complete_workflow_activation(WorkflowActivationCompletion::from_cmds(
            replayed.run_id.clone(),
            vec![
                output_commit_command(output_manifest(&replayed.run_id, 1, "")),
                start_timer_cmd(1, Duration::from_secs(10)),
            ],
        ))
        .await
        .unwrap();
    worker.shutdown().await;
    worker.finalize_shutdown().await;
}

/// The predicate a mock applies to every Workflow Task failure it is asked to report.
type FailMatcher =
    Box<dyn Fn(&TaskToken, &WorkflowTaskFailedCause, &Option<Failure>) -> bool + Send>;

/// Accepts only a nondeterminism failure naming `message`, and signals `failed` when one comes.
fn nondeterminism_matcher(message: &'static str, failed: Arc<Notify>) -> FailMatcher {
    Box::new(move |_, cause, failure| {
        let matches = *cause == WorkflowTaskFailedCause::NonDeterministicError
            && failure
                .as_ref()
                .is_some_and(|f| f.message.contains(message));
        if matches {
            failed.notify_one();
        }
        matches
    })
}

/// Waits a bounded time for the expected Workflow Task failure. Without it, a test whose failure
/// never comes waits forever in shutdown instead of failing.
async fn failed_within_deadline(failed: &Notify) {
    tokio::time::timeout(Duration::from_secs(10), failed.notified())
        .await
        .expect("the expected Workflow Task failure never came");
}

/// A worker that expects exactly one Workflow Task failure: nondeterminism naming `message`.
fn worker_expecting_nondeterminism(
    history: TestHistoryBuilder,
    batches: Vec<ResponseType>,
    message: &'static str,
) -> (crate::Worker, Arc<Notify>) {
    let failed = Arc::new(Notify::new());
    let mut mock_cfg =
        MockPollCfg::from_resp_batches("fakeid", history, batches, mock_worker_client());
    mock_cfg.num_expected_fails = 1;
    mock_cfg.expect_fail_wft_matcher = nondeterminism_matcher(message, failed.clone());
    let mut mock = build_mock_pollers(mock_cfg);
    mock.worker_cfg(|w| {
        w.task_types = WorkerTaskTypes::workflow_only();
        w.max_cached_workflows = 1;
    });
    (mock_worker(mock), failed)
}

#[tokio::test]
async fn a_replay_that_commits_less_than_history_fails_when_going_live() {
    let (history, manifest) = output_then_timer_history();
    let (worker, failed) =
        worker_expecting_nondeterminism(history, vec![2.into()], "did not commit");

    let replayed = worker.poll_workflow_activation().await.unwrap();
    assert!(replayed.is_replaying);
    assert_eq!(replay_outputs(&replayed), vec![manifest]);
    // The Workflow no longer publishes, so lang sends the timer alone.
    worker
        .complete_workflow_activation(WorkflowActivationCompletion::from_cmds(
            replayed.run_id.clone(),
            vec![start_timer_cmd(1, Duration::from_secs(10))],
        ))
        .await
        .unwrap();
    failed_within_deadline(&failed).await;
    worker.shutdown().await;
    worker.finalize_shutdown().await;
}

#[tokio::test]
async fn a_replay_that_commits_less_than_history_fails_before_the_next_replayed_task() {
    let (mut history, manifest) = output_then_timer_history();
    history.add_workflow_task_completed();
    let second_timer = history.add_by_type(EventType::TimerStarted);
    history.add_timer_fired(second_timer, "2".to_string());
    history.add_workflow_task_scheduled_and_started();
    let (worker, failed) =
        worker_expecting_nondeterminism(history, vec![3.into()], "did not commit");

    let replayed = worker.poll_workflow_activation().await.unwrap();
    assert!(replayed.is_replaying);
    assert_eq!(replay_outputs(&replayed), vec![manifest]);
    worker
        .complete_workflow_activation(WorkflowActivationCompletion::from_cmds(
            replayed.run_id.clone(),
            vec![start_timer_cmd(1, Duration::from_secs(10))],
        ))
        .await
        .unwrap();
    failed_within_deadline(&failed).await;
    worker.shutdown().await;
    worker.finalize_shutdown().await;
}

#[tokio::test]
async fn a_replay_that_commits_less_in_the_final_task_of_a_closed_history_fails() {
    let mut history = TestHistoryBuilder::default();
    history.add_by_type(EventType::WorkflowExecutionStarted);
    let manifest = output_manifest(history.get_orig_run_id(), 1, "terminal-token");
    history.add_full_wf_task();
    history.add_external_stream_marker_data(output_marker(
        ExternalStreamBoundary::WorkflowCompleted,
        manifest.clone(),
    ));
    history.add_workflow_execution_completed();
    let (worker, failed) =
        worker_expecting_nondeterminism(history, vec![ResponseType::AllHistory], "did not commit");

    let replayed = worker.poll_workflow_activation().await.unwrap();
    assert!(replayed.is_replaying);
    assert_eq!(replay_outputs(&replayed), vec![manifest]);
    worker
        .complete_workflow_activation(WorkflowActivationCompletion::from_cmds(
            replayed.run_id.clone(),
            vec![CompleteWorkflowExecution::default().into()],
        ))
        .await
        .unwrap();
    failed_within_deadline(&failed).await;
    worker.shutdown().await;
    worker.finalize_shutdown().await;
}

/// Every completion's commands, in the order the completions were reported.
type RecordedCommands = Arc<Mutex<Vec<Vec<Command>>>>;

/// A worker that runs local activities and records the commands of every completion.
fn local_activity_worker(
    history: TestHistoryBuilder,
    batches: Vec<ResponseType>,
    completions: RecordedCommands,
    num_expected_fails: usize,
) -> (crate::Worker, Arc<Notify>) {
    let failed = Arc::new(Notify::new());
    let mut mock_cfg =
        MockPollCfg::from_resp_batches("fakeid", history, batches, mock_worker_client());
    mock_cfg.num_expected_fails = num_expected_fails;
    if num_expected_fails == 0 {
        mock_cfg.completion_mock_fn = Some(Box::new(move |wft| {
            completions.lock().push(wft.commands.clone());
            Ok(RespondWorkflowTaskCompletedResponse::default())
        }));
    } else {
        mock_cfg.expect_fail_wft_matcher = nondeterminism_matcher("did not commit", failed.clone());
    }
    let mut mock = build_mock_pollers(mock_cfg);
    mock.worker_cfg(|w| {
        w.task_types = WorkerTaskTypes {
            enable_local_activities: true,
            ..WorkerTaskTypes::workflow_only()
        };
        w.max_cached_workflows = 1;
    });
    (mock_worker(mock), failed)
}

fn schedule_local_activity(seq: u32) -> workflow_command::Variant {
    schedule_local_activity_cmd(
        seq,
        &seq.to_string(),
        ActivityCancellationType::TryCancel,
        Duration::from_secs(10),
    )
}

fn local_activity_result() -> Payload {
    Payload {
        data: b"done".to_vec(),
        ..Default::default()
    }
}

/// Runs the local activity lang just scheduled and returns the activation that resolves it.
async fn run_local_activity(worker: &crate::Worker) -> WorkflowActivation {
    let task = worker.poll_activity_task().await.unwrap();
    worker
        .complete_activity_task(ActivityTaskCompletion {
            task_token: task.task_token,
            result: Some(ActivityExecutionResult::ok(local_activity_result())),
        })
        .await
        .unwrap();
    worker.poll_workflow_activation().await.unwrap()
}

fn only_resolves_local_activity(activation: &WorkflowActivation, seq: u32) -> bool {
    matches!(
        activation.jobs.as_slice(),
        [job] if matches!(
            &job.variant,
            Some(workflow_activation_job::Variant::ResolveActivity(r)) if r.seq == seq
        )
    )
}

fn stream_markers_in(commands: &[Command]) -> Vec<ExternalStreamMarkerData> {
    commands
        .iter()
        .filter_map(|c| match &c.attributes {
            Some(command::Attributes::RecordMarkerCommandAttributes(m))
                if m.marker_name == EXTERNAL_STREAM_MARKER_NAME =>
            {
                extract_external_stream_marker_data(&m.details)
            }
            _ => None,
        })
        .collect()
}

/// The activation index each local activity marker in `commands` recorded.
fn local_activity_activation_indexes(commands: &[Command]) -> Vec<Option<u64>> {
    commands
        .iter()
        .filter_map(|c| match &c.attributes {
            Some(command::Attributes::RecordMarkerCommandAttributes(m))
                if m.marker_name == LOCAL_ACTIVITY_MARKER_NAME =>
            {
                extract_local_activity_marker_data(&m.details).map(|d| d.activation_index)
            }
            _ => None,
        })
        .collect()
}

/// Two output manifests with the same floor and run, told apart by their topic.
fn two_manifests(run_id: &str, floor: i64) -> [ExternalOutputStreamManifest; 2] {
    let first = output_manifest(run_id, floor, "first-token");
    let mut second = output_manifest(run_id, floor, "second-token");
    second.topics[0].topic = "progress".to_string();
    [first, second]
}

/// What a first task that commits, runs a local activity, then commits again and starts a timer
/// leaves in History, followed by the timer firing and the next task.
fn two_commits_around_a_local_activity_history()
-> (TestHistoryBuilder, [ExternalOutputStreamManifest; 2]) {
    let mut t = TestHistoryBuilder::default();
    t.add_by_type(EventType::WorkflowExecutionStarted);
    let manifests = two_manifests(t.get_orig_run_id(), 1);
    t.add_full_wf_task();
    t.add_external_stream_marker_data(output_marker(
        ExternalStreamBoundary::CommandsProduced,
        manifests[0].clone(),
    ));
    t.add_local_activity_marker(1, "1", Some(local_activity_result()), None, |d| {
        d.activation_index = Some(1)
    });
    t.add_external_stream_marker_data(output_marker(
        ExternalStreamBoundary::CommandsProduced,
        manifests[1].clone(),
    ));
    let timer_started = t.add_by_type(EventType::TimerStarted);
    t.add_timer_fired(timer_started, "1".to_string());
    t.add_workflow_task_scheduled_and_started();
    (t, manifests)
}

#[tokio::test]
async fn commits_around_a_local_activity_write_one_marker_each_in_order() {
    let (history, manifests) = two_commits_around_a_local_activity_history();
    let completions: RecordedCommands = Default::default();
    let (worker, _) =
        local_activity_worker(history, vec![1.into(), 2.into()], completions.clone(), 0);

    let first = worker.poll_workflow_activation().await.unwrap();
    let run_id = first.run_id.clone();
    worker
        .complete_workflow_activation(WorkflowActivationCompletion::from_cmds(
            run_id.clone(),
            vec![
                output_commit_command(manifests[0].clone()),
                schedule_local_activity(1),
            ],
        ))
        .await
        .unwrap();
    let resolved = run_local_activity(&worker).await;
    assert!(only_resolves_local_activity(&resolved, 1));
    assert_eq!(
        resolved.history_floor_event_id, first.history_floor_event_id,
        "both activations belong to the same Workflow Task"
    );
    worker
        .complete_workflow_activation(WorkflowActivationCompletion::from_cmds(
            run_id.clone(),
            vec![
                output_commit_command(manifests[1].clone()),
                start_timer_cmd(1, Duration::from_secs(10)),
            ],
        ))
        .await
        .unwrap();
    // The next task's History holds the markers written above, and Core matches them in order.
    let fired = worker.poll_workflow_activation().await.unwrap();
    assert!(has_fire_timer(&fired));
    worker.complete_execution(&run_id).await;
    worker.drain_pollers_and_shutdown().await;

    let completions = completions.lock();
    let first_task = &completions[0];
    assert_eq!(
        first_task
            .iter()
            .map(|c| c.command_type())
            .collect::<Vec<_>>(),
        vec![
            CommandType::RecordMarker,
            CommandType::RecordMarker,
            CommandType::RecordMarker,
            CommandType::StartTimer,
        ]
    );
    assert_eq!(
        stream_markers_in(first_task),
        manifests
            .iter()
            .map(|m| output_marker(ExternalStreamBoundary::CommandsProduced, m.clone()))
            .collect::<Vec<_>>()
    );
    assert_eq!(
        local_activity_activation_indexes(first_task),
        vec![Some(1)],
        "the local activity resolved in the task's second activation"
    );
}

#[tokio::test]
async fn replay_splits_commits_around_a_local_activity_into_the_live_activations() {
    let (history, manifests) = two_commits_around_a_local_activity_history();
    let completions: RecordedCommands = Default::default();
    let (worker, _) = local_activity_worker(history, vec![2.into()], completions.clone(), 0);

    let first = worker.poll_workflow_activation().await.unwrap();
    let run_id = first.run_id.clone();
    assert!(first.is_replaying);
    assert_eq!(
        replay_outputs(&first),
        manifests.to_vec(),
        "every manifest of the task arrives in its first activation, in History order"
    );
    worker
        .complete_workflow_activation(WorkflowActivationCompletion::from_cmds(
            run_id.clone(),
            vec![
                output_commit_command(unstaged(manifests[0].clone())),
                schedule_local_activity(1),
            ],
        ))
        .await
        .unwrap();
    // The recorded result resolves the activity in its own activation, as it did live.
    let resolved = worker.poll_workflow_activation().await.unwrap();
    assert!(resolved.is_replaying);
    assert!(only_resolves_local_activity(&resolved, 1));
    worker
        .complete_workflow_activation(WorkflowActivationCompletion::from_cmds(
            run_id.clone(),
            vec![
                output_commit_command(unstaged(manifests[1].clone())),
                start_timer_cmd(1, Duration::from_secs(10)),
            ],
        ))
        .await
        .unwrap();
    let fired = worker.poll_workflow_activation().await.unwrap();
    assert!(!fired.is_replaying);
    assert!(has_fire_timer(&fired));
    worker.complete_execution(&run_id).await;
    worker.drain_pollers_and_shutdown().await;

    assert!(
        completions
            .lock()
            .iter()
            .all(|commands| stream_markers_in(commands).is_empty()),
        "replay never writes an output marker"
    );
}

#[tokio::test]
async fn a_replay_that_drops_the_commit_after_a_local_activity_is_nondeterministic() {
    let (history, manifests) = two_commits_around_a_local_activity_history();
    let (worker, failed) = local_activity_worker(history, vec![2.into()], Default::default(), 1);

    let first = worker.poll_workflow_activation().await.unwrap();
    let run_id = first.run_id.clone();
    worker
        .complete_workflow_activation(WorkflowActivationCompletion::from_cmds(
            run_id.clone(),
            vec![
                output_commit_command(unstaged(manifests[0].clone())),
                schedule_local_activity(1),
            ],
        ))
        .await
        .unwrap();
    let resolved = worker.poll_workflow_activation().await.unwrap();
    assert!(only_resolves_local_activity(&resolved, 1));
    // The Workflow no longer publishes after the activity.
    worker
        .complete_workflow_activation(WorkflowActivationCompletion::from_cmds(
            run_id,
            vec![start_timer_cmd(1, Duration::from_secs(10))],
        ))
        .await
        .unwrap();
    failed_within_deadline(&failed).await;
    worker.shutdown().await;
    worker.finalize_shutdown().await;
}

#[tokio::test]
async fn commits_around_a_local_activity_replay_then_go_live_in_the_next_task() {
    let (history, manifests) = two_commits_around_a_local_activity_history();
    let completions: RecordedCommands = Default::default();
    let (worker, _) = local_activity_worker(history, vec![2.into()], completions.clone(), 0);

    let first = worker.poll_workflow_activation().await.unwrap();
    let run_id = first.run_id.clone();
    worker
        .complete_workflow_activation(WorkflowActivationCompletion::from_cmds(
            run_id.clone(),
            vec![
                output_commit_command(unstaged(manifests[0].clone())),
                schedule_local_activity(1),
            ],
        ))
        .await
        .unwrap();
    let resolved = worker.poll_workflow_activation().await.unwrap();
    assert!(only_resolves_local_activity(&resolved, 1));
    worker
        .complete_workflow_activation(WorkflowActivationCompletion::from_cmds(
            run_id.clone(),
            vec![
                output_commit_command(unstaged(manifests[1].clone())),
                start_timer_cmd(1, Duration::from_secs(10)),
            ],
        ))
        .await
        .unwrap();

    // The first live task publishes around a local activity of its own.
    let fired = worker.poll_workflow_activation().await.unwrap();
    assert!(!fired.is_replaying);
    assert!(has_fire_timer(&fired));
    assert!(replay_outputs(&fired).is_empty());
    let live = two_manifests(&run_id, fired.history_floor_event_id);
    worker
        .complete_workflow_activation(WorkflowActivationCompletion::from_cmds(
            run_id.clone(),
            vec![
                output_commit_command(live[0].clone()),
                schedule_local_activity(2),
            ],
        ))
        .await
        .unwrap();
    let resolved = run_local_activity(&worker).await;
    assert!(!resolved.is_replaying);
    assert!(only_resolves_local_activity(&resolved, 2));
    worker
        .complete_workflow_activation(WorkflowActivationCompletion::from_cmds(
            run_id,
            vec![
                CompleteWorkflowExecution::default().into(),
                output_commit_command(live[1].clone()),
            ],
        ))
        .await
        .unwrap();
    worker.drain_pollers_and_shutdown().await;

    let completions = completions.lock();
    assert_eq!(completions.len(), 1, "only the live task completes");
    assert_eq!(
        stream_markers_in(&completions[0]),
        vec![
            output_marker(ExternalStreamBoundary::CommandsProduced, live[0].clone()),
            output_marker(ExternalStreamBoundary::WorkflowCompleted, live[1].clone()),
        ]
    );
    assert_eq!(
        local_activity_activation_indexes(&completions[0]),
        vec![Some(1)]
    );
}

#[tokio::test]
async fn replay_keeps_each_local_activity_result_and_its_commit_in_the_live_activation() {
    // Live, lang committed, scheduled two local activities, and committed again after each one
    // resolved in an activation of its own. Both activity markers sit where they were scheduled.
    let mut history = TestHistoryBuilder::default();
    history.add_by_type(EventType::WorkflowExecutionStarted);
    let [first, second] = two_manifests(history.get_orig_run_id(), 1);
    let mut third = output_manifest(history.get_orig_run_id(), 1, "third-token");
    third.topics[0].topic = "summary".to_string();
    let manifests = vec![first, second, third];
    history.add_full_wf_task();
    history.add_external_stream_marker_data(output_marker(
        ExternalStreamBoundary::CommandsProduced,
        manifests[0].clone(),
    ));
    for seq in 1..=2 {
        history.add_local_activity_marker(
            seq,
            &seq.to_string(),
            Some(local_activity_result()),
            None,
            |d| d.activation_index = Some(u64::from(seq)),
        );
    }
    for (manifest, boundary) in manifests[1..].iter().zip([
        ExternalStreamBoundary::TaskCompleted,
        ExternalStreamBoundary::CommandsProduced,
    ]) {
        history.add_external_stream_marker_data(output_marker(boundary, manifest.clone()));
    }
    let timer_started = history.add_by_type(EventType::TimerStarted);
    history.add_timer_fired(timer_started, "1".to_string());
    history.add_workflow_task_scheduled_and_started();

    let (worker, _) = local_activity_worker(history, vec![2.into()], Default::default(), 0);
    let activation = worker.poll_workflow_activation().await.unwrap();
    let run_id = activation.run_id.clone();
    assert_eq!(replay_outputs(&activation), manifests);
    worker
        .complete_workflow_activation(WorkflowActivationCompletion::from_cmds(
            run_id.clone(),
            vec![
                output_commit_command(unstaged(manifests[0].clone())),
                schedule_local_activity(1),
                schedule_local_activity(2),
            ],
        ))
        .await
        .unwrap();
    let resolved_first = worker.poll_workflow_activation().await.unwrap();
    assert!(
        only_resolves_local_activity(&resolved_first, 1),
        "the second result was recorded for a later activation: {:?}",
        resolved_first.jobs
    );
    worker
        .complete_workflow_activation(WorkflowActivationCompletion::from_cmd(
            run_id.clone(),
            output_commit_command(unstaged(manifests[1].clone())),
        ))
        .await
        .unwrap();
    let resolved_second = worker.poll_workflow_activation().await.unwrap();
    assert!(only_resolves_local_activity(&resolved_second, 2));
    worker
        .complete_workflow_activation(WorkflowActivationCompletion::from_cmds(
            run_id.clone(),
            vec![
                output_commit_command(unstaged(manifests[2].clone())),
                start_timer_cmd(1, Duration::from_secs(10)),
            ],
        ))
        .await
        .unwrap();
    let fired = worker.poll_workflow_activation().await.unwrap();
    assert!(has_fire_timer(&fired));
    worker.complete_execution(&run_id).await;
    worker.drain_pollers_and_shutdown().await;
}
