//! External stream output commit: the marker a completion writes for staged output, the history
//! floor the manifest is checked against, and how replay hands the recorded manifest back and
//! checks a recomputed one against it.

use crate::{
    replay::{DEFAULT_ACTIVITY_TYPE, TestHistoryBuilder, canned_histories},
    test_help::{
        MockPollCfg, PollWFTRespExt, ResponseType, WorkerExt, WorkerTestHelpers,
        build_mock_pollers, hist_to_poll_resp, mock_worker, start_timer_cmd,
    },
    worker::client::{WorkflowTaskCompletion, mocks::mock_worker_client},
};
use parking_lot::Mutex;
use std::{sync::Arc, time::Duration};
use temporalio_common::{
    protos::{
        constants::EXTERNAL_STREAM_MARKER_NAME,
        coresdk::{
            external_data::{
                ExternalOutputSegmentManifest, ExternalOutputStreamManifest,
                ExternalOutputTopicManifest, ExternalStreamBoundary, ExternalStreamMarkerData,
                extract_external_stream_marker_data,
            },
            workflow_activation::{WorkflowActivation, workflow_activation_job},
            workflow_commands::{
                CompleteWorkflowExecution, ScheduleActivity, UpdateResponse,
                WorkflowOutputStreamCommit, update_response::Response as UpdateOutcome,
                workflow_command,
            },
            workflow_completion::WorkflowActivationCompletion,
        },
        temporal::api::{
            command::v1::command,
            enums::v1::{CommandType, EventType, WorkflowTaskFailedCause},
            workflowservice::v1::RespondWorkflowTaskCompletedResponse,
        },
    },
    worker::WorkerTaskTypes,
};

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
    assert_eq!(replay_outputs(&replayed), vec![manifest]);
    worker
        .complete_workflow_activation(WorkflowActivationCompletion::from_cmds(
            replayed.run_id.clone(),
            vec![start_timer_cmd(1, Duration::from_secs(10))],
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
            vec![start_timer_cmd(1, Duration::from_secs(10))],
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
    let (history, mut manifest) = output_then_timer_history();
    manifest.stage_token.clear();
    replay_with_commit(history, manifest, 0).await;
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

async fn exercise_speculative_output_redelivery(changed_manifest: bool) {
    let workflow_id = if changed_manifest {
        "speculative-changed"
    } else {
        "speculative-identical"
    };
    let mut base = TestHistoryBuilder::default();
    base.add_by_type(EventType::WorkflowExecutionStarted);
    base.add_full_wf_task();
    base.add_activity_task_scheduled("act1");

    let mut speculative_history = base.clone();
    speculative_history.add_workflow_task_scheduled_and_started();
    let update_id = "speculative-update";
    let mut first_attempt =
        hist_to_poll_resp(&speculative_history, workflow_id, ResponseType::OneTask(2));
    first_attempt.add_update_request(update_id, 1);
    let mut redelivery =
        hist_to_poll_resp(&speculative_history, workflow_id, ResponseType::OneTask(2));
    redelivery.add_update_request(update_id, 1);

    let completions: RecordedMarkers = Default::default();
    let reset_ids: Arc<Mutex<Vec<i64>>> = Default::default();
    let recorded = completions.clone();
    let recorded_resets = reset_ids.clone();
    let mut mock_cfg = MockPollCfg::from_resp_batches(
        workflow_id,
        base,
        [
            ResponseType::ToTaskNum(1),
            first_attempt.into(),
            redelivery.into(),
        ],
        mock_worker_client(),
    );
    let mut completion_number = 0;
    mock_cfg.completion_mock_fn = Some(Box::new(move |wft| {
        completion_number += 1;
        recorded.lock().push(stream_marker_data(wft));
        let mut response = RespondWorkflowTaskCompletedResponse::default();
        if completion_number == 2 {
            response.reset_history_event_id = 3;
        }
        recorded_resets.lock().push(response.reset_history_event_id);
        Ok(response)
    }));
    let mut mock = build_mock_pollers(mock_cfg);
    mock.worker_cfg(|w| {
        w.task_types = WorkerTaskTypes::workflow_only();
        w.max_cached_workflows = 1;
    });
    let worker = mock_worker(mock);

    let initial = worker.poll_workflow_activation().await.unwrap();
    let run_id = initial.run_id.clone();
    worker
        .complete_workflow_activation(WorkflowActivationCompletion::from_cmd(
            run_id.clone(),
            ScheduleActivity {
                activity_id: "act1".to_string(),
                activity_type: DEFAULT_ACTIVITY_TYPE.to_string(),
                ..Default::default()
            }
            .into(),
        ))
        .await
        .unwrap();

    let speculative = worker.poll_workflow_activation().await.unwrap();
    let floor = speculative.history_floor_event_id;
    let discarded = output_manifest(&run_id, floor, "discarded-stage-token");
    worker
        .complete_workflow_activation(WorkflowActivationCompletion::from_cmds(
            run_id.clone(),
            vec![
                output_commit_command(discarded.clone()),
                UpdateResponse {
                    protocol_instance_id: update_id.to_string(),
                    response: Some(UpdateOutcome::Rejected(Default::default())),
                }
                .into(),
            ],
        ))
        .await
        .unwrap();

    let redelivered = worker.poll_workflow_activation().await.unwrap();
    assert_eq!(
        redelivered.history_floor_event_id, floor,
        "redelivery reused event IDs and therefore the same exact floor"
    );
    let mut accepted = output_manifest(&run_id, floor, "accepted-stage-token");
    if changed_manifest {
        accepted.topics[0].logical_fingerprint = vec![b'g'; 32];
    }
    worker
        .complete_workflow_activation(WorkflowActivationCompletion::from_cmds(
            run_id,
            vec![
                output_commit_command(accepted.clone()),
                UpdateResponse {
                    protocol_instance_id: update_id.to_string(),
                    response: Some(UpdateOutcome::Accepted(())),
                }
                .into(),
            ],
        ))
        .await
        .unwrap();
    worker.drain_pollers_and_shutdown().await;

    let written = completions.lock();
    assert_eq!(written.len(), 3);
    assert!(written[0].is_empty());
    assert_eq!(written[1].len(), 1);
    assert_eq!(written[1][0].output.as_ref(), Some(&discarded));
    assert_eq!(
        written[2].len(),
        1,
        "the rejected task's marker must not ride along on the redelivery"
    );
    assert_eq!(written[2][0].output.as_ref(), Some(&accepted));
    assert_eq!(
        reset_ids.lock().as_slice(),
        &[0, 3, 0],
        "only the first token-bearing completion was discarded; the redelivery was accepted"
    );
}

#[tokio::test]
async fn speculative_output_redelivery_accepts_a_fresh_token_once() {
    exercise_speculative_output_redelivery(false).await;
    exercise_speculative_output_redelivery(true).await;
}
