use crate::{
    Worker,
    replay::TestHistoryBuilder,
    test_help::{MockPollCfg, ResponseType, WorkerTestHelpers, build_mock_pollers, mock_worker},
    worker::client::mocks::mock_worker_client,
};
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};
use temporalio_common::protos::{
    coresdk::{
        workflow_commands::{AppendStreamRecords, SubscribeStream},
        workflow_completion::WorkflowActivationCompletion,
    },
    temporal::api::{
        command::v1::command,
        enums::v1::{CommandType, EventType, WorkflowTaskFailedCause},
        stream::v1::StreamRecord,
        workflowservice::v1::RespondWorkflowTaskCompletedResponse,
    },
};

/// A worker served one poll response, expected to fail that task as
/// nondeterministic. Returns the worker and the count of failures it reported,
/// which the test asserts itself: the mock only verifies call counts when it
/// is dropped, and a missing failure would otherwise go unnoticed. The task
/// stream is kept open so the eviction that follows the failure can be polled.
fn worker_expecting_one_nondeterminism_failure(
    t: TestHistoryBuilder,
    resp: ResponseType,
) -> (Worker, Arc<AtomicUsize>) {
    worker_expecting_one_failure(t, resp, WorkflowTaskFailedCause::NonDeterministicError)
}

fn worker_expecting_one_failure(
    t: TestHistoryBuilder,
    resp: ResponseType,
    expected: WorkflowTaskFailedCause,
) -> (Worker, Arc<AtomicUsize>) {
    let failures = Arc::new(AtomicUsize::new(0));
    let counted = failures.clone();
    let mut mock = MockPollCfg::from_resp_batches("wfid", t, [resp], mock_worker_client());
    mock.num_expected_fails = 1;
    mock.expect_fail_wft_matcher = Box::new(move |_, cause, _| {
        counted.fetch_add(1, Ordering::Relaxed);
        *cause == expected
    });
    let mut mock = build_mock_pollers(mock);
    mock.make_wft_stream_interminable();
    (mock_worker(mock), failures)
}
// A workflow subscribing itself. The command exists at all because every SDK
// matches issued commands against command-generated events in order, so a
// command producing no event would put that matching out of step. This asserts
// the command goes out and that replaying its event does not trip that check.
#[tokio::test]
async fn subscribe_command_round_trips_through_replay() {
    let mut t = TestHistoryBuilder::default();
    t.add_by_type(EventType::WorkflowExecutionStarted);
    t.add_full_wf_task();
    t.add_stream_subscribed("s1", 4);
    t.add_full_wf_task();

    let mut mock_client = mock_worker_client();
    // A replayed task sends no commands, so there is nothing to assert on the
    // completion here. Rejection is what this test watches for, and core
    // signals that by failing the task.
    mock_client
        .expect_complete_workflow_task()
        .returning(|_, _| Ok(RespondWorkflowTaskCompletedResponse::default()));
    mock_client
        .expect_fail_workflow_task()
        .returning(|_, _, f| panic!("core rejected the reissued subscribe: {f:?}"));

    let mock = MockPollCfg::from_resp_batches("wfid", t, [ResponseType::AllHistory], mock_client);
    let core = mock_worker(build_mock_pollers(mock));

    // Full history, so this activation replays the recorded subscription. Lang
    // reissues the command, and core has to match it to that event rather than
    // calling it nondeterministic.
    let task = core.poll_workflow_activation().await.unwrap();
    core.complete_workflow_activation(WorkflowActivationCompletion::from_cmds(
        task.run_id,
        vec![
            SubscribeStream {
                stream_id: "s1".to_string(),
                start_offset: -1,
            }
            .into(),
        ],
    ))
    .await
    .unwrap();
}

/// The command has to reach the server carrying the bodies.
///
/// Only a task that is not being replayed sends commands, so this drives a
/// single open Workflow Task rather than a recorded history.
#[tokio::test]
async fn publish_command_reaches_the_server_with_its_payloads() {
    let mut t = TestHistoryBuilder::default();
    t.add_by_type(EventType::WorkflowExecutionStarted);
    t.add_workflow_task_scheduled_and_started();

    let mut mock_client = mock_worker_client();
    mock_client
        .expect_complete_workflow_task()
        .times(1)
        .returning(|resp, _| {
            let cmd = resp.commands.first().expect("a command was sent");
            assert_eq!(cmd.command_type(), CommandType::AppendStreamRecords);
            match cmd.attributes.as_ref().unwrap() {
                command::Attributes::AppendStreamRecordsCommandAttributes(a) => {
                    assert_eq!(a.stream_id, "s1");
                    // The bodies are the half of the batch History never sees,
                    // so the command is the only thing that can carry them.
                    let bodies: Vec<_> = a
                        .records
                        .iter()
                        .map(|m| m.body.as_ref().unwrap().data.clone())
                        .collect();
                    assert_eq!(bodies, vec![b"one".to_vec(), b"two".to_vec()]);
                }
                other => panic!("wrong attributes: {other:?}"),
            }
            Ok(RespondWorkflowTaskCompletedResponse::default())
        });

    let mock = MockPollCfg::from_resp_batches("wfid", t, [ResponseType::AllHistory], mock_client);
    let core = mock_worker(build_mock_pollers(mock));

    let task = core.poll_workflow_activation().await.unwrap();
    core.complete_workflow_activation(WorkflowActivationCompletion::from_cmds(
        task.run_id,
        vec![publish_two("s1").into()],
    ))
    .await
    .unwrap();
    core.shutdown().await;
}

/// A publish reissued on replay has to match the event the original run wrote.
///
/// This is the property the event exists for. Core pops one queued command per
/// command-generated event, so a publish producing none would leave every later
/// command matched against the wrong event. Core signals the mismatch by
/// failing the workflow task, which the mock turns into a panic.
#[tokio::test]
async fn publish_command_round_trips_through_replay() {
    let mut t = TestHistoryBuilder::default();
    t.add_by_type(EventType::WorkflowExecutionStarted);
    t.add_full_wf_task();
    t.add_stream_records_appended("s1", 0, 2);
    t.add_full_wf_task();

    let mut mock_client = mock_worker_client();
    mock_client
        .expect_complete_workflow_task()
        .returning(|_, _| Ok(RespondWorkflowTaskCompletedResponse::default()));
    mock_client
        .expect_fail_workflow_task()
        .returning(|_, _, f| panic!("core rejected the reissued publish: {f:?}"));

    let mock = MockPollCfg::from_resp_batches("wfid", t, [ResponseType::AllHistory], mock_client);
    let core = mock_worker(build_mock_pollers(mock));

    // First activation replays the recorded publish: lang reissues it, and core
    // has to match it to that event rather than calling it nondeterministic.
    let task = core.poll_workflow_activation().await.unwrap();
    core.complete_workflow_activation(WorkflowActivationCompletion::from_cmds(
        task.run_id,
        vec![publish_two("s1").into()],
    ))
    .await
    .unwrap();

    core.shutdown().await;
}

/// The subscribe command has to reach the server, which only a task that is
/// not being replayed will send.
#[tokio::test]
async fn subscribe_command_reaches_the_server() {
    let mut t = TestHistoryBuilder::default();
    t.add_by_type(EventType::WorkflowExecutionStarted);
    t.add_workflow_task_scheduled_and_started();

    let mut mock_client = mock_worker_client();
    mock_client
        .expect_complete_workflow_task()
        .times(1)
        .returning(|resp, _| {
            let cmd = resp.commands.first().expect("a command was sent");
            assert_eq!(cmd.command_type(), CommandType::SubscribeStream);
            match cmd.attributes.as_ref().unwrap() {
                command::Attributes::SubscribeStreamCommandAttributes(a) => {
                    assert_eq!(a.stream_id, "s1");
                    // Passed through unresolved: the server turns it into a
                    // real offset and records that.
                    assert_eq!(a.start_offset, -1);
                }
                other => panic!("wrong attributes: {other:?}"),
            }
            Ok(RespondWorkflowTaskCompletedResponse::default())
        });

    let mock = MockPollCfg::from_resp_batches("wfid", t, [ResponseType::AllHistory], mock_client);
    let core = mock_worker(build_mock_pollers(mock));

    let task = core.poll_workflow_activation().await.unwrap();
    core.complete_workflow_activation(WorkflowActivationCompletion::from_cmds(
        task.run_id,
        vec![
            SubscribeStream {
                stream_id: "s1".to_string(),
                start_offset: -1,
            }
            .into(),
        ],
    ))
    .await
    .unwrap();
    core.shutdown().await;
}

fn publish_two(stream_id: &str) -> AppendStreamRecords {
    AppendStreamRecords {
        stream_id: stream_id.to_string(),
        records: vec![
            StreamRecord {
                body: Some(b"one".to_vec().into()),
                ..Default::default()
            },
            StreamRecord {
                body: Some(b"two".to_vec().into()),
                ..Default::default()
            },
        ],
    }
}
/// A publish reissued on replay is held against the recorded event, not only
/// against its type. Core sends no commands while replaying, so this check is
/// the only place a publish to the wrong stream can be noticed.
#[tokio::test]
async fn a_publish_reissued_to_a_different_stream_fails_the_task() {
    let mut t = TestHistoryBuilder::default();
    t.add_by_type(EventType::WorkflowExecutionStarted);
    t.add_full_wf_task();
    t.add_stream_records_appended("s1", 0, 2);
    t.add_full_wf_task();

    let (core, failures) = worker_expecting_one_nondeterminism_failure(t, ResponseType::AllHistory);

    let task = core.poll_workflow_activation().await.unwrap();
    core.complete_workflow_activation(WorkflowActivationCompletion::from_cmds(
        task.run_id,
        vec![publish_two("s2").into()],
    ))
    .await
    .unwrap();
    core.handle_eviction().await;
    assert_eq!(failures.load(Ordering::Relaxed), 1);
    core.shutdown().await;
}

/// The batch size is part of the record too: the event names how many records
/// landed, so a replay that publishes fewer has diverged from the original run.
#[tokio::test]
async fn a_publish_reissued_with_a_different_batch_size_fails_the_task() {
    let mut t = TestHistoryBuilder::default();
    t.add_by_type(EventType::WorkflowExecutionStarted);
    t.add_full_wf_task();
    t.add_stream_records_appended("s1", 0, 2);
    t.add_full_wf_task();

    let (core, failures) = worker_expecting_one_nondeterminism_failure(t, ResponseType::AllHistory);

    let task = core.poll_workflow_activation().await.unwrap();
    core.complete_workflow_activation(WorkflowActivationCompletion::from_cmds(
        task.run_id,
        vec![
            AppendStreamRecords {
                stream_id: "s1".to_string(),
                records: vec![StreamRecord {
                    body: Some(b"one".to_vec().into()),
                    ..Default::default()
                }],
            }
            .into(),
        ],
    ))
    .await
    .unwrap();
    core.handle_eviction().await;
    assert_eq!(failures.load(Ordering::Relaxed), 1);
    core.shutdown().await;
}

/// A subscription is checked on the stream alone. The recorded start offset is
/// the server's resolution of what the command asked for, so it is not the
/// command's to reproduce.
#[tokio::test]
async fn a_subscribe_reissued_to_a_different_stream_fails_the_task() {
    let mut t = TestHistoryBuilder::default();
    t.add_by_type(EventType::WorkflowExecutionStarted);
    t.add_full_wf_task();
    t.add_stream_subscribed("s1", 4);
    t.add_full_wf_task();

    let (core, failures) = worker_expecting_one_nondeterminism_failure(t, ResponseType::AllHistory);

    let task = core.poll_workflow_activation().await.unwrap();
    core.complete_workflow_activation(WorkflowActivationCompletion::from_cmds(
        task.run_id,
        vec![
            SubscribeStream {
                stream_id: "s2".to_string(),
                start_offset: -1,
            }
            .into(),
        ],
    ))
    .await
    .unwrap();
    core.handle_eviction().await;
    assert_eq!(failures.load(Ordering::Relaxed), 1);
    core.shutdown().await;
}
