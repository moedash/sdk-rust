use crate::{
    init_replay_worker,
    replay::{HistoryFeeder, HistoryForReplay, ReplayWorkerInput, TestHistoryBuilder},
    test_help::{
        MockPollCfg, PollWFTRespExt, ResponseType, WorkerTestHelpers, build_mock_pollers,
        hist_to_poll_resp, mock_worker, test_worker_cfg,
    },
    worker::client::mocks::mock_worker_client,
};
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};
use temporalio_common::protos::{
    coresdk::{
        workflow_activation::{WorkflowActivation, workflow_activation_job},
        workflow_commands::{CompleteWorkflowExecution, SubscribeNotificationChannel},
        workflow_completion::WorkflowActivationCompletion,
    },
    temporal::api::{
        command::v1::command,
        enums::v1::{CommandType, EventType, WorkflowTaskFailedCause},
        notification::v1::Notification,
        workflowservice::v1::RespondWorkflowTaskCompletedResponse,
    },
};

fn subscribe(channel: &str) -> SubscribeNotificationChannel {
    SubscribeNotificationChannel {
        channel: channel.to_string(),
    }
}

/// The command has to reach the server naming the channel. Only a task that is
/// not being replayed sends commands, so this drives a single open task.
#[tokio::test]
async fn subscribe_channel_command_reaches_the_server() {
    let mut t = TestHistoryBuilder::default();
    t.add_by_type(EventType::WorkflowExecutionStarted);
    t.add_workflow_task_scheduled_and_started();

    let mut mock_client = mock_worker_client();
    mock_client
        .expect_complete_workflow_task()
        .times(1)
        .returning(|resp, _| {
            let cmd = resp.commands.first().expect("a command was sent");
            assert_eq!(
                cmd.command_type(),
                CommandType::SubscribeNotificationChannel
            );
            match cmd.attributes.as_ref().unwrap() {
                command::Attributes::SubscribeNotificationChannelCommandAttributes(a) => {
                    assert_eq!(a.channel, "orders");
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
        vec![subscribe("orders").into()],
    ))
    .await
    .unwrap();
    core.shutdown().await;
}

/// A subscribe reissued on replay has to match the event the original run
/// wrote. Core pops one queued command per command-generated event, so this is
/// what keeps every later command lined up with its own event.
#[tokio::test]
async fn subscribe_channel_command_round_trips_through_replay() {
    let mut t = TestHistoryBuilder::default();
    t.add_by_type(EventType::WorkflowExecutionStarted);
    t.add_full_wf_task();
    t.add_notification_channel_subscribed("orders");
    t.add_full_wf_task();

    let mut mock_client = mock_worker_client();
    mock_client
        .expect_complete_workflow_task()
        .returning(|_, _| Ok(RespondWorkflowTaskCompletedResponse::default()));
    mock_client
        .expect_fail_workflow_task()
        .returning(|_, _, f| panic!("core rejected the reissued subscribe: {f:?}"));

    let mock = MockPollCfg::from_resp_batches("wfid", t, [ResponseType::AllHistory], mock_client);
    let core = mock_worker(build_mock_pollers(mock));

    let task = core.poll_workflow_activation().await.unwrap();
    core.complete_workflow_activation(WorkflowActivationCompletion::from_cmds(
        task.run_id,
        vec![subscribe("orders").into()],
    ))
    .await
    .unwrap();
    core.shutdown().await;
}

/// The channel is part of what the recorded event holds the reissued command
/// to. Core sends no commands while replaying, so this check is the only place
/// a subscribe to the wrong channel can be noticed.
#[tokio::test]
async fn a_subscribe_reissued_to_a_different_channel_fails_the_task() {
    let mut t = TestHistoryBuilder::default();
    t.add_by_type(EventType::WorkflowExecutionStarted);
    t.add_full_wf_task();
    t.add_notification_channel_subscribed("orders");
    t.add_full_wf_task();

    let failures = Arc::new(AtomicUsize::new(0));
    let counted = failures.clone();
    let mut mock =
        MockPollCfg::from_resp_batches("wfid", t, [ResponseType::AllHistory], mock_worker_client());
    mock.num_expected_fails = 1;
    mock.expect_fail_wft_matcher = Box::new(move |_, cause, _| {
        counted.fetch_add(1, Ordering::Relaxed);
        *cause == WorkflowTaskFailedCause::NonDeterministicError
    });
    let mut mock = build_mock_pollers(mock);
    mock.make_wft_stream_interminable();
    let core = mock_worker(mock);

    let task = core.poll_workflow_activation().await.unwrap();
    core.complete_workflow_activation(WorkflowActivationCompletion::from_cmds(
        task.run_id,
        vec![subscribe("invoices").into()],
    ))
    .await
    .unwrap();
    core.handle_eviction().await;
    assert_eq!(failures.load(Ordering::Relaxed), 1);
    core.shutdown().await;
}

fn notification(channel: &str, counter: i64) -> Notification {
    Notification {
        channel: channel.to_string(),
        position: format!("pos-{counter}").into_bytes(),
        counter,
        ..Default::default()
    }
}

/// The notification jobs in an activation, each as the notifications it carries.
fn received(task: &WorkflowActivation) -> Vec<Vec<Notification>> {
    task.jobs
        .iter()
        .filter_map(|j| match j.variant.as_ref() {
            Some(workflow_activation_job::Variant::NotificationsReceived(n)) => {
                Some(n.notifications.clone())
            }
            _ => None,
        })
        .collect()
}

/// A short name for each job, so a test can assert their order.
fn job_kinds(task: &WorkflowActivation) -> Vec<&'static str> {
    task.jobs
        .iter()
        .map(|j| match j.variant.as_ref().unwrap() {
            workflow_activation_job::Variant::InitializeWorkflow(_) => "init",
            workflow_activation_job::Variant::NotificationsReceived(_) => "notifications",
            workflow_activation_job::Variant::DeliverStreamRecords(_) => "stream",
            _ => "other",
        })
        .collect()
}

/// A run whose only task was scheduled with notifications for two channels.
fn one_task_with_notifications() -> TestHistoryBuilder {
    let mut t = TestHistoryBuilder::default();
    t.add_by_type(EventType::WorkflowExecutionStarted);
    t.add_workflow_task_scheduled_with_notifications(vec![
        notification("orders", 3),
        notification("invoices", 7),
    ]);
    t.add_workflow_task_started();
    t
}

/// The notifications on the scheduled event reach the activation of that task,
/// as one job, ahead of the stream ranges the same task was handed.
#[tokio::test]
async fn notifications_on_the_scheduled_event_reach_the_live_activation() {
    let t = one_task_with_notifications();
    let mut poll_resp = hist_to_poll_resp(&t, "wfid".to_owned(), ResponseType::AllHistory);
    poll_resp.add_stream_slice("s1", 0, 0, &["alpha"]);

    let mock = MockPollCfg::from_resp_batches(
        "wfid",
        t,
        [ResponseType::Raw(poll_resp.resp)],
        mock_worker_client(),
    );
    let core = mock_worker(build_mock_pollers(mock));

    let task = core.poll_workflow_activation().await.unwrap();
    assert!(!task.is_replaying);
    assert_eq!(job_kinds(&task), vec!["init", "notifications", "stream"]);
    assert_eq!(
        received(&task),
        vec![vec![notification("orders", 3), notification("invoices", 7)]]
    );
    core.complete_workflow_activation(WorkflowActivationCompletion::empty(task.run_id))
        .await
        .unwrap();
}

/// Replaying the same history yields the same job. Nothing is re-supplied: the
/// notifications are on the event, so History alone carries them.
#[tokio::test]
async fn a_replayed_history_yields_the_same_notifications() {
    let mut t = one_task_with_notifications();
    t.add_workflow_task_completed();
    t.add_workflow_execution_completed();

    let (feeder, stream) = HistoryFeeder::new(1);
    feeder
        .feed(HistoryForReplay::new(
            t.get_full_history_info().unwrap(),
            "wfid",
        ))
        .await
        .unwrap();
    let core = init_replay_worker(ReplayWorkerInput::new(
        test_worker_cfg().build().unwrap(),
        stream,
    ))
    .unwrap();

    let task = core.poll_workflow_activation().await.unwrap();
    assert!(task.is_replaying);
    assert_eq!(job_kinds(&task), vec!["init", "notifications"]);
    assert_eq!(
        received(&task),
        vec![vec![notification("orders", 3), notification("invoices", 7)]]
    );
    core.complete_workflow_activation(WorkflowActivationCompletion::from_cmds(
        task.run_id,
        vec![CompleteWorkflowExecution { result: None }.into()],
    ))
    .await
    .unwrap();
    drop(feeder);
    core.shutdown().await;
}

/// On a later task the job lands in that task's activation and not in an
/// earlier one, also when the worker replays its way there. The first task
/// issued no command, which replay would otherwise read as a heartbeat and
/// fold together with the next one.
#[tokio::test]
async fn notifications_belong_to_the_task_whose_scheduled_event_carries_them() {
    let mut t = TestHistoryBuilder::default();
    t.add_by_type(EventType::WorkflowExecutionStarted);
    t.add_full_wf_task();
    t.add_workflow_task_scheduled_with_notifications(vec![notification("orders", 1)]);
    t.add_workflow_task_started();
    t.add_workflow_task_completed();
    t.add_we_signaled("sig", vec![]);
    t.add_workflow_task_scheduled_and_started();

    let mock =
        MockPollCfg::from_resp_batches("wfid", t, [ResponseType::AllHistory], mock_worker_client());
    let core = mock_worker(build_mock_pollers(mock));

    let task = core.poll_workflow_activation().await.unwrap();
    assert!(received(&task).is_empty());
    core.complete_workflow_activation(WorkflowActivationCompletion::empty(task.run_id))
        .await
        .unwrap();

    let task = core.poll_workflow_activation().await.unwrap();
    assert!(task.is_replaying);
    assert_eq!(received(&task), vec![vec![notification("orders", 1)]]);
    core.complete_workflow_activation(WorkflowActivationCompletion::empty(task.run_id))
        .await
        .unwrap();

    let task = core.poll_workflow_activation().await.unwrap();
    assert!(received(&task).is_empty());
    core.complete_workflow_activation(WorkflowActivationCompletion::empty(task.run_id))
        .await
        .unwrap();
}

/// A scheduled event without notifications produces no job, not an empty one.
#[tokio::test]
async fn a_scheduled_event_without_notifications_yields_no_job() {
    let mut t = TestHistoryBuilder::default();
    t.add_by_type(EventType::WorkflowExecutionStarted);
    t.add_workflow_task_scheduled_and_started();

    let mock =
        MockPollCfg::from_resp_batches("wfid", t, [ResponseType::AllHistory], mock_worker_client());
    let core = mock_worker(build_mock_pollers(mock));

    let task = core.poll_workflow_activation().await.unwrap();
    assert_eq!(job_kinds(&task), vec!["init"]);
    core.complete_workflow_activation(WorkflowActivationCompletion::empty(task.run_id))
        .await
        .unwrap();
}
