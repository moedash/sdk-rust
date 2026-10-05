use crate::{
    init_replay_worker,
    replay::{HistoryFeeder, HistoryForReplay, ReplayWorkerInput, TestHistoryBuilder},
    test_help::{
        MockPollCfg, ResponseType, WorkerTestHelpers, build_mock_pollers, mock_worker,
        test_worker_cfg,
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
        workflow_commands::{
            CompleteWorkflowExecution, SubscribeNotificationChannel, UnsubscribeNotificationChannel,
        },
        workflow_completion::WorkflowActivationCompletion,
    },
    temporal::api::{
        command::v1::command,
        enums::v1::{CommandType, EventType, WorkflowTaskFailedCause},
        failure::v1::Failure,
        notification::v1::Notification,
        workflowservice::v1::RespondWorkflowTaskCompletedResponse,
    },
};

fn subscribe(channel: &str) -> SubscribeNotificationChannel {
    SubscribeNotificationChannel {
        channel: channel.to_string(),
    }
}

fn unsubscribe(channel: &str) -> UnsubscribeNotificationChannel {
    UnsubscribeNotificationChannel {
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

/// The unsubscribe is an ordinary command: it has to reach the server naming
/// the channel, from a task that is not being replayed.
#[tokio::test]
async fn unsubscribe_channel_command_reaches_the_server() {
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
                CommandType::UnsubscribeNotificationChannel
            );
            match cmd.attributes.as_ref().unwrap() {
                command::Attributes::UnsubscribeNotificationChannelCommandAttributes(a) => {
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
        vec![unsubscribe("orders").into()],
    ))
    .await
    .unwrap();
    core.shutdown().await;
}

/// A subscribe on one task and an unsubscribe on the next each match their own
/// event on replay. The unsubscribed event names the subscribed event it ended,
/// which is the server's record and not part of what the command is held to.
/// The second task is the one a notification scheduled, since a task with
/// nothing in it gets no activation of its own.
#[tokio::test]
async fn subscribe_then_unsubscribe_round_trips_through_replay() {
    let mut t = TestHistoryBuilder::default();
    t.add_by_type(EventType::WorkflowExecutionStarted);
    t.add_full_wf_task();
    let subscribed_id = t.add_notification_channel_subscribed("orders");
    t.add_workflow_task_scheduled_with_notifications(vec![notification("orders", 1)]);
    t.add_workflow_task_started();
    t.add_workflow_task_completed();
    t.add_notification_channel_unsubscribed("orders", subscribed_id);
    t.add_we_signaled("done", vec![]);
    t.add_workflow_task_scheduled_and_started();

    let mut mock_client = mock_worker_client();
    mock_client
        .expect_complete_workflow_task()
        .returning(|_, _| Ok(RespondWorkflowTaskCompletedResponse::default()));
    mock_client
        .expect_fail_workflow_task()
        .returning(|_, _, f| panic!("core rejected a reissued channel command: {f:?}"));

    let mock = MockPollCfg::from_resp_batches("wfid", t, [ResponseType::AllHistory], mock_client);
    let core = mock_worker(build_mock_pollers(mock));

    let task = core.poll_workflow_activation().await.unwrap();
    assert!(task.is_replaying);
    core.complete_workflow_activation(WorkflowActivationCompletion::from_cmds(
        task.run_id,
        vec![subscribe("orders").into()],
    ))
    .await
    .unwrap();

    let task = core.poll_workflow_activation().await.unwrap();
    assert!(task.is_replaying);
    assert_eq!(received(&task), vec![vec![notification("orders", 1)]]);
    core.complete_workflow_activation(WorkflowActivationCompletion::from_cmds(
        task.run_id,
        vec![unsubscribe("orders").into()],
    ))
    .await
    .unwrap();

    let task = core.poll_workflow_activation().await.unwrap();
    assert!(!task.is_replaying);
    core.complete_workflow_activation(WorkflowActivationCompletion::empty(task.run_id))
        .await
        .unwrap();
    core.shutdown().await;
}

/// The recorded event holds the reissued unsubscribe to its channel, the same
/// way the subscribed event holds a subscribe.
#[tokio::test]
async fn an_unsubscribe_reissued_for_a_different_channel_fails_the_task() {
    let mut t = TestHistoryBuilder::default();
    t.add_by_type(EventType::WorkflowExecutionStarted);
    t.add_full_wf_task();
    t.add_notification_channel_unsubscribed("orders", 0);
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
        vec![unsubscribe("invoices").into()],
    ))
    .await
    .unwrap();
    core.handle_eviction().await;
    assert_eq!(failures.load(Ordering::Relaxed), 1);
    core.shutdown().await;
}

/// Core hands over whatever the scheduled events carry and keeps no view of
/// the run's subscriptions. The notification on the task that unsubscribes
/// reaches that task, and one the server put on a later scheduled event still
/// becomes a job: dropping it for a closed subscription is lang's call. The
/// unsubscribed event here records no subscription, which is what the server
/// writes for a channel the run never subscribed to.
#[tokio::test]
async fn an_unsubscribe_leaves_the_notification_jobs_alone() {
    let mut t = TestHistoryBuilder::default();
    t.add_by_type(EventType::WorkflowExecutionStarted);
    t.add_workflow_task_scheduled_with_notifications(vec![notification("orders", 1)]);
    t.add_workflow_task_started();
    t.add_workflow_task_completed();
    t.add_notification_channel_unsubscribed("orders", 0);
    t.add_workflow_task_scheduled_with_notifications(vec![notification("orders", 2)]);
    t.add_workflow_task_started();

    let mut mock_client = mock_worker_client();
    mock_client
        .expect_fail_workflow_task()
        .returning(|_, _, f| panic!("core rejected the reissued unsubscribe: {f:?}"));
    let mock = MockPollCfg::from_resp_batches("wfid", t, [ResponseType::AllHistory], mock_client);
    let core = mock_worker(build_mock_pollers(mock));

    let task = core.poll_workflow_activation().await.unwrap();
    assert!(task.is_replaying);
    assert_eq!(received(&task), vec![vec![notification("orders", 1)]]);
    core.complete_workflow_activation(WorkflowActivationCompletion::from_cmds(
        task.run_id,
        vec![unsubscribe("orders").into()],
    ))
    .await
    .unwrap();

    let task = core.poll_workflow_activation().await.unwrap();
    assert!(!task.is_replaying);
    assert_eq!(job_kinds(&task), vec!["notifications"]);
    assert_eq!(received(&task), vec![vec![notification("orders", 2)]]);
    core.complete_workflow_activation(WorkflowActivationCompletion::empty(task.run_id))
        .await
        .unwrap();
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

/// The first activation a replay worker makes of a history that ends with the run
/// completing, and that worker, so the test can finish the run on it.
async fn replayed_first_activation(
    mut t: TestHistoryBuilder,
) -> (crate::Worker, HistoryFeeder, WorkflowActivation) {
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
    (core, feeder, task)
}

/// The first activation of a history served live, as one poll response.
async fn live_first_activation(t: TestHistoryBuilder) -> WorkflowActivation {
    let mock =
        MockPollCfg::from_resp_batches("wfid", t, [ResponseType::AllHistory], mock_worker_client());
    let core = mock_worker(build_mock_pollers(mock));
    let task = core.poll_workflow_activation().await.unwrap();
    core.complete_workflow_activation(WorkflowActivationCompletion::empty(task.run_id.clone()))
        .await
        .unwrap();
    task
}

/// A first task that failed, scheduled with one notification on channel A, and its
/// retry, scheduled with a newer one on A and one on B.
fn failed_task_then_retry() -> TestHistoryBuilder {
    let mut t = TestHistoryBuilder::default();
    t.add_by_type(EventType::WorkflowExecutionStarted);
    t.add_workflow_task_scheduled_with_notifications(vec![notification("a", 1)]);
    t.add_workflow_task_started();
    t.add_workflow_task_failed_with_failure(
        WorkflowTaskFailedCause::Unspecified,
        Failure::default(),
    );
    t.add_workflow_task_scheduled_with_notifications(vec![
        notification("a", 5),
        notification("b", 2),
    ]);
    t.add_workflow_task_started();
    t
}

/// The server clears what it put on a scheduled event, so the failed task's
/// notification is only in History. The retry's activation gets it folded with
/// the retry's own, one per channel, live and on replay alike.
#[tokio::test]
async fn a_failed_task_and_its_retry_yield_one_job_folded_per_channel() {
    let expected = vec![vec![notification("a", 5), notification("b", 2)]];

    let task = live_first_activation(failed_task_then_retry()).await;
    assert!(!task.is_replaying);
    assert_eq!(job_kinds(&task), vec!["init", "notifications"]);
    assert_eq!(received(&task), expected);

    let (core, feeder, task) = replayed_first_activation(failed_task_then_retry()).await;
    assert!(task.is_replaying);
    assert_eq!(job_kinds(&task), vec!["init", "notifications"]);
    assert_eq!(received(&task), expected);
    core.complete_workflow_activation(WorkflowActivationCompletion::from_cmds(
        task.run_id,
        vec![CompleteWorkflowExecution { result: None }.into()],
    ))
    .await
    .unwrap();
    drop(feeder);
    core.shutdown().await;
}

/// A notification with a lower counter than the one already held for its channel
/// does not replace it, and channels keep the order they first appeared in.
#[tokio::test]
async fn a_lower_counter_does_not_replace_the_held_notification() {
    let mut t = TestHistoryBuilder::default();
    t.add_by_type(EventType::WorkflowExecutionStarted);
    t.add_workflow_task_scheduled_with_notifications(vec![notification("a", 5)]);
    t.add_workflow_task_started();
    t.add_workflow_task_timed_out();
    t.add_workflow_task_scheduled_with_notifications(vec![
        notification("b", 1),
        notification("a", 3),
    ]);
    t.add_workflow_task_started();

    let task = live_first_activation(t).await;
    assert_eq!(
        received(&task),
        vec![vec![notification("a", 5), notification("b", 1)]]
    );
}

/// On a counter tie the notification already held for the channel stays.
#[tokio::test]
async fn a_counter_tie_keeps_the_held_notification() {
    let held = Notification {
        position: b"held".to_vec(),
        ..notification("a", 4)
    };
    let tied = Notification {
        position: b"tied".to_vec(),
        ..notification("a", 4)
    };
    let mut t = TestHistoryBuilder::default();
    t.add_by_type(EventType::WorkflowExecutionStarted);
    t.add_workflow_task_scheduled_with_notifications(vec![held.clone()]);
    t.add_workflow_task_started();
    t.add_workflow_task_timed_out();
    t.add_workflow_task_scheduled_with_notifications(vec![tied]);
    t.add_workflow_task_started();

    let task = live_first_activation(t).await;
    assert_eq!(received(&task), vec![vec![held]]);
}

/// The failed attempt is a later task, so a sticky worker reaches it in an incremental history
/// update together with its retry, and a cold one replays the first task before it. Either way
/// the retry's activation holds the fold of both attempts' notifications and the first holds none.
#[rstest::rstest]
#[case::incremental(vec![ResponseType::ToTaskNum(1), ResponseType::ToTaskNum(2)])]
#[case::cold(vec![ResponseType::AllHistory])]
#[tokio::test]
async fn a_failed_later_task_hands_its_notifications_to_the_retry(
    #[case] batches: Vec<ResponseType>,
) {
    let mut t = TestHistoryBuilder::default();
    t.add_by_type(EventType::WorkflowExecutionStarted);
    t.add_full_wf_task();
    t.add_workflow_task_scheduled_with_notifications(vec![notification("a", 1)]);
    t.add_workflow_task_started();
    t.add_workflow_task_failed_with_failure(
        WorkflowTaskFailedCause::Unspecified,
        Failure::default(),
    );
    t.add_workflow_task_scheduled_with_notifications(vec![
        notification("a", 5),
        notification("b", 2),
    ]);
    t.add_workflow_task_started();

    let mut mock = build_mock_pollers(MockPollCfg::from_resp_batches(
        "wfid",
        t,
        batches,
        mock_worker_client(),
    ));
    mock.worker_cfg(|w| w.max_cached_workflows = 1);
    let core = mock_worker(mock);

    let task = core.poll_workflow_activation().await.unwrap();
    assert!(received(&task).is_empty(), "got jobs {:?}", task.jobs);
    core.complete_workflow_activation(WorkflowActivationCompletion::empty(task.run_id))
        .await
        .unwrap();

    let task = core.poll_workflow_activation().await.unwrap();
    assert!(!task.is_replaying);
    assert_eq!(
        received(&task),
        vec![vec![notification("a", 5), notification("b", 2)]]
    );
    core.complete_workflow_activation(WorkflowActivationCompletion::from_cmds(
        task.run_id,
        vec![CompleteWorkflowExecution { result: None }.into()],
    ))
    .await
    .unwrap();
    core.shutdown().await;
}
