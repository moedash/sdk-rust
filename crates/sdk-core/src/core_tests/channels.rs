use crate::{
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
        workflow_commands::SubscribeNotificationChannel,
        workflow_completion::WorkflowActivationCompletion,
    },
    temporal::api::{
        command::v1::command,
        enums::v1::{CommandType, EventType, WorkflowTaskFailedCause},
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
