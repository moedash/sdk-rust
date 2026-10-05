use super::{
    HistEventData, Machines, OnEventWrapper, StateMachine, WFMachinesError,
    subscribe_notification_channel_state_machine::*,
};
use crate::{
    replay::TestHistoryBuilder,
    test_help::{
        MockPollCfg, ResponseType, WorkerExt, build_mock_pollers, hist_to_poll_resp, mock_worker,
        start_timer_cmd,
    },
    worker::client::mocks::mock_worker_client,
};
use parking_lot::Mutex;
use std::{sync::Arc, time::Duration};
use temporalio_common::{
    protos::{
        coresdk::{
            workflow_commands::{
                CompleteWorkflowExecution, SubscribeNotificationChannel, workflow_command,
            },
            workflow_completion::WorkflowActivationCompletion,
        },
        temporal::api::{
            command::v1::command,
            enums::v1::{CommandType, EventType, WorkflowTaskFailedCause},
            history::v1::{
                HistoryEvent, WorkflowNotificationChannelSubscribedEventAttributes, history_event,
            },
        },
    },
    worker::WorkerTaskTypes,
};

fn subscribe(channel: &str) -> workflow_command::Variant {
    SubscribeNotificationChannel {
        channel: channel.to_string(),
    }
    .into()
}

fn recorded(channel: &str) -> SubscribeNotificationChannelMachineEvents {
    HistEventData {
        event: HistoryEvent {
            event_type: EventType::WorkflowNotificationChannelSubscribed as i32,
            attributes: Some(
                history_event::Attributes::WorkflowNotificationChannelSubscribedEventAttributes(
                    WorkflowNotificationChannelSubscribedEventAttributes {
                        workflow_task_completed_event_id: 0,
                        channel: channel.to_string(),
                    },
                ),
            ),
            ..Default::default()
        },
        replaying: true,
        current_task_is_last_in_history: false,
    }
    .try_into()
    .unwrap()
}

fn issued(channel: &str) -> SubscribeNotificationChannelMachine {
    let Machines::SubscribeNotificationChannelMachine(mut sm) =
        subscribe_notification_channel(SubscribeNotificationChannel {
            channel: channel.to_string(),
        })
        .machine
    else {
        unreachable!("the constructor builds a SubscribeNotificationChannelMachine");
    };
    OnEventWrapper::on_event_mut(
        &mut sm,
        CommandType::SubscribeNotificationChannel
            .try_into()
            .unwrap(),
    )
    .expect("CommandScheduled should transition Created -> CommandIssued");
    assert_eq!(CommandIssued {}.to_string(), sm.state().to_string());
    sm
}

#[test]
fn the_recorded_event_for_the_same_channel_completes_the_machine() {
    let mut sm = issued("orders");
    OnEventWrapper::on_event_mut(&mut sm, recorded("orders"))
        .expect("CommandRecorded should transition CommandIssued -> Done");
    assert_eq!(Done {}.to_string(), sm.state().to_string());
}

#[test]
fn a_recorded_event_for_another_channel_is_nondeterminism() {
    let mut sm = issued("orders");
    let err = OnEventWrapper::on_event_mut(&mut sm, recorded("invoices"))
        .expect_err("a different channel must not match");
    let message = format!("{err:?}");
    assert!(
        message.contains("does not match") && message.contains("invoices"),
        "the error must name both channels, got {message}"
    );
}

#[test]
fn another_event_type_is_nondeterminism() {
    let event = HistEventData {
        event: HistoryEvent {
            event_type: EventType::TimerStarted as i32,
            ..Default::default()
        },
        replaying: true,
        current_task_is_last_in_history: false,
    };
    let res: Result<SubscribeNotificationChannelMachineEvents, _> = event.try_into();
    assert!(matches!(res, Err(WFMachinesError::Nondeterminism(_))));
}

#[tokio::test]
async fn the_command_reaches_the_completion_with_its_channel() {
    let mut t = TestHistoryBuilder::default();
    t.add_by_type(EventType::WorkflowExecutionStarted);
    t.add_workflow_task_scheduled_and_started();

    let sent = Arc::new(Mutex::new(vec![]));
    let recorder = sent.clone();
    let mut cfg = MockPollCfg::from_resp_batches("fakeid", t, [1], mock_worker_client());
    cfg.completion_mock_fn = Some(Box::new(move |wftc| {
        recorder.lock().extend(wftc.commands.iter().cloned());
        Ok(Default::default())
    }));
    let mut mock = build_mock_pollers(cfg);
    mock.worker_cfg(|w| {
        w.task_types = WorkerTaskTypes::workflow_only();
        w.max_cached_workflows = 1;
    });
    let worker = mock_worker(mock);

    let first = worker.poll_workflow_activation().await.unwrap();
    assert!(!first.is_replaying);
    worker
        .complete_workflow_activation(WorkflowActivationCompletion::from_cmd(
            first.run_id,
            subscribe("orders"),
        ))
        .await
        .unwrap();

    let commands = sent.lock().clone();
    assert_eq!(commands.len(), 1, "got {commands:?}");
    assert_eq!(
        commands[0].command_type(),
        CommandType::SubscribeNotificationChannel
    );
    assert!(
        matches!(
            &commands[0].attributes,
            Some(command::Attributes::SubscribeNotificationChannelCommandAttributes(a))
                if a.channel == "orders"
        ),
        "got {:?}",
        commands[0]
    );

    worker.drain_pollers_and_shutdown().await;
}

/// Replays a first task whose recorded command event `record` writes, with lang reissuing
/// `reissued`. Returns the nondeterminism message Core reported, if any, and whether lang was
/// activated for the live task after it.
async fn replay_first_task(
    record: impl FnOnce(&mut TestHistoryBuilder),
    reissued: workflow_command::Variant,
    expect_failure: bool,
) -> (Option<String>, bool) {
    let mut t = TestHistoryBuilder::default();
    t.add_by_type(EventType::WorkflowExecutionStarted);
    t.add_full_wf_task();
    record(&mut t);
    // Gives the live task a job, so a match shows up as an activation rather than as silence.
    t.add_we_signaled("go", vec![]);
    t.add_workflow_task_scheduled_and_started();

    let cold = hist_to_poll_resp(&t, "fakeid".to_owned(), ResponseType::AllHistory);
    let mut cfg = MockPollCfg::from_resp_batches(
        "fakeid",
        t,
        [ResponseType::Raw(cold.resp)],
        mock_worker_client(),
    );
    let failure = Arc::new(Mutex::new(None));
    if expect_failure {
        cfg.num_expected_fails = 1;
        let recorder = failure.clone();
        cfg.expect_fail_wft_matcher = Box::new(move |_, cause, f| {
            if matches!(cause, WorkflowTaskFailedCause::NonDeterministicError) {
                *recorder.lock() = Some(f.as_ref().map(|f| f.message.clone()).unwrap_or_default());
            }
            true
        });
    }
    let mut mock = build_mock_pollers(cfg);
    mock.worker_cfg(|w| {
        w.task_types = WorkerTaskTypes::workflow_only();
        w.max_cached_workflows = 1;
    });
    let worker = mock_worker(mock);

    let first = worker.poll_workflow_activation().await.unwrap();
    assert!(first.is_replaying);
    worker
        .complete_workflow_activation(WorkflowActivationCompletion::from_cmd(
            first.run_id.clone(),
            reissued,
        ))
        .await
        .unwrap();

    let next = tokio::time::timeout(
        Duration::from_millis(500),
        worker.poll_workflow_activation(),
    )
    .await;
    let mut activated = false;
    if let Ok(Ok(act)) = next {
        if act.is_only_eviction() {
            worker
                .complete_workflow_activation(WorkflowActivationCompletion::empty(act.run_id))
                .await
                .unwrap();
        } else {
            assert!(
                !expect_failure,
                "a mismatch must not activate lang, got {act:?}"
            );
            activated = true;
            worker
                .complete_workflow_activation(WorkflowActivationCompletion::from_cmd(
                    act.run_id,
                    CompleteWorkflowExecution::default().into(),
                ))
                .await
                .unwrap();
        }
    }
    worker.drain_pollers_and_shutdown().await;
    let failure = failure.lock().clone();
    (failure, activated)
}

#[tokio::test]
async fn replay_matches_the_recorded_subscription() {
    let (failure, activated) = replay_first_task(
        |t| {
            t.add_notification_channel_subscribed("orders");
        },
        subscribe("orders"),
        false,
    )
    .await;
    assert_eq!(failure, None);
    assert!(
        activated,
        "the live task after a matched subscription must activate lang"
    );
}

#[tokio::test]
async fn replay_with_another_channel_is_nondeterminism() {
    let failure = replay_first_task(
        |t| {
            t.add_notification_channel_subscribed("invoices");
        },
        subscribe("orders"),
        true,
    )
    .await
    .0
    .expect("the task must fail as nondeterminism");
    assert!(failure.contains("does not match"), "got {failure}");
}

#[tokio::test]
async fn replay_without_the_subscribed_event_is_nondeterminism() {
    let failure = replay_first_task(
        |t| {
            t.add_timer_started("1".to_string());
        },
        subscribe("orders"),
        true,
    )
    .await
    .0
    .expect("the task must fail as nondeterminism");
    assert!(
        failure.contains("SubscribeNotificationChannelMachine does not handle"),
        "got {failure}"
    );
}

#[tokio::test]
async fn a_recorded_subscription_lang_does_not_reissue_is_nondeterminism() {
    let failure = replay_first_task(
        |t| {
            t.add_notification_channel_subscribed("orders");
        },
        start_timer_cmd(1, Duration::from_secs(1)),
        true,
    )
    .await
    .0
    .expect("the task must fail as nondeterminism");
    assert!(!failure.is_empty());
}
