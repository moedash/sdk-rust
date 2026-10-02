use super::{
    NewMachineWithCommand, StateMachine, TransitionResult, fsm, workflow_machines::MachineResponse,
};
use crate::worker::workflow::{
    WFMachinesError, fatal,
    machines::{EventInfo, HistEventData, WFMachinesAdapter},
    nondeterminism,
};
use temporalio_common::protos::{
    coresdk::workflow_commands::UnsubscribeNotificationChannel,
    temporal::api::{
        enums::v1::{CommandType, EventType},
        history::v1::{WorkflowNotificationChannelUnsubscribedEventAttributes, history_event},
    },
};

fsm! {
    pub(super) name UnsubscribeNotificationChannelMachine;
    command UnsubscribeNotificationChannelMachineCommand;
    error WFMachinesError;
    shared_state SharedState;

    Created --(CommandScheduled) --> CommandIssued;
    CommandIssued --(CommandRecorded(WorkflowNotificationChannelUnsubscribedEventAttributes),
        shared on_command_recorded) --> Done;
}

/// The channel the command named, kept so the recorded event can be held against it on replay.
#[derive(Default, Clone)]
pub(super) struct SharedState {
    channel: String,
}

/// End this run's subscription to a notification channel. The server records the event whether
/// or not the run held a subscription, so every command has an event to match on replay.
pub(super) fn unsubscribe_notification_channel(
    lang_cmd: UnsubscribeNotificationChannel,
) -> NewMachineWithCommand {
    let sm = UnsubscribeNotificationChannelMachine::from_parts(
        Created {}.into(),
        SharedState {
            channel: lang_cmd.channel.clone(),
        },
    );
    NewMachineWithCommand {
        command: lang_cmd.into(),
        machine: sm.into(),
    }
}

#[derive(Debug, derive_more::Display)]
pub(super) enum UnsubscribeNotificationChannelMachineCommand {}

#[derive(Debug, Default, Clone, derive_more::Display)]
pub(super) struct Created {}

#[derive(Debug, Default, Clone, derive_more::Display)]
pub(super) struct CommandIssued {}

impl CommandIssued {
    pub(super) fn on_command_recorded(
        self,
        dat: &mut SharedState,
        attrs: WorkflowNotificationChannelUnsubscribedEventAttributes,
    ) -> UnsubscribeNotificationChannelMachineTransition<Done> {
        if dat.channel == attrs.channel {
            TransitionResult::default()
        } else {
            TransitionResult::Err(nondeterminism!(
                "Recorded unsubscription from notification channel {:?} does not match the \
                 reissued unsubscription from channel {:?}",
                attrs.channel,
                dat.channel
            ))
        }
    }
}

#[derive(Debug, Default, Clone, derive_more::Display)]
pub(super) struct Done {}

impl WFMachinesAdapter for UnsubscribeNotificationChannelMachine {
    fn adapt_response(
        &self,
        _my_command: Self::Command,
        _event_info: Option<EventInfo>,
    ) -> Result<Vec<MachineResponse>, Self::Error> {
        Err(fatal!(
            "UnsubscribeNotificationChannel does not use state machine commands"
        ))
    }
}

impl TryFrom<HistEventData> for UnsubscribeNotificationChannelMachineEvents {
    type Error = WFMachinesError;

    fn try_from(e: HistEventData) -> Result<Self, Self::Error> {
        let e = e.event;
        match e.event_type() {
            EventType::WorkflowNotificationChannelUnsubscribed => {
                if let Some(
                    history_event::Attributes::WorkflowNotificationChannelUnsubscribedEventAttributes(
                        attrs,
                    ),
                ) = e.attributes
                {
                    Ok(UnsubscribeNotificationChannelMachineEvents::CommandRecorded(
                        attrs,
                    ))
                } else {
                    Err(fatal!(
                        "Notification channel unsubscribed attributes were unset: {e}"
                    ))
                }
            }
            _ => Err(Self::Error::Nondeterminism(format!(
                "UnsubscribeNotificationChannelMachine does not handle {e}"
            ))),
        }
    }
}

impl TryFrom<CommandType> for UnsubscribeNotificationChannelMachineEvents {
    type Error = WFMachinesError;

    fn try_from(c: CommandType) -> Result<Self, Self::Error> {
        match c {
            CommandType::UnsubscribeNotificationChannel => {
                Ok(UnsubscribeNotificationChannelMachineEvents::CommandScheduled)
            }
            _ => Err(Self::Error::Nondeterminism(format!(
                "UnsubscribeNotificationChannelMachine does not handle command type {c:?}"
            ))),
        }
    }
}

impl From<Created> for CommandIssued {
    fn from(_: Created) -> Self {
        Self {}
    }
}

#[cfg(test)]
mod tests {
    use super::{super::OnEventWrapper, *};
    use crate::{
        replay::TestHistoryBuilder,
        test_help::{
            MockPollCfg, ResponseType, WorkerExt, build_mock_pollers, hist_to_poll_resp,
            mock_worker, start_timer_cmd,
        },
        worker::client::mocks::mock_worker_client,
    };
    use parking_lot::Mutex;
    use std::{sync::Arc, time::Duration};
    use temporalio_common::{
        protos::{
            coresdk::{
                workflow_commands::{CompleteWorkflowExecution, workflow_command},
                workflow_completion::WorkflowActivationCompletion,
            },
            temporal::api::{
                command::v1::command, enums::v1::WorkflowTaskFailedCause, history::v1::HistoryEvent,
            },
        },
        worker::WorkerTaskTypes,
    };

    fn unsubscribe(channel: &str) -> workflow_command::Variant {
        UnsubscribeNotificationChannel {
            channel: channel.to_string(),
        }
        .into()
    }

    fn recorded(channel: &str) -> UnsubscribeNotificationChannelMachineEvents {
        HistEventData {
            event: HistoryEvent {
                event_type: EventType::WorkflowNotificationChannelUnsubscribed as i32,
                attributes: Some(
                    history_event::Attributes::WorkflowNotificationChannelUnsubscribedEventAttributes(
                        WorkflowNotificationChannelUnsubscribedEventAttributes {
                            workflow_task_completed_event_id: 0,
                            channel: channel.to_string(),
                            subscribed_event_id: 0,
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

    fn issued(channel: &str) -> UnsubscribeNotificationChannelMachine {
        let mut sm = UnsubscribeNotificationChannelMachine::from_parts(
            Created {}.into(),
            SharedState {
                channel: channel.to_string(),
            },
        );
        OnEventWrapper::on_event_mut(
            &mut sm,
            CommandType::UnsubscribeNotificationChannel
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
    fn the_subscribed_event_is_not_an_unsubscription() {
        // The two events carry the same channel field, so the type is what tells them apart.
        let event = HistEventData {
            event: HistoryEvent {
                event_type: EventType::WorkflowNotificationChannelSubscribed as i32,
                ..Default::default()
            },
            replaying: true,
            current_task_is_last_in_history: false,
        };
        let res: Result<UnsubscribeNotificationChannelMachineEvents, _> = event.try_into();
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
                unsubscribe("orders"),
            ))
            .await
            .unwrap();

        let commands = sent.lock().clone();
        assert_eq!(commands.len(), 1, "got {commands:?}");
        assert_eq!(
            commands[0].command_type(),
            CommandType::UnsubscribeNotificationChannel
        );
        assert!(
            matches!(
                &commands[0].attributes,
                Some(command::Attributes::UnsubscribeNotificationChannelCommandAttributes(a))
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
                    *recorder.lock() =
                        Some(f.as_ref().map(|f| f.message.clone()).unwrap_or_default());
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
    async fn replay_matches_the_recorded_unsubscription() {
        let (failure, activated) = replay_first_task(
            |t| {
                t.add_notification_channel_unsubscribed("orders", 0);
            },
            unsubscribe("orders"),
            false,
        )
        .await;
        assert_eq!(failure, None);
        assert!(
            activated,
            "the live task after a matched unsubscription must activate lang"
        );
    }

    #[tokio::test]
    async fn replay_with_another_channel_is_nondeterminism() {
        let failure = replay_first_task(
            |t| {
                t.add_notification_channel_unsubscribed("invoices", 0);
            },
            unsubscribe("orders"),
            true,
        )
        .await
        .0
        .expect("the task must fail as nondeterminism");
        assert!(failure.contains("does not match"), "got {failure}");
    }

    #[tokio::test]
    async fn a_recorded_subscription_does_not_match_an_unsubscription() {
        let failure = replay_first_task(
            |t| {
                t.add_notification_channel_subscribed("orders");
            },
            unsubscribe("orders"),
            true,
        )
        .await
        .0
        .expect("the task must fail as nondeterminism");
        assert!(
            failure.contains("UnsubscribeNotificationChannelMachine does not handle"),
            "got {failure}"
        );
    }

    #[tokio::test]
    async fn a_recorded_unsubscription_lang_does_not_reissue_is_nondeterminism() {
        let failure = replay_first_task(
            |t| {
                t.add_notification_channel_unsubscribed("orders", 0);
            },
            start_timer_cmd(1, Duration::from_secs(1)),
            true,
        )
        .await
        .0
        .expect("the task must fail as nondeterminism");
        assert!(!failure.is_empty());
    }
}
