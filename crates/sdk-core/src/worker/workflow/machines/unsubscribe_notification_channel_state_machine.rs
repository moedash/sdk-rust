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

/// The channel the command named, kept so the recorded event can be held against it.
#[derive(Default, Clone)]
pub(super) struct SharedState {
    channel: String,
}

/// End this workflow's subscription to a notification channel. The server
/// records the event whether or not the run held a subscription, so the match
/// on replay is by channel alone and the recorded subscribed event id is not
/// part of it.
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
