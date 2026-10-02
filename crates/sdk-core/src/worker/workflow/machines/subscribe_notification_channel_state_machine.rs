use super::{
    NewMachineWithCommand, StateMachine, TransitionResult, fsm, workflow_machines::MachineResponse,
};
use crate::worker::workflow::{
    WFMachinesError, fatal,
    machines::{EventInfo, HistEventData, WFMachinesAdapter},
    nondeterminism,
};
use temporalio_common::protos::{
    coresdk::workflow_commands::SubscribeNotificationChannel,
    temporal::api::{
        enums::v1::{CommandType, EventType},
        history::v1::{WorkflowNotificationChannelSubscribedEventAttributes, history_event},
    },
};

fsm! {
    pub(super) name SubscribeNotificationChannelMachine;
    command SubscribeNotificationChannelMachineCommand;
    error WFMachinesError;
    shared_state SharedState;

    Created --(CommandScheduled) --> CommandIssued;
    CommandIssued --(CommandRecorded(WorkflowNotificationChannelSubscribedEventAttributes),
        shared on_command_recorded) --> Done;
}

/// The channel the command named, kept so the recorded event can be held against it.
#[derive(Default, Clone)]
pub(super) struct SharedState {
    channel: String,
}

/// Subscribe this workflow to a notification channel. The command carries only
/// the channel name; the notifications arrive later on the scheduled event of
/// each Workflow Task, so nothing is handed back here.
pub(super) fn subscribe_notification_channel(
    lang_cmd: SubscribeNotificationChannel,
) -> NewMachineWithCommand {
    let sm = SubscribeNotificationChannelMachine::from_parts(
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
pub(super) enum SubscribeNotificationChannelMachineCommand {}

#[derive(Debug, Default, Clone, derive_more::Display)]
pub(super) struct Created {}

#[derive(Debug, Default, Clone, derive_more::Display)]
pub(super) struct CommandIssued {}

impl CommandIssued {
    pub(super) fn on_command_recorded(
        self,
        dat: &mut SharedState,
        attrs: WorkflowNotificationChannelSubscribedEventAttributes,
    ) -> SubscribeNotificationChannelMachineTransition<Done> {
        if dat.channel == attrs.channel {
            TransitionResult::default()
        } else {
            TransitionResult::Err(nondeterminism!(
                "Recorded subscription to notification channel {:?} does not match the \
                 reissued subscription to channel {:?}",
                attrs.channel,
                dat.channel
            ))
        }
    }
}

#[derive(Debug, Default, Clone, derive_more::Display)]
pub(super) struct Done {}

impl WFMachinesAdapter for SubscribeNotificationChannelMachine {
    fn adapt_response(
        &self,
        _my_command: Self::Command,
        _event_info: Option<EventInfo>,
    ) -> Result<Vec<MachineResponse>, Self::Error> {
        Err(fatal!(
            "SubscribeNotificationChannel does not use state machine commands"
        ))
    }
}

impl TryFrom<HistEventData> for SubscribeNotificationChannelMachineEvents {
    type Error = WFMachinesError;

    fn try_from(e: HistEventData) -> Result<Self, Self::Error> {
        let e = e.event;
        match e.event_type() {
            EventType::WorkflowNotificationChannelSubscribed => {
                if let Some(
                    history_event::Attributes::WorkflowNotificationChannelSubscribedEventAttributes(
                        attrs,
                    ),
                ) = e.attributes
                {
                    Ok(SubscribeNotificationChannelMachineEvents::CommandRecorded(
                        attrs,
                    ))
                } else {
                    Err(fatal!(
                        "Notification channel subscribed attributes were unset: {e}"
                    ))
                }
            }
            _ => Err(Self::Error::Nondeterminism(format!(
                "SubscribeNotificationChannelMachine does not handle {e}"
            ))),
        }
    }
}

impl TryFrom<CommandType> for SubscribeNotificationChannelMachineEvents {
    type Error = WFMachinesError;

    fn try_from(c: CommandType) -> Result<Self, Self::Error> {
        match c {
            CommandType::SubscribeNotificationChannel => {
                Ok(SubscribeNotificationChannelMachineEvents::CommandScheduled)
            }
            _ => Err(Self::Error::Nondeterminism(format!(
                "SubscribeNotificationChannelMachine does not handle command type {c:?}"
            ))),
        }
    }
}

impl From<Created> for CommandIssued {
    fn from(_: Created) -> Self {
        Self {}
    }
}
