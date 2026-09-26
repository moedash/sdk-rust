use super::{
    NewMachineWithCommand, StateMachine, TransitionResult, fsm, workflow_machines::MachineResponse,
};
use crate::worker::workflow::{
    WFMachinesError, fatal,
    machines::{EventInfo, HistEventData, WFMachinesAdapter},
    nondeterminism,
};
use temporalio_common::protos::{
    coresdk::workflow_commands::SubscribeStream,
    temporal::api::{
        enums::v1::{CommandType, EventType},
        history::v1::{WorkflowStreamSubscribedEventAttributes, history_event},
    },
};

fsm! {
    pub(super) name SubscribeStreamMachine;
    command SubscribeStreamMachineCommand;
    error WFMachinesError;
    shared_state SharedState;

    Created --(CommandScheduled) --> CommandIssued;
    CommandIssued --(CommandRecorded(WorkflowStreamSubscribedEventAttributes),
        shared on_command_recorded) --> Done;
}

/// The stream the command named, kept so the recorded event can be held against it.
///
/// The offset is not kept. Comparing it would only be sound for the run's first
/// subscribe to a stream, since the server records a later one at wherever the
/// cursor has already reached, and which one is first cannot be told from here:
/// a subscription made through the stream service leaves no event at all. A
/// check that can fire on a run that did nothing wrong costs more than the drift
/// it would catch.
#[derive(Default, Clone)]
pub(super) struct SharedState {
    stream_name_or_id: String,
}

/// Subscribe this workflow to a stream. The command carries only the name or id
/// and a start offset; the server resolves the addressing, because a workflow
/// cannot look it up without doing I/O and a value it carried would be a
/// reading rather than a fact.
pub(super) fn subscribe_stream(lang_cmd: SubscribeStream) -> NewMachineWithCommand {
    let sm = SubscribeStreamMachine::from_parts(
        Created {}.into(),
        SharedState {
            stream_name_or_id: lang_cmd.stream_name_or_id.clone(),
        },
    );
    NewMachineWithCommand {
        command: lang_cmd.into(),
        machine: sm.into(),
    }
}

#[derive(Debug, derive_more::Display)]
pub(super) enum SubscribeStreamMachineCommand {}

#[derive(Debug, Default, Clone, derive_more::Display)]
pub(super) struct Created {}

#[derive(Debug, Default, Clone, derive_more::Display)]
pub(super) struct CommandIssued {}

impl CommandIssued {
    pub(super) fn on_command_recorded(
        self,
        dat: &mut SharedState,
        attrs: WorkflowStreamSubscribedEventAttributes,
    ) -> SubscribeStreamMachineTransition<Done> {
        if dat.stream_name_or_id == attrs.stream_id {
            TransitionResult::default()
        } else {
            TransitionResult::Err(nondeterminism!(
                "Recorded subscription to stream {:?} does not match the reissued subscription \
                 to stream {:?}",
                attrs.stream_id,
                dat.stream_name_or_id
            ))
        }
    }
}

#[derive(Debug, Default, Clone, derive_more::Display)]
pub(super) struct Done {}

impl WFMachinesAdapter for SubscribeStreamMachine {
    fn adapt_response(
        &self,
        _my_command: Self::Command,
        _event_info: Option<EventInfo>,
    ) -> Result<Vec<MachineResponse>, Self::Error> {
        Err(fatal!(
            "SubscribeStream does not use state machine commands"
        ))
    }
}

impl TryFrom<HistEventData> for SubscribeStreamMachineEvents {
    type Error = WFMachinesError;

    fn try_from(e: HistEventData) -> Result<Self, Self::Error> {
        let e = e.event;
        match e.event_type() {
            EventType::WorkflowStreamSubscribed => {
                if let Some(history_event::Attributes::WorkflowStreamSubscribedEventAttributes(
                    attrs,
                )) = e.attributes
                {
                    Ok(SubscribeStreamMachineEvents::CommandRecorded(attrs))
                } else {
                    Err(fatal!("Stream subscribed attributes were unset: {e}"))
                }
            }
            _ => Err(Self::Error::Nondeterminism(format!(
                "SubscribeStreamMachine does not handle {e}"
            ))),
        }
    }
}

impl TryFrom<CommandType> for SubscribeStreamMachineEvents {
    type Error = WFMachinesError;

    fn try_from(c: CommandType) -> Result<Self, Self::Error> {
        match c {
            CommandType::SubscribeStream => Ok(SubscribeStreamMachineEvents::CommandScheduled),
            _ => Err(Self::Error::Nondeterminism(format!(
                "SubscribeStreamMachine does not handle command type {c:?}"
            ))),
        }
    }
}

impl From<Created> for CommandIssued {
    fn from(_: Created) -> Self {
        Self {}
    }
}
