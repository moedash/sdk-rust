use super::{
    NewMachineWithCommand, StateMachine, TransitionResult, fsm, workflow_machines::MachineResponse,
};
use crate::worker::workflow::{
    WFMachinesError,
    machines::{EventInfo, HistEventData, WFMachinesAdapter},
};
use temporalio_common::protos::{
    coresdk::workflow_commands::SubscribeStream,
    temporal::api::enums::v1::{CommandType, EventType},
};

fsm! {
    pub(super) name SubscribeStreamMachine;
    command SubscribeStreamMachineCommand;
    error WFMachinesError;

    Created --(CommandScheduled) --> CommandIssued;
    CommandIssued --(CommandRecorded) --> Done;
}

/// Subscribe this workflow to a stream. The command carries only the stream id
/// and a start offset; the server resolves the addressing, because a workflow
/// cannot look it up without doing I/O and a value it carried would be a
/// reading rather than a fact.
pub(super) fn subscribe_stream(lang_cmd: SubscribeStream) -> NewMachineWithCommand {
    let sm = SubscribeStreamMachine::from_parts(Created {}.into(), ());
    NewMachineWithCommand {
        command: lang_cmd.into(),
        machine: sm.into(),
    }
}

type SharedState = ();

#[derive(Debug, derive_more::Display)]
pub(super) enum SubscribeStreamMachineCommand {}

#[derive(Debug, Default, Clone, derive_more::Display)]
pub(super) struct Created {}

#[derive(Debug, Default, Clone, derive_more::Display)]
pub(super) struct CommandIssued {}

#[derive(Debug, Default, Clone, derive_more::Display)]
pub(super) struct Done {}

impl WFMachinesAdapter for SubscribeStreamMachine {
    fn adapt_response(
        &self,
        _my_command: Self::Command,
        _event_info: Option<EventInfo>,
    ) -> Result<Vec<MachineResponse>, Self::Error> {
        Err(Self::Error::Nondeterminism(
            "SubscribeStream does not use state machine commands".to_string(),
        ))
    }
}

impl TryFrom<HistEventData> for SubscribeStreamMachineEvents {
    type Error = WFMachinesError;

    fn try_from(e: HistEventData) -> Result<Self, Self::Error> {
        let e = e.event;
        match e.event_type() {
            EventType::WorkflowStreamSubscribed => {
                Ok(SubscribeStreamMachineEvents::CommandRecorded)
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

impl From<CommandIssued> for Done {
    fn from(_: CommandIssued) -> Self {
        Self {}
    }
}

impl From<Created> for CommandIssued {
    fn from(_: Created) -> Self {
        Self {}
    }
}
