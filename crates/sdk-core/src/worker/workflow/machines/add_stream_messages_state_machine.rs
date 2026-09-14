use super::{
    NewMachineWithCommand, StateMachine, TransitionResult, fsm, workflow_machines::MachineResponse,
};
use crate::worker::workflow::{
    WFMachinesError,
    machines::{EventInfo, HistEventData, WFMachinesAdapter},
};
use temporalio_common::protos::{
    coresdk::workflow_commands::AddStreamMessages,
    temporal::api::enums::v1::{CommandType, EventType},
};

fsm! {
    pub(super) name AddStreamMessagesMachine;
    command AddStreamMessagesMachineCommand;
    error WFMachinesError;

    Created --(CommandScheduled) --> CommandIssued;
    CommandIssued --(CommandRecorded) --> Done;
}

/// Publish a batch of messages to a stream this workflow owns.
///
/// The bodies go to the stream's own log, and History gets one event naming the
/// offset range the batch landed at. The offsets are assigned by the server, so
/// nothing here predicts them.
pub(super) fn add_stream_messages(lang_cmd: AddStreamMessages) -> NewMachineWithCommand {
    let sm = AddStreamMessagesMachine::from_parts(Created {}.into(), ());
    NewMachineWithCommand {
        command: lang_cmd.into(),
        machine: sm.into(),
    }
}

#[derive(Debug, derive_more::Display)]
pub(super) enum AddStreamMessagesMachineCommand {}

#[derive(Debug, Default, Clone, derive_more::Display)]
pub(super) struct Created {}

#[derive(Debug, Default, Clone, derive_more::Display)]
pub(super) struct CommandIssued {}

#[derive(Debug, Default, Clone, derive_more::Display)]
pub(super) struct Done {}

impl WFMachinesAdapter for AddStreamMessagesMachine {
    fn adapt_response(
        &self,
        _my_command: Self::Command,
        _event_info: Option<EventInfo>,
    ) -> Result<Vec<MachineResponse>, Self::Error> {
        Err(Self::Error::Nondeterminism(
            "AddStreamMessages does not use state machine commands".to_string(),
        ))
    }
}

impl TryFrom<HistEventData> for AddStreamMessagesMachineEvents {
    type Error = WFMachinesError;

    fn try_from(e: HistEventData) -> Result<Self, Self::Error> {
        let e = e.event;
        match e.event_type() {
            EventType::WorkflowStreamMessagesAdded => {
                Ok(AddStreamMessagesMachineEvents::CommandRecorded)
            }
            _ => Err(Self::Error::Nondeterminism(format!(
                "AddStreamMessagesMachine does not handle {e}"
            ))),
        }
    }
}

impl TryFrom<CommandType> for AddStreamMessagesMachineEvents {
    type Error = WFMachinesError;

    fn try_from(c: CommandType) -> Result<Self, Self::Error> {
        match c {
            CommandType::AddStreamMessages => Ok(AddStreamMessagesMachineEvents::CommandScheduled),
            _ => Err(Self::Error::Nondeterminism(format!(
                "AddStreamMessagesMachine does not handle command type {c:?}"
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
