use super::{
    NewMachineWithCommand, StateMachine, TransitionResult, fsm, workflow_machines::MachineResponse,
};
use crate::worker::workflow::{
    WFMachinesError, fatal,
    machines::{EventInfo, HistEventData, WFMachinesAdapter},
    nondeterminism,
};
use temporalio_common::protos::{
    coresdk::workflow_commands::AddStreamMessages,
    temporal::api::{
        enums::v1::{CommandType, EventType},
        history::v1::{WorkflowStreamMessagesAddedEventAttributes, history_event},
    },
};

fsm! {
    pub(super) name AddStreamMessagesMachine;
    command AddStreamMessagesMachineCommand;
    error WFMachinesError;
    shared_state SharedState;

    Created --(CommandScheduled) --> CommandIssued;
    CommandIssued --(CommandRecorded(WorkflowStreamMessagesAddedEventAttributes),
        shared on_command_recorded) --> Done;
}

/// What the command claimed, kept so the recorded event can be held against it.
#[derive(Default, Clone)]
pub(super) struct SharedState {
    stream_id: String,
    message_count: i64,
}

/// Publish a batch of messages to a stream this workflow owns.
///
/// The bodies go to the stream's own log, and History gets one event naming the
/// offset range the batch landed at. The offsets are assigned by the server, so
/// nothing here predicts them.
pub(super) fn add_stream_messages(lang_cmd: AddStreamMessages) -> NewMachineWithCommand {
    let sm = AddStreamMessagesMachine::from_parts(
        Created {}.into(),
        SharedState {
            stream_id: lang_cmd.stream_id.clone(),
            message_count: lang_cmd.messages.len() as i64,
        },
    );
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

impl CommandIssued {
    pub(super) fn on_command_recorded(
        self,
        dat: &mut SharedState,
        attrs: WorkflowStreamMessagesAddedEventAttributes,
    ) -> AddStreamMessagesMachineTransition<Done> {
        // An empty id names the workflow's default stream, and the server is the
        // one that resolves that name, so only a named stream can be compared.
        let same_stream = dat.stream_id.is_empty() || dat.stream_id == attrs.stream_id;
        if same_stream && dat.message_count == attrs.message_count {
            TransitionResult::default()
        } else {
            TransitionResult::Err(nondeterminism!(
                "Recorded publish of {} messages to stream {:?} does not match the reissued \
                 publish of {} messages to stream {:?}",
                attrs.message_count,
                attrs.stream_id,
                dat.message_count,
                dat.stream_id
            ))
        }
    }
}

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
                if let Some(
                    history_event::Attributes::WorkflowStreamMessagesAddedEventAttributes(attrs),
                ) = e.attributes
                {
                    Ok(AddStreamMessagesMachineEvents::CommandRecorded(attrs))
                } else {
                    Err(fatal!("Stream messages added attributes were unset: {e}"))
                }
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

impl From<Created> for CommandIssued {
    fn from(_: Created) -> Self {
        Self {}
    }
}
