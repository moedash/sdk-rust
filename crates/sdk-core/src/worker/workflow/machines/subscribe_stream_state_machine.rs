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

/// What the command claimed, kept so the recorded event can be held against it.
///
/// The offset is only kept when it is one the server echoes back rather than one
/// it works out for itself. A negative offset asks the server where the stream
/// stands, and a repeat subscription is recorded at wherever the cursor has
/// already reached, so in both cases the recorded value is the server's answer
/// rather than what the command said.
#[derive(Default, Clone)]
pub(super) struct SharedState {
    stream_id: String,
    start_offset: Option<i64>,
}

/// Subscribe this workflow to a stream. The command carries only the stream id
/// and a start offset; the server resolves the addressing, because a workflow
/// cannot look it up without doing I/O and a value it carried would be a
/// reading rather than a fact.
pub(super) fn subscribe_stream(
    lang_cmd: SubscribeStream,
    first_for_stream: bool,
) -> NewMachineWithCommand {
    let sm = SubscribeStreamMachine::from_parts(
        Created {}.into(),
        SharedState {
            stream_id: lang_cmd.stream_id.clone(),
            start_offset: (first_for_stream && lang_cmd.start_offset >= 0)
                .then_some(lang_cmd.start_offset),
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
        if dat.stream_id != attrs.stream_id {
            return TransitionResult::Err(nondeterminism!(
                "Recorded subscription to stream {:?} does not match the reissued subscription \
                 to stream {:?}",
                attrs.stream_id,
                dat.stream_id
            ));
        }
        if let Some(asked_for) = dat.start_offset
            && asked_for != attrs.start_offset
        {
            return TransitionResult::Err(nondeterminism!(
                "Recorded subscription to stream {:?} starts at offset {}, and the reissued \
                 subscription asked for offset {}",
                attrs.stream_id,
                attrs.start_offset,
                asked_for
            ));
        }
        TransitionResult::default()
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
