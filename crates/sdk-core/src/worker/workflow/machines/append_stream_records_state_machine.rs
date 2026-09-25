use super::{
    NewMachineWithCommand, StateMachine, TransitionResult, fsm, workflow_machines::MachineResponse,
};
use crate::worker::workflow::{
    WFMachinesError, fatal,
    machines::{EventInfo, HistEventData, WFMachinesAdapter},
    nondeterminism,
};
use temporalio_common::protos::{
    coresdk::workflow_commands::AppendStreamRecords,
    temporal::api::{
        enums::v1::{CommandType, EventType},
        history::v1::{WorkflowStreamRecordsAppendedEventAttributes, history_event},
    },
};

fsm! {
    pub(super) name AppendStreamRecordsMachine;
    command AppendStreamRecordsMachineCommand;
    error WFMachinesError;
    shared_state SharedState;

    Created --(CommandScheduled) --> CommandIssued;
    CommandIssued --(CommandRecorded(WorkflowStreamRecordsAppendedEventAttributes),
        shared on_command_recorded) --> Done;
}

/// What the command claimed, kept so the recorded event can be held against it.
#[derive(Default, Clone)]
pub(super) struct SharedState {
    stream_id: String,
    record_count: i64,
}

/// Append a batch of records to a stream this workflow owns.
///
/// The bodies go to the stream's own log, and History gets one event naming the
/// offset range the batch landed at. The offsets are assigned by the server, so
/// nothing here predicts them.
pub(super) fn append_stream_records(lang_cmd: AppendStreamRecords) -> NewMachineWithCommand {
    let sm = AppendStreamRecordsMachine::from_parts(
        Created {}.into(),
        SharedState {
            stream_id: lang_cmd.stream_id.clone(),
            record_count: lang_cmd.records.len() as i64,
        },
    );
    NewMachineWithCommand {
        command: lang_cmd.into(),
        machine: sm.into(),
    }
}

#[derive(Debug, derive_more::Display)]
pub(super) enum AppendStreamRecordsMachineCommand {}

#[derive(Debug, Default, Clone, derive_more::Display)]
pub(super) struct Created {}

#[derive(Debug, Default, Clone, derive_more::Display)]
pub(super) struct CommandIssued {}

impl CommandIssued {
    pub(super) fn on_command_recorded(
        self,
        dat: &mut SharedState,
        attrs: WorkflowStreamRecordsAppendedEventAttributes,
    ) -> AppendStreamRecordsMachineTransition<Done> {
        // An empty id names the workflow's default stream, and the server is the
        // one that resolves that name, so only a named stream can be compared.
        let same_stream = dat.stream_id.is_empty() || dat.stream_id == attrs.stream_id;
        if same_stream && dat.record_count == attrs.record_count {
            TransitionResult::default()
        } else {
            TransitionResult::Err(nondeterminism!(
                "Recorded append of {} records to stream {:?} does not match the reissued \
                 append of {} records to stream {:?}",
                attrs.record_count,
                attrs.stream_id,
                dat.record_count,
                dat.stream_id
            ))
        }
    }
}

#[derive(Debug, Default, Clone, derive_more::Display)]
pub(super) struct Done {}

impl WFMachinesAdapter for AppendStreamRecordsMachine {
    fn adapt_response(
        &self,
        _my_command: Self::Command,
        _event_info: Option<EventInfo>,
    ) -> Result<Vec<MachineResponse>, Self::Error> {
        Err(Self::Error::Nondeterminism(
            "AppendStreamRecords does not use state machine commands".to_string(),
        ))
    }
}

impl TryFrom<HistEventData> for AppendStreamRecordsMachineEvents {
    type Error = WFMachinesError;

    fn try_from(e: HistEventData) -> Result<Self, Self::Error> {
        let e = e.event;
        match e.event_type() {
            EventType::WorkflowStreamRecordsAppended => {
                if let Some(
                    history_event::Attributes::WorkflowStreamRecordsAppendedEventAttributes(attrs),
                ) = e.attributes
                {
                    Ok(AppendStreamRecordsMachineEvents::CommandRecorded(attrs))
                } else {
                    Err(fatal!("Stream records appended attributes were unset: {e}"))
                }
            }
            _ => Err(Self::Error::Nondeterminism(format!(
                "AppendStreamRecordsMachine does not handle {e}"
            ))),
        }
    }
}

impl TryFrom<CommandType> for AppendStreamRecordsMachineEvents {
    type Error = WFMachinesError;

    fn try_from(c: CommandType) -> Result<Self, Self::Error> {
        match c {
            CommandType::AppendStreamRecords => {
                Ok(AppendStreamRecordsMachineEvents::CommandScheduled)
            }
            _ => Err(Self::Error::Nondeterminism(format!(
                "AppendStreamRecordsMachine does not handle command type {c:?}"
            ))),
        }
    }
}

impl From<Created> for CommandIssued {
    fn from(_: Created) -> Self {
        Self {}
    }
}
