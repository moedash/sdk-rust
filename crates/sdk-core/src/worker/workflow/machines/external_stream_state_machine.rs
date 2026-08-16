//! The External Workflow Stream marker machine (C9).
//!
//! Modeled on [`super::local_activity_state_machine`], and for the same reason: a marker written
//! live must be *matched* by the `MarkerRecorded` event on replay, and on replay the marker must
//! be found by lookahead before the thing that depends on it is resolved.
//!
//! **Exactly one machine per Workflow Task**, however many progress reports that task carried.
//! Accumulating the annotation is the wait set's job -- it is what receives the deltas -- and this
//! machine's job is only to turn one accumulated annotation into one `RecordMarker` command and
//! then to reconcile it against history.

// The replay half -- `HandleKnownResult` and `marker_data` -- is driven by C10's marker lookahead,
// which is the only thing that can find a marker before the wait set it closes.
#![allow(dead_code)]

use super::{
    EventInfo, HistEventData, NewMachineWithCommand, OnEventWrapper, TransitionResult,
    WFMachinesAdapter, WFMachinesError, fsm, workflow_machines::MachineResponse,
};
use crate::worker::workflow::nondeterminism;
use std::convert::TryFrom;
use temporalio_common::protos::{
    constants::EXTERNAL_STREAM_MARKER_NAME,
    coresdk::external_data::{
        ExternalStreamMarkerData, build_external_stream_marker_details,
        extract_external_stream_marker_data,
    },
    temporal::api::{
        command::v1::{Command as ProtoCommand, RecordMarkerCommandAttributes, command},
        enums::v1::{CommandType, EventType},
        history::v1::{HistoryEvent, MarkerRecordedEventAttributes, history_event},
    },
};

fsm! {
    pub(super) name ExternalStreamMachine;
    command ExternalStreamCommand;
    error WFMachinesError;
    shared_state SharedState;

    // Live path: the marker is created from the annotation this Workflow Task accumulated.
    Created --(Emit, shared on_emit) --> MarkerCommandCreated;
    MarkerCommandCreated --(CommandRecordMarker, on_command_record_marker) --> ResultNotified;
    ResultNotified --(MarkerRecorded(ExternalStreamMarkerData), shared on_marker_recorded)
      --> MarkerCommandRecorded;

    // Replay path: the marker is found by lookahead before the wait set it closes is resolved,
    // and is settled when its `MarkerRecorded` event is reached. Driven by C10.
    Replaying --(Emit, shared on_replay_emit) --> WaitingResolveFromMarkerLookAhead;
    WaitingResolveFromMarkerLookAhead --(HandleKnownResult(ExternalStreamMarkerData),
                                         shared on_handle_known_result)
      --> ResolvedFromMarkerLookAheadWaitingMarkerEvent;
    ResolvedFromMarkerLookAheadWaitingMarkerEvent
      --(MarkerRecorded(ExternalStreamMarkerData), shared on_marker_recorded)
      --> MarkerCommandRecorded;
}

#[derive(Debug, Clone)]
pub(super) struct SharedState {
    /// The envelope this machine will write, or did write.
    ///
    /// Held rather than rebuilt so the command and the later reconciliation are provably the same
    /// data -- Core is annotation-blind, so it has no way to notice a discrepancy otherwise.
    data: ExternalStreamMarkerData,
    /// Whether the machine was created while replaying, which is what decides whether a command
    /// is issued at all.
    replaying_when_invoked: bool,
}

#[derive(Debug, derive_more::Display)]
pub(super) enum ExternalStreamCommand {
    /// Write the marker to History.
    RecordMarker,
    /// The marker already exists in History; nothing to write.
    AlreadyRecorded,
}

#[derive(Default, Clone)]
pub(super) struct Created {}

#[derive(Default, Clone)]
pub(super) struct Replaying {}

#[derive(Default, Clone)]
pub(super) struct MarkerCommandCreated {}

#[derive(Default, Clone)]
pub(super) struct ResultNotified {}

#[derive(Default, Clone)]
pub(super) struct WaitingResolveFromMarkerLookAhead {}

#[derive(Default, Clone)]
pub(super) struct ResolvedFromMarkerLookAheadWaitingMarkerEvent {}

#[derive(Default, Clone)]
pub(super) struct MarkerCommandRecorded {}

impl Created {
    pub(super) fn on_emit(
        self,
        _state: &mut SharedState,
    ) -> ExternalStreamMachineTransition<MarkerCommandCreated> {
        TransitionResult::commands(vec![ExternalStreamCommand::RecordMarker])
    }
}

impl Replaying {
    pub(super) fn on_replay_emit(
        self,
        _state: &mut SharedState,
    ) -> ExternalStreamMachineTransition<WaitingResolveFromMarkerLookAhead> {
        // Nothing is written on replay: the marker is already in History, and writing it again
        // would produce a command mismatch against the very event it was derived from.
        TransitionResult::commands(vec![ExternalStreamCommand::AlreadyRecorded])
    }
}

impl MarkerCommandCreated {
    pub(super) fn on_command_record_marker(
        self,
    ) -> ExternalStreamMachineTransition<ResultNotified> {
        TransitionResult::default()
    }
}

impl WaitingResolveFromMarkerLookAhead {
    pub(super) fn on_handle_known_result(
        self,
        state: &mut SharedState,
        data: ExternalStreamMarkerData,
    ) -> ExternalStreamMachineTransition<ResolvedFromMarkerLookAheadWaitingMarkerEvent> {
        state.data = data;
        TransitionResult::default()
    }
}

impl ResultNotified {
    pub(super) fn on_marker_recorded(
        self,
        state: &mut SharedState,
        data: ExternalStreamMarkerData,
    ) -> ExternalStreamMachineTransition<MarkerCommandRecorded> {
        verify_marker_matches(state, &data)
    }
}

impl ResolvedFromMarkerLookAheadWaitingMarkerEvent {
    pub(super) fn on_marker_recorded(
        self,
        state: &mut SharedState,
        data: ExternalStreamMarkerData,
    ) -> ExternalStreamMachineTransition<MarkerCommandRecorded> {
        verify_marker_matches(state, &data)
    }
}

/// Rejects a marker that closes a different quiescent snapshot than this machine's.
///
/// Core cannot check the annotation -- it is opaque by design -- so the quiescence generation is
/// the only thing it *can* check, and checking it is what turns a reordered marker into a
/// nondeterminism error rather than a silently different stream result.
fn verify_marker_matches(
    state: &mut SharedState,
    data: &ExternalStreamMarkerData,
) -> ExternalStreamMachineTransition<MarkerCommandRecorded> {
    if data.quiescence_generation != state.data.quiescence_generation {
        return TransitionResult::Err(WFMachinesError::Nondeterminism(format!(
            "External stream marker in history closes quiescence generation {} but the machine \
             expecting it closes {}",
            data.quiescence_generation, state.data.quiescence_generation
        )));
    }
    TransitionResult::default()
}

impl ExternalStreamMachine {
    /// A machine for one Workflow Task's marker.
    ///
    /// `replaying` decides which path it takes, and therefore whether a command is issued at all.
    pub(super) fn new(data: ExternalStreamMarkerData, replaying: bool) -> NewMachineWithCommand {
        let (machine, command) = Self::new_scheduled(data, replaying);
        NewMachineWithCommand {
            command,
            machine: machine.into(),
        }
    }

    fn new_scheduled(
        data: ExternalStreamMarkerData,
        replaying: bool,
    ) -> (Self, command::Attributes) {
        let mut machine = ExternalStreamMachine {
            state: Some(if replaying {
                ExternalStreamMachineState::Replaying(Replaying {})
            } else {
                ExternalStreamMachineState::Created(Created {})
            }),
            shared_state: SharedState {
                data: data.clone(),
                replaying_when_invoked: replaying,
            },
        };
        OnEventWrapper::on_event_mut(&mut machine, ExternalStreamMachineEvents::Emit)
            .expect("Emit is always valid from the initial state");
        (machine, marker_command(&data))
    }

    /// The envelope this machine wrote or expects.
    pub(super) fn marker_data(&self) -> &ExternalStreamMarkerData {
        &self.shared_state.data
    }
}

fn marker_command(data: &ExternalStreamMarkerData) -> command::Attributes {
    command::Attributes::RecordMarkerCommandAttributes(RecordMarkerCommandAttributes {
        marker_name: EXTERNAL_STREAM_MARKER_NAME.to_string(),
        details: build_external_stream_marker_details(data),
        header: None,
        failure: None,
    })
}

impl TryFrom<CommandType> for ExternalStreamMachineEvents {
    type Error = ();

    fn try_from(c: CommandType) -> Result<Self, Self::Error> {
        Ok(match c {
            CommandType::RecordMarker => Self::CommandRecordMarker,
            _ => return Err(()),
        })
    }
}

impl TryFrom<HistEventData> for ExternalStreamMachineEvents {
    type Error = WFMachinesError;

    fn try_from(e: HistEventData) -> Result<Self, Self::Error> {
        let e = e.event;
        if e.event_type() != EventType::MarkerRecorded {
            return Err(nondeterminism!(
                "External stream machine cannot handle this event: {e}"
            ));
        }
        match extract_stream_marker(&e) {
            Some(data) => Ok(ExternalStreamMachineEvents::MarkerRecorded(data)),
            None => Err(nondeterminism!(
                "Marker recorded event {e} is not an external stream marker"
            )),
        }
    }
}

/// The envelope inside a `MarkerRecorded` event, if it is one of ours.
pub(super) fn extract_stream_marker(e: &HistoryEvent) -> Option<ExternalStreamMarkerData> {
    if e.event_type() != EventType::MarkerRecorded {
        return None;
    }
    match &e.attributes {
        Some(history_event::Attributes::MarkerRecordedEventAttributes(
            MarkerRecordedEventAttributes {
                marker_name,
                details,
                ..
            },
        )) if marker_name == EXTERNAL_STREAM_MARKER_NAME => {
            extract_external_stream_marker_data(details)
        }
        _ => None,
    }
}

impl WFMachinesAdapter for ExternalStreamMachine {
    fn adapt_response(
        &self,
        my_command: Self::Command,
        _event_info: Option<EventInfo>,
    ) -> Result<Vec<MachineResponse>, WFMachinesError> {
        Ok(match my_command {
            ExternalStreamCommand::RecordMarker => {
                if self.shared_state.replaying_when_invoked {
                    vec![]
                } else {
                    vec![MachineResponse::IssueNewCommand(ProtoCommand {
                        ..marker_command(&self.shared_state.data).into()
                    })]
                }
            }
            ExternalStreamCommand::AlreadyRecorded => vec![],
        })
    }
}
