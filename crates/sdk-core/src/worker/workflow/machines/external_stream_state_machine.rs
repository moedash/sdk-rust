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
//!
//! The two paths are created by different callers and never by the same one: the live marker comes
//! from a completion that reached a boundary, and the replay marker comes from C10's lookahead,
//! which is the only thing that can find a marker *before* the wait set it closes is resolved.

use super::{
    EventInfo, HistEventData, NewMachineWithCommand, OnEventWrapper, StateMachine,
    TransitionResult, WFMachinesAdapter, WFMachinesError, fsm, workflow_machines::MachineResponse,
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
    if data.output != state.data.output {
        return TransitionResult::Err(WFMachinesError::Nondeterminism(
            "External stream marker in history carries a different external output manifest than \
             the machine expecting it"
                .to_string(),
        ));
    }
    TransitionResult::default()
}

impl ExternalStreamMachine {
    /// A machine that **writes** one Workflow Task's marker.
    ///
    /// The live path only. A machine created here issues the `RecordMarker` command and then
    /// reconciles it against the `MarkerRecorded` event the server writes back.
    pub(super) fn record_marker(data: ExternalStreamMarkerData) -> NewMachineWithCommand {
        let mut machine = ExternalStreamMachine {
            state: Some(ExternalStreamMachineState::Created(Created {})),
            shared_state: SharedState { data: data.clone() },
        };
        OnEventWrapper::on_event_mut(&mut machine, ExternalStreamMachineEvents::Emit)
            .expect("Emit is always valid from the initial state");
        NewMachineWithCommand {
            command: marker_command(&data),
            machine: machine.into(),
        }
    }

    /// A machine for a marker C10's lookahead found ahead of the wait set it closes.
    ///
    /// Nothing is written: the marker is already in History, and issuing a command for it would
    /// produce a mismatch against the very event it was read from. The machine exists only so that
    /// event, when history reaches it, has something to be matched by -- which is what turns a
    /// marker Core did not expect into a nondeterminism error rather than a silent skip.
    pub(super) fn resolved_from_marker_lookahead(data: ExternalStreamMarkerData) -> Self {
        let mut machine = ExternalStreamMachine {
            state: Some(ExternalStreamMachineState::Replaying(Replaying {})),
            shared_state: SharedState { data: data.clone() },
        };
        OnEventWrapper::on_event_mut(&mut machine, ExternalStreamMachineEvents::Emit)
            .expect("Emit is always valid from the initial state");
        OnEventWrapper::on_event_mut(
            &mut machine,
            ExternalStreamMachineEvents::HandleKnownResult(data),
        )
        .expect("A looked-ahead marker always resolves the machine created for it");
        machine
    }

    /// Whether the `MarkerRecorded` event for this machine bypasses normal command matching.
    ///
    /// It does exactly when the machine came from lookahead, because no command was ever issued
    /// for it and there is therefore nothing in the outgoing queue to match. A machine that wrote
    /// its own marker is in `ResultNotified` and must go through the queue like any other command,
    /// or the command it issued would never be consumed.
    pub(super) fn marker_should_get_special_handling(&self) -> Result<bool, WFMachinesError> {
        match self.state() {
            ExternalStreamMachineState::ResultNotified(_) => Ok(false),
            ExternalStreamMachineState::ResolvedFromMarkerLookAheadWaitingMarkerEvent(_) => {
                Ok(true)
            }
            _ => Err(WFMachinesError::Fatal(format!(
                "Attempted to check for external stream marker handling in invalid state {}",
                self.state()
            ))),
        }
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
                vec![MachineResponse::IssueNewCommand(ProtoCommand {
                    ..marker_command(&self.shared_state.data).into()
                })]
            }
            ExternalStreamCommand::AlreadyRecorded => vec![],
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use temporalio_common::protos::coresdk::external_data::ExternalOutputStreamManifest;

    fn marker_with_stage_token(stage_token: &str) -> ExternalStreamMarkerData {
        ExternalStreamMarkerData {
            schema_version: 1,
            output: Some(ExternalOutputStreamManifest {
                schema_version: 1,
                fingerprint_version: 1,
                stage_token: stage_token.to_string(),
                history_floor_event_id: 1,
                run_id: "run-id".to_string(),
                provider_id: "provider".to_string(),
                provider_format_version: 1,
                ..Default::default()
            }),
            ..Default::default()
        }
    }

    #[test]
    fn a_different_output_manifest_is_nondeterministic() {
        let expected = marker_with_stage_token("expected");
        let mut state = SharedState {
            data: expected.clone(),
        };
        let actual = marker_with_stage_token("different");

        assert!(matches!(
            verify_marker_matches(&mut state, &actual),
            TransitionResult::Err(WFMachinesError::Nondeterminism(message))
                if message.contains("different external output manifest")
        ));
    }

    #[test]
    fn the_same_output_manifest_matches() {
        let expected = marker_with_stage_token("expected");
        let mut state = SharedState {
            data: expected.clone(),
        };

        assert!(matches!(
            verify_marker_matches(&mut state, &expected),
            TransitionResult::Ok { .. }
        ));
    }
}
