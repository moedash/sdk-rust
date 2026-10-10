//! Nexus operation progress: the server folds it onto a Workflow Task's scheduled event, and Core
//! hands it to lang as one job per started operation per task, live and on replay alike.

use crate::{
    replay::TestHistoryBuilder,
    test_help::{
        MockPollCfg, ResponseType, WorkerExt, build_mock_pollers, mock_worker, start_timer_cmd,
    },
    worker::client::mocks::mock_worker_client,
};
use std::time::Duration;
use temporalio_common::{
    protos::{
        coresdk::{
            workflow_activation::{
                ResolveNexusOperationProgress, WorkflowActivation, workflow_activation_job,
            },
            workflow_commands::{CompleteWorkflowExecution, ScheduleNexusOperation},
            workflow_completion::WorkflowActivationCompletion,
        },
        temporal::api::{
            common::v1::Payload,
            enums::v1::EventType,
            history::v1::{
                NexusOperationCompletedEventAttributes, NexusOperationScheduledEventAttributes,
                NexusOperationStartedEventAttributes,
            },
            nexus::v1::{NexusOperationProgress, nexus_operation_progress},
        },
    },
    worker::WorkerTaskTypes,
};

fn progress(scheduled_event_id: i64, counter: i64) -> NexusOperationProgress {
    NexusOperationProgress {
        operation: Some(nexus_operation_progress::Operation::ScheduledEventId(
            scheduled_event_id,
        )),
        position: format!("position-{counter}").into_bytes(),
        counter,
        metadata: [(
            "records".to_string(),
            Payload {
                data: counter.to_string().into_bytes(),
                ..Default::default()
            },
        )]
        .into(),
    }
}

/// The job lang should get for `progress` of the operation lang scheduled as `seq`.
fn job_for(seq: u32, progress: &NexusOperationProgress) -> workflow_activation_job::Variant {
    workflow_activation_job::Variant::ResolveNexusOperationProgress(ResolveNexusOperationProgress {
        seq,
        position: progress.position.clone(),
        counter: progress.counter,
        metadata: progress.metadata.clone(),
    })
}

fn schedule_operation(seq: u32) -> ScheduleNexusOperation {
    ScheduleNexusOperation {
        seq,
        endpoint: "endpoint".to_string(),
        service: "service".to_string(),
        operation: "operation".to_string(),
        ..Default::default()
    }
}

fn add_operation_scheduled(t: &mut TestHistoryBuilder) -> i64 {
    t.add(NexusOperationScheduledEventAttributes {
        endpoint: "endpoint".to_string(),
        service: "service".to_string(),
        operation: "operation".to_string(),
        ..Default::default()
    })
}

fn add_operation_started(t: &mut TestHistoryBuilder, scheduled_event_id: i64) {
    t.add(NexusOperationStartedEventAttributes {
        scheduled_event_id,
        operation_token: format!("token-{scheduled_event_id}"),
        ..Default::default()
    });
}

fn job_names(activation: &WorkflowActivation) -> Vec<String> {
    activation
        .jobs
        .iter()
        .map(|job| match &job.variant {
            Some(workflow_activation_job::Variant::ResolveNexusOperationProgress(p)) => {
                format!("Progress({}, {})", p.seq, p.counter)
            }
            Some(workflow_activation_job::Variant::ResolveNexusOperationStart(s)) => {
                format!("Start({})", s.seq)
            }
            Some(workflow_activation_job::Variant::ResolveNexusOperation(r)) => {
                format!("Resolve({})", r.seq)
            }
            Some(other) => other.to_string(),
            None => "none".to_string(),
        })
        .collect()
}

fn progress_jobs(activation: &WorkflowActivation) -> Vec<workflow_activation_job::Variant> {
    activation
        .jobs
        .iter()
        .filter_map(|job| match &job.variant {
            v @ Some(workflow_activation_job::Variant::ResolveNexusOperationProgress(_)) => {
                v.clone()
            }
            _ => None,
        })
        .collect()
}

fn worker(history: TestHistoryBuilder, batches: Vec<ResponseType>) -> crate::Worker {
    let mut mock = build_mock_pollers(MockPollCfg::from_resp_batches(
        "fake_wf_id",
        history,
        batches,
        mock_worker_client(),
    ));
    mock.worker_cfg(|w| {
        w.task_types = WorkerTaskTypes::workflow_only();
        w.max_cached_workflows = 1;
    });
    mock_worker(mock)
}

async fn complete(
    worker: &crate::Worker,
    run_id: &str,
    commands: Vec<
        impl Into<temporalio_common::protos::coresdk::workflow_commands::workflow_command::Variant>,
    >,
) {
    worker
        .complete_workflow_activation(WorkflowActivationCompletion::from_cmds(
            run_id.to_string(),
            commands.into_iter().map(Into::into).collect(),
        ))
        .await
        .unwrap();
}

/// A started operation, then a task with nothing for lang to do, then two tasks scheduled only
/// because progress arrived. The second-to-last one completes with no commands, which is the
/// shape of a heartbeat chain, so replay must still give it an activation of its own.
fn progress_after_a_quiet_task() -> (TestHistoryBuilder, [NexusOperationProgress; 2]) {
    let mut t = TestHistoryBuilder::default();
    t.add_by_type(EventType::WorkflowExecutionStarted);
    t.add_full_wf_task();
    let scheduled = add_operation_scheduled(&mut t);
    add_operation_started(&mut t, scheduled);
    t.add_full_wf_task();
    let first = progress(scheduled, 1);
    t.add_workflow_task_scheduled_with_nexus_progress(vec![first.clone()]);
    t.add_workflow_task_started();
    t.add_workflow_task_completed();
    let second = progress(scheduled, 2);
    t.add_workflow_task_scheduled_with_nexus_progress(vec![second.clone()]);
    t.add_workflow_task_started();
    (t, [first, second])
}

/// Drives `progress_after_a_quiet_task` the way a Workflow would, checking each activation.
async fn run_progress_after_a_quiet_task(batches: Vec<ResponseType>, replaying: bool) {
    let (history, [first, second]) = progress_after_a_quiet_task();
    let worker = worker(history, batches);

    let init = worker.poll_workflow_activation().await.unwrap();
    let run_id = init.run_id.clone();
    complete(&worker, &run_id, vec![schedule_operation(1)]).await;

    let started = worker.poll_workflow_activation().await.unwrap();
    assert_eq!(job_names(&started), vec!["Start(1)"]);
    assert_eq!(started.is_replaying, replaying);
    complete(&worker, &run_id, Vec::<ScheduleNexusOperation>::new()).await;

    let first_progress = worker.poll_workflow_activation().await.unwrap();
    assert_eq!(
        progress_jobs(&first_progress),
        vec![job_for(1, &first)],
        "the progress-only task gets an activation of its own: {:?}",
        job_names(&first_progress)
    );
    assert_eq!(first_progress.is_replaying, replaying);
    complete(&worker, &run_id, Vec::<ScheduleNexusOperation>::new()).await;

    let second_progress = worker.poll_workflow_activation().await.unwrap();
    assert_eq!(progress_jobs(&second_progress), vec![job_for(1, &second)]);
    assert!(!second_progress.is_replaying);
    complete(&worker, &run_id, vec![CompleteWorkflowExecution::default()]).await;
    worker.drain_pollers_and_shutdown().await;
}

#[tokio::test]
async fn progress_reaches_lang_as_a_job_per_task_live() {
    run_progress_after_a_quiet_task(vec![1.into(), 2.into(), 3.into(), 4.into()], false).await;
}

#[tokio::test]
async fn progress_reaches_lang_in_the_same_activations_on_replay() {
    run_progress_after_a_quiet_task(vec![4.into()], true).await;
}

#[tokio::test]
async fn progress_follows_the_start_it_arrived_with() {
    let mut t = TestHistoryBuilder::default();
    t.add_by_type(EventType::WorkflowExecutionStarted);
    t.add_full_wf_task();
    let scheduled = add_operation_scheduled(&mut t);
    add_operation_started(&mut t, scheduled);
    let only = progress(scheduled, 7);
    t.add_workflow_task_scheduled_with_nexus_progress(vec![only.clone()]);
    t.add_workflow_task_started();
    let worker = worker(t, vec![1.into(), 2.into()]);

    let init = worker.poll_workflow_activation().await.unwrap();
    complete(&worker, &init.run_id, vec![schedule_operation(1)]).await;
    let next = worker.poll_workflow_activation().await.unwrap();
    assert_eq!(job_names(&next), vec!["Start(1)", "Progress(1, 7)"]);
    assert_eq!(progress_jobs(&next), vec![job_for(1, &only)]);
    complete(
        &worker,
        &init.run_id,
        vec![CompleteWorkflowExecution::default()],
    )
    .await;
    worker.drain_pollers_and_shutdown().await;
}

#[tokio::test]
async fn a_burst_across_a_failed_task_and_its_retry_is_one_job() {
    let mut t = TestHistoryBuilder::default();
    t.add_by_type(EventType::WorkflowExecutionStarted);
    t.add_full_wf_task();
    let scheduled = add_operation_scheduled(&mut t);
    add_operation_started(&mut t, scheduled);
    // The failed task's scheduled event and its retry's are applied together. The first holds two
    // entries for the same operation, out of order, to show the fold does not depend on the
    // server having folded already.
    t.add_workflow_task_scheduled_with_nexus_progress(vec![
        progress(scheduled, 4),
        progress(scheduled, 2),
    ]);
    t.add_workflow_task_started();
    t.add_workflow_task_failed_with_failure(
        temporalio_common::protos::temporal::api::enums::v1::WorkflowTaskFailedCause::Unspecified,
        Default::default(),
    );
    let latest = progress(scheduled, 6);
    t.add_workflow_task_scheduled_with_nexus_progress(vec![progress(scheduled, 5), latest.clone()]);
    t.add_workflow_task_started();
    let worker = worker(t, vec![1.into(), ResponseType::AllHistory]);

    let init = worker.poll_workflow_activation().await.unwrap();
    complete(&worker, &init.run_id, vec![schedule_operation(1)]).await;
    let burst = worker.poll_workflow_activation().await.unwrap();
    assert_eq!(job_names(&burst), vec!["Start(1)", "Progress(1, 6)"]);
    assert_eq!(progress_jobs(&burst), vec![job_for(1, &latest)]);
    complete(
        &worker,
        &init.run_id,
        vec![CompleteWorkflowExecution::default()],
    )
    .await;
    worker.drain_pollers_and_shutdown().await;
}

#[tokio::test]
async fn progress_for_an_unknown_closed_or_stale_operation_is_dropped() {
    let mut t = TestHistoryBuilder::default();
    t.add_by_type(EventType::WorkflowExecutionStarted);
    t.add_full_wf_task();
    let open = add_operation_scheduled(&mut t);
    let closing = add_operation_scheduled(&mut t);
    let timer = t.add_by_type(EventType::TimerStarted);
    add_operation_started(&mut t, open);
    add_operation_started(&mut t, closing);
    let delivered = progress(open, 5);
    t.add_workflow_task_scheduled_with_nexus_progress(vec![delivered.clone()]);
    t.add_workflow_task_started();
    t.add_workflow_task_completed();
    t.add(NexusOperationCompletedEventAttributes {
        scheduled_event_id: closing,
        ..Default::default()
    });
    t.add_workflow_task_scheduled_with_nexus_progress(vec![
        // Resolved in this same task, so lang gets the result and no progress.
        progress(closing, 1),
        // Lower than what lang already has.
        progress(open, 3),
        // No event with this id.
        progress(999, 1),
        // An event that is not a Nexus operation.
        progress(timer, 1),
    ]);
    t.add_workflow_task_started();
    let worker = worker(t, vec![1.into(), 2.into(), 3.into()]);

    let init = worker.poll_workflow_activation().await.unwrap();
    let run_id = init.run_id.clone();
    worker
        .complete_workflow_activation(WorkflowActivationCompletion::from_cmds(
            run_id.clone(),
            vec![
                schedule_operation(1).into(),
                schedule_operation(2).into(),
                start_timer_cmd(1, Duration::from_secs(60)),
            ],
        ))
        .await
        .unwrap();
    let started = worker.poll_workflow_activation().await.unwrap();
    assert_eq!(
        job_names(&started),
        vec!["Start(1)", "Start(2)", "Progress(1, 5)"]
    );
    complete(&worker, &run_id, Vec::<ScheduleNexusOperation>::new()).await;

    let last = worker.poll_workflow_activation().await.unwrap();
    assert_eq!(job_names(&last), vec!["Resolve(2)"]);
    complete(&worker, &run_id, vec![CompleteWorkflowExecution::default()]).await;
    worker.drain_pollers_and_shutdown().await;
}
