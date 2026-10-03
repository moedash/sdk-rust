use crate::{
    init_replay_worker,
    replay::{HistoryFeeder, HistoryForReplay, ReplayWorkerInput, TestHistoryBuilder},
    test_help::{MockPollCfg, ResponseType, build_mock_pollers, mock_worker, test_worker_cfg},
    worker::client::mocks::mock_worker_client,
};
use temporalio_common::protos::{
    coresdk::{
        workflow_activation::{WorkflowActivation, workflow_activation_job},
        workflow_commands::CompleteWorkflowExecution,
        workflow_completion::WorkflowActivationCompletion,
    },
    temporal::api::{
        enums::v1::{EventType, WorkflowTaskFailedCause},
        failure::v1::Failure,
        notification::v1::Notification,
    },
};

fn notification(channel: &str, counter: i64) -> Notification {
    Notification {
        channel: channel.to_string(),
        position: format!("pos-{counter}").into_bytes(),
        counter,
        ..Default::default()
    }
}

/// The notification jobs in an activation, each as the notifications it carries.
fn received(task: &WorkflowActivation) -> Vec<Vec<Notification>> {
    task.jobs
        .iter()
        .filter_map(|j| match j.variant.as_ref() {
            Some(workflow_activation_job::Variant::NotificationsReceived(n)) => {
                Some(n.notifications.clone())
            }
            _ => None,
        })
        .collect()
}

/// A short name for each job, so a test can assert their order.
fn job_kinds(task: &WorkflowActivation) -> Vec<&'static str> {
    task.jobs
        .iter()
        .map(|j| match j.variant.as_ref().unwrap() {
            workflow_activation_job::Variant::InitializeWorkflow(_) => "init",
            workflow_activation_job::Variant::NotificationsReceived(_) => "notifications",
            _ => "other",
        })
        .collect()
}

/// A run whose only task was scheduled with notifications for two channels.
fn one_task_with_notifications() -> TestHistoryBuilder {
    let mut t = TestHistoryBuilder::default();
    t.add_by_type(EventType::WorkflowExecutionStarted);
    t.add_workflow_task_scheduled_with_notifications(vec![
        notification("orders", 3),
        notification("invoices", 7),
    ]);
    t.add_workflow_task_started();
    t
}

/// Replaying the same history yields the same job. Nothing is re-supplied: the
/// notifications are on the event, so History alone carries them.
#[tokio::test]
async fn a_replayed_history_yields_the_same_notifications() {
    let mut t = one_task_with_notifications();
    t.add_workflow_task_completed();
    t.add_workflow_execution_completed();

    let (feeder, stream) = HistoryFeeder::new(1);
    feeder
        .feed(HistoryForReplay::new(
            t.get_full_history_info().unwrap(),
            "wfid",
        ))
        .await
        .unwrap();
    let core = init_replay_worker(ReplayWorkerInput::new(
        test_worker_cfg().build().unwrap(),
        stream,
    ))
    .unwrap();

    let task = core.poll_workflow_activation().await.unwrap();
    assert!(task.is_replaying);
    assert_eq!(job_kinds(&task), vec!["init", "notifications"]);
    assert_eq!(
        received(&task),
        vec![vec![notification("orders", 3), notification("invoices", 7)]]
    );
    core.complete_workflow_activation(WorkflowActivationCompletion::from_cmds(
        task.run_id,
        vec![CompleteWorkflowExecution { result: None }.into()],
    ))
    .await
    .unwrap();
    drop(feeder);
    core.shutdown().await;
}

/// On a later task the job lands in that task's activation and not in an
/// earlier one, also when the worker replays its way there. The first task
/// issued no command, which replay would otherwise read as a heartbeat and
/// fold together with the next one.
#[tokio::test]
async fn notifications_belong_to_the_task_whose_scheduled_event_carries_them() {
    let mut t = TestHistoryBuilder::default();
    t.add_by_type(EventType::WorkflowExecutionStarted);
    t.add_full_wf_task();
    t.add_workflow_task_scheduled_with_notifications(vec![notification("orders", 1)]);
    t.add_workflow_task_started();
    t.add_workflow_task_completed();
    t.add_we_signaled("sig", vec![]);
    t.add_workflow_task_scheduled_and_started();

    let mock =
        MockPollCfg::from_resp_batches("wfid", t, [ResponseType::AllHistory], mock_worker_client());
    let core = mock_worker(build_mock_pollers(mock));

    let task = core.poll_workflow_activation().await.unwrap();
    assert!(received(&task).is_empty());
    core.complete_workflow_activation(WorkflowActivationCompletion::empty(task.run_id))
        .await
        .unwrap();

    let task = core.poll_workflow_activation().await.unwrap();
    assert!(task.is_replaying);
    assert_eq!(received(&task), vec![vec![notification("orders", 1)]]);
    core.complete_workflow_activation(WorkflowActivationCompletion::empty(task.run_id))
        .await
        .unwrap();

    let task = core.poll_workflow_activation().await.unwrap();
    assert!(received(&task).is_empty());
    core.complete_workflow_activation(WorkflowActivationCompletion::empty(task.run_id))
        .await
        .unwrap();
}

/// A scheduled event without notifications produces no job, not an empty one.
#[tokio::test]
async fn a_scheduled_event_without_notifications_yields_no_job() {
    let mut t = TestHistoryBuilder::default();
    t.add_by_type(EventType::WorkflowExecutionStarted);
    t.add_workflow_task_scheduled_and_started();

    let mock =
        MockPollCfg::from_resp_batches("wfid", t, [ResponseType::AllHistory], mock_worker_client());
    let core = mock_worker(build_mock_pollers(mock));

    let task = core.poll_workflow_activation().await.unwrap();
    assert_eq!(job_kinds(&task), vec!["init"]);
    core.complete_workflow_activation(WorkflowActivationCompletion::empty(task.run_id))
        .await
        .unwrap();
}

/// The first activation a replay worker makes of a history that ends with the run
/// completing, and that worker, so the test can finish the run on it.
async fn replayed_first_activation(
    mut t: TestHistoryBuilder,
) -> (crate::Worker, HistoryFeeder, WorkflowActivation) {
    t.add_workflow_task_completed();
    t.add_workflow_execution_completed();
    let (feeder, stream) = HistoryFeeder::new(1);
    feeder
        .feed(HistoryForReplay::new(
            t.get_full_history_info().unwrap(),
            "wfid",
        ))
        .await
        .unwrap();
    let core = init_replay_worker(ReplayWorkerInput::new(
        test_worker_cfg().build().unwrap(),
        stream,
    ))
    .unwrap();
    let task = core.poll_workflow_activation().await.unwrap();
    (core, feeder, task)
}

/// The first activation of a history served live, as one poll response.
async fn live_first_activation(t: TestHistoryBuilder) -> WorkflowActivation {
    let mock =
        MockPollCfg::from_resp_batches("wfid", t, [ResponseType::AllHistory], mock_worker_client());
    let core = mock_worker(build_mock_pollers(mock));
    let task = core.poll_workflow_activation().await.unwrap();
    core.complete_workflow_activation(WorkflowActivationCompletion::empty(task.run_id.clone()))
        .await
        .unwrap();
    task
}

/// A first task that failed, scheduled with one notification on channel A, and its
/// retry, scheduled with a newer one on A and one on B.
fn failed_task_then_retry() -> TestHistoryBuilder {
    let mut t = TestHistoryBuilder::default();
    t.add_by_type(EventType::WorkflowExecutionStarted);
    t.add_workflow_task_scheduled_with_notifications(vec![notification("a", 1)]);
    t.add_workflow_task_started();
    t.add_workflow_task_failed_with_failure(
        WorkflowTaskFailedCause::Unspecified,
        Failure::default(),
    );
    t.add_workflow_task_scheduled_with_notifications(vec![
        notification("a", 5),
        notification("b", 2),
    ]);
    t.add_workflow_task_started();
    t
}

/// The server clears what it put on a scheduled event, so the failed task's
/// notification is only in History. The retry's activation gets it folded with
/// the retry's own, one per channel, live and on replay alike.
#[tokio::test]
async fn a_failed_task_and_its_retry_yield_one_job_folded_per_channel() {
    let expected = vec![vec![notification("a", 5), notification("b", 2)]];

    let task = live_first_activation(failed_task_then_retry()).await;
    assert!(!task.is_replaying);
    assert_eq!(job_kinds(&task), vec!["init", "notifications"]);
    assert_eq!(received(&task), expected);

    let (core, feeder, task) = replayed_first_activation(failed_task_then_retry()).await;
    assert!(task.is_replaying);
    assert_eq!(job_kinds(&task), vec!["init", "notifications"]);
    assert_eq!(received(&task), expected);
    core.complete_workflow_activation(WorkflowActivationCompletion::from_cmds(
        task.run_id,
        vec![CompleteWorkflowExecution { result: None }.into()],
    ))
    .await
    .unwrap();
    drop(feeder);
    core.shutdown().await;
}

/// A notification with a lower counter than the one already held for its channel
/// does not replace it, and channels keep the order they first appeared in.
#[tokio::test]
async fn a_lower_counter_does_not_replace_the_held_notification() {
    let mut t = TestHistoryBuilder::default();
    t.add_by_type(EventType::WorkflowExecutionStarted);
    t.add_workflow_task_scheduled_with_notifications(vec![notification("a", 5)]);
    t.add_workflow_task_started();
    t.add_workflow_task_timed_out();
    t.add_workflow_task_scheduled_with_notifications(vec![
        notification("b", 1),
        notification("a", 3),
    ]);
    t.add_workflow_task_started();

    let task = live_first_activation(t).await;
    assert_eq!(
        received(&task),
        vec![vec![notification("a", 5), notification("b", 1)]]
    );
}

/// On a counter tie the notification already held for the channel stays.
#[tokio::test]
async fn a_counter_tie_keeps_the_held_notification() {
    let held = Notification {
        position: b"held".to_vec(),
        ..notification("a", 4)
    };
    let tied = Notification {
        position: b"tied".to_vec(),
        ..notification("a", 4)
    };
    let mut t = TestHistoryBuilder::default();
    t.add_by_type(EventType::WorkflowExecutionStarted);
    t.add_workflow_task_scheduled_with_notifications(vec![held.clone()]);
    t.add_workflow_task_started();
    t.add_workflow_task_timed_out();
    t.add_workflow_task_scheduled_with_notifications(vec![tied]);
    t.add_workflow_task_started();

    let task = live_first_activation(t).await;
    assert_eq!(received(&task), vec![vec![held]]);
}
