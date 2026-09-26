//! Delivery of server-side stream ranges to a workflow.
//!
//! History records the offsets a task consumed and never the payloads, so the
//! server sends the bytes on the poll response: untagged for the task about to
//! run, and tagged with a WorkflowTaskCompleted event id when it is
//! re-supplying what an earlier task consumed. These tests pin that both
//! arrive, in the right order, and that an empty range is still delivered.

use crate::{
    Worker, init_replay_worker,
    replay::{HistoryFeeder, HistoryForReplay, ReplayWorkerInput, TestHistoryBuilder},
    test_help::{
        MockPollCfg, PollWFTRespExt, ResponseType, WorkerTestHelpers, build_mock_pollers,
        hist_to_poll_resp, mock_worker, test_worker_cfg,
    },
    worker::client::mocks::mock_worker_client,
};
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};
use temporalio_common::protos::{
    coresdk::{
        workflow_activation::{
            RemoveFromCache, WorkflowActivation, WorkflowActivationJob,
            remove_from_cache::EvictionReason, workflow_activation_job,
        },
        workflow_commands::{AppendStreamRecords, CompleteWorkflowExecution, SubscribeStream},
        workflow_completion::WorkflowActivationCompletion,
    },
    temporal::api::{
        command::v1::command,
        enums::v1::{CommandType, EventType, WorkflowTaskFailedCause},
        history::v1::{History, HistoryEvent},
        query::v1::WorkflowQuery,
        stream::v1::{StreamRange, StreamRecord, StreamSlice},
        workflowservice::v1::{
            GetWorkflowExecutionHistoryResponse, RespondWorkflowTaskCompletedResponse,
        },
    },
};

fn cursor(stream_id: &str, from: i64, to: i64) -> StreamRange {
    StreamRange {
        stream_id: stream_id.to_string(),
        from_offset: from,
        to_offset: to,
    }
}

fn delivered(job: &WorkflowActivationJob) -> (&str, i64, i64, Vec<&[u8]>) {
    match job.variant.as_ref().unwrap() {
        workflow_activation_job::Variant::DeliverStreamRecords(d) => (
            d.stream_id.as_str(),
            d.from_offset,
            d.to_offset,
            d.records
                .iter()
                .map(|m| m.body.as_ref().unwrap().data.as_slice())
                .collect(),
        ),
        other => panic!("expected a stream delivery, got {other:?}"),
    }
}

/// The stream deliveries in an activation, as (stream, from, to).
fn delivered_ranges(task: &WorkflowActivation) -> Vec<(String, i64, i64)> {
    task.jobs
        .iter()
        .filter(|j| {
            matches!(
                j.variant,
                Some(workflow_activation_job::Variant::DeliverStreamRecords(_))
            )
        })
        .map(|j| {
            let (stream, from, to, _) = delivered(j);
            (stream.to_string(), from, to)
        })
        .collect()
}

/// A worker served one poll response, expected to fail that task as
/// nondeterministic. Returns the worker and the count of failures it reported,
/// which the test asserts itself: the mock only verifies call counts when it
/// is dropped, and a missing failure would otherwise go unnoticed. The task
/// stream is kept open so the eviction that follows the failure can be polled.
fn worker_expecting_one_nondeterminism_failure(
    t: TestHistoryBuilder,
    resp: ResponseType,
) -> (Worker, Arc<AtomicUsize>) {
    worker_expecting_one_failure(t, resp, WorkflowTaskFailedCause::NonDeterministicError)
}

fn worker_expecting_one_failure(
    t: TestHistoryBuilder,
    resp: ResponseType,
    expected: WorkflowTaskFailedCause,
) -> (Worker, Arc<AtomicUsize>) {
    let failures = Arc::new(AtomicUsize::new(0));
    let counted = failures.clone();
    let mut mock = MockPollCfg::from_resp_batches("wfid", t, [resp], mock_worker_client());
    mock.num_expected_fails = 1;
    mock.expect_fail_wft_matcher = Box::new(move |_, cause, _| {
        counted.fetch_add(1, Ordering::Relaxed);
        *cause == expected
    });
    let mut mock = build_mock_pollers(mock);
    mock.make_wft_stream_interminable();
    (mock_worker(mock), failures)
}

/// A worker whose only assertion is that nothing is rejected. Core signals a
/// reissued command it will not accept by failing the workflow task, so turning
/// that into a panic is how a test says the command was accepted.
fn worker_rejecting_any_failure(t: TestHistoryBuilder) -> Worker {
    let mut mock_client = mock_worker_client();
    mock_client
        .expect_complete_workflow_task()
        .returning(|_, _| Ok(RespondWorkflowTaskCompletedResponse::default()));
    mock_client
        .expect_fail_workflow_task()
        .returning(|_, _, f| panic!("core rejected a reissued command: {f:?}"));
    let mock = MockPollCfg::from_resp_batches("wfid", t, [ResponseType::AllHistory], mock_client);
    mock_worker(build_mock_pollers(mock))
}

fn history_page(
    events: &[HistoryEvent],
    next_page_token: Vec<u8>,
) -> GetWorkflowExecutionHistoryResponse {
    GetWorkflowExecutionHistoryResponse {
        history: Some(History {
            events: events.to_vec(),
        }),
        next_page_token,
        ..Default::default()
    }
}

#[tokio::test]
async fn delivers_the_range_for_the_current_task() {
    let mut t = TestHistoryBuilder::default();
    t.add_by_type(EventType::WorkflowExecutionStarted);
    t.add_workflow_task_scheduled_and_started();

    let mut poll_resp = hist_to_poll_resp(&t, "wfid".to_owned(), ResponseType::AllHistory);
    poll_resp.add_stream_slice("s1", 0, 0, &["alpha", "beta"]);

    let mock = MockPollCfg::from_resp_batches(
        "wfid",
        t,
        [ResponseType::Raw(poll_resp.resp)],
        mock_worker_client(),
    );
    let core = mock_worker(build_mock_pollers(mock));

    let task = core.poll_workflow_activation().await.unwrap();
    let stream_jobs: Vec<_> = task
        .jobs
        .iter()
        .filter(|j| {
            matches!(
                j.variant,
                Some(workflow_activation_job::Variant::DeliverStreamRecords(_))
            )
        })
        .collect();
    assert_eq!(stream_jobs.len(), 1);
    assert_eq!(
        delivered(stream_jobs[0]),
        ("s1", 0, 2, vec![b"alpha".as_slice(), b"beta".as_slice()])
    );

    core.complete_workflow_activation(WorkflowActivationCompletion::empty(task.run_id))
        .await
        .unwrap();
}

// A task where the subscription saw nothing is a fact replay has to reproduce,
// so the range still has to arrive rather than being dropped as uninteresting.
#[tokio::test]
async fn delivers_an_empty_range() {
    let mut t = TestHistoryBuilder::default();
    t.add_by_type(EventType::WorkflowExecutionStarted);
    t.add_workflow_task_scheduled_and_started();

    let mut poll_resp = hist_to_poll_resp(&t, "wfid".to_owned(), ResponseType::AllHistory);
    poll_resp.add_stream_slice("s1", 0, 4, &[]);

    let mock = MockPollCfg::from_resp_batches(
        "wfid",
        t,
        [ResponseType::Raw(poll_resp.resp)],
        mock_worker_client(),
    );
    let core = mock_worker(build_mock_pollers(mock));

    let task = core.poll_workflow_activation().await.unwrap();
    let job = task
        .jobs
        .iter()
        .find(|j| {
            matches!(
                j.variant,
                Some(workflow_activation_job::Variant::DeliverStreamRecords(_))
            )
        })
        .expect("an empty range is still delivered");
    assert_eq!(delivered(job), ("s1", 4, 4, vec![]));

    core.complete_workflow_activation(WorkflowActivationCompletion::empty(task.run_id))
        .await
        .unwrap();
}

// On a cache miss every prior task replays, so ranges those tasks consumed have
// to be handed back in the order they were consumed, before the range for the
// task about to run.
#[tokio::test]
async fn replays_recorded_ranges_in_order_before_the_current_one() {
    let mut t = TestHistoryBuilder::default();
    t.add_by_type(EventType::WorkflowExecutionStarted);
    t.add_workflow_task_scheduled_and_started();
    let first_completed =
        t.add_workflow_task_completed_with_consumed_stream_ranges(vec![cursor("s1", 0, 2)]);
    t.add_workflow_task_scheduled_and_started();
    let second_completed =
        t.add_workflow_task_completed_with_consumed_stream_ranges(vec![cursor("s1", 2, 3)]);
    t.add_workflow_task_scheduled_and_started();

    let mut poll_resp = hist_to_poll_resp(&t, "wfid".to_owned(), ResponseType::AllHistory);
    // Deliberately out of order, to prove the ordering comes from the events
    // rather than from however the server happened to lay them out.
    poll_resp.add_stream_slice("s1", second_completed, 2, &["gamma"]);
    poll_resp.add_stream_slice("s1", 0, 3, &["delta"]);
    poll_resp.add_stream_slice("s1", first_completed, 0, &["alpha", "beta"]);

    let mock = MockPollCfg::from_resp_batches(
        "wfid",
        t,
        [ResponseType::Raw(poll_resp.resp)],
        mock_worker_client(),
    );
    let mut mock = build_mock_pollers(mock);
    mock.worker_cfg(|wc| wc.max_cached_workflows = 1);
    let core = mock_worker(mock);

    // Each entry is (activation, from, to, bodies). The activation matters as
    // much as the order: a range that came back one activation late would
    // still be in order.
    let mut seen = vec![];
    for activation in 1..=3 {
        let task = core.poll_workflow_activation().await.unwrap();
        for job in &task.jobs {
            if matches!(
                job.variant,
                Some(workflow_activation_job::Variant::DeliverStreamRecords(_))
            ) {
                let (_, from, to, bodies) = delivered(job);
                seen.push((
                    activation,
                    from,
                    to,
                    bodies.iter().map(|b| b.to_vec()).collect::<Vec<_>>(),
                ));
            }
        }
        core.complete_workflow_activation(WorkflowActivationCompletion::empty(task.run_id))
            .await
            .unwrap();
    }

    assert_eq!(
        seen,
        vec![
            (1, 0, 2, vec![b"alpha".to_vec(), b"beta".to_vec()]),
            (2, 2, 3, vec![b"gamma".to_vec()]),
            (3, 3, 4, vec![b"delta".to_vec()]),
        ],
        "each recorded range comes back in the activation of the task that consumed it, \
         then the live one"
    );
}

// A workflow subscribing itself. The command exists at all because every SDK
// matches issued commands against command-generated events in order, so a
// command producing no event would put that matching out of step. This asserts
// the command goes out and that replaying its event does not trip that check.
#[tokio::test]
async fn subscribe_command_round_trips_through_replay() {
    let mut t = TestHistoryBuilder::default();
    t.add_by_type(EventType::WorkflowExecutionStarted);
    t.add_full_wf_task();
    t.add_stream_subscribed("s1", 4);
    t.add_full_wf_task();

    let mut mock_client = mock_worker_client();
    // A replayed task sends no commands, so there is nothing to assert on the
    // completion here. Rejection is what this test watches for, and core
    // signals that by failing the task.
    mock_client
        .expect_complete_workflow_task()
        .returning(|_, _| Ok(RespondWorkflowTaskCompletedResponse::default()));
    mock_client
        .expect_fail_workflow_task()
        .returning(|_, _, f| panic!("core rejected the reissued subscribe: {f:?}"));

    let mock = MockPollCfg::from_resp_batches("wfid", t, [ResponseType::AllHistory], mock_client);
    let core = mock_worker(build_mock_pollers(mock));

    // Full history, so this activation replays the recorded subscription. Lang
    // reissues the command, and core has to match it to that event rather than
    // calling it nondeterministic.
    let task = core.poll_workflow_activation().await.unwrap();
    core.complete_workflow_activation(WorkflowActivationCompletion::from_cmds(
        task.run_id,
        vec![
            SubscribeStream {
                stream_name_or_id: "s1".to_string(),
                start_offset: -1,
            }
            .into(),
        ],
    ))
    .await
    .unwrap();
}

/// The command has to reach the server carrying the bodies.
///
/// Only a task that is not being replayed sends commands, so this drives a
/// single open Workflow Task rather than a recorded history.
#[tokio::test]
async fn publish_command_reaches_the_server_with_its_payloads() {
    let mut t = TestHistoryBuilder::default();
    t.add_by_type(EventType::WorkflowExecutionStarted);
    t.add_workflow_task_scheduled_and_started();

    let mut mock_client = mock_worker_client();
    mock_client
        .expect_complete_workflow_task()
        .times(1)
        .returning(|resp, _| {
            let cmd = resp.commands.first().expect("a command was sent");
            assert_eq!(cmd.command_type(), CommandType::AppendStreamRecords);
            match cmd.attributes.as_ref().unwrap() {
                command::Attributes::AppendStreamRecordsCommandAttributes(a) => {
                    assert_eq!(a.stream_name, "s1");
                    // The bodies are the half of the batch History never sees,
                    // so the command is the only thing that can carry them.
                    let bodies: Vec<_> = a
                        .records
                        .iter()
                        .map(|m| m.body.as_ref().unwrap().data.clone())
                        .collect();
                    assert_eq!(bodies, vec![b"one".to_vec(), b"two".to_vec()]);
                }
                other => panic!("wrong attributes: {other:?}"),
            }
            Ok(RespondWorkflowTaskCompletedResponse::default())
        });

    let mock = MockPollCfg::from_resp_batches("wfid", t, [ResponseType::AllHistory], mock_client);
    let core = mock_worker(build_mock_pollers(mock));

    let task = core.poll_workflow_activation().await.unwrap();
    core.complete_workflow_activation(WorkflowActivationCompletion::from_cmds(
        task.run_id,
        vec![publish_two("s1").into()],
    ))
    .await
    .unwrap();
    core.shutdown().await;
}

/// A publish reissued on replay has to match the event the original run wrote.
///
/// This is the property the event exists for. Core pops one queued command per
/// command-generated event, so a publish producing none would leave every later
/// command matched against the wrong event. Core signals the mismatch by
/// failing the workflow task, which the mock turns into a panic.
#[tokio::test]
async fn publish_command_round_trips_through_replay() {
    let mut t = TestHistoryBuilder::default();
    t.add_by_type(EventType::WorkflowExecutionStarted);
    t.add_full_wf_task();
    t.add_stream_records_appended("s1", 0, 2);
    t.add_full_wf_task();

    let mut mock_client = mock_worker_client();
    mock_client
        .expect_complete_workflow_task()
        .returning(|_, _| Ok(RespondWorkflowTaskCompletedResponse::default()));
    mock_client
        .expect_fail_workflow_task()
        .returning(|_, _, f| panic!("core rejected the reissued publish: {f:?}"));

    let mock = MockPollCfg::from_resp_batches("wfid", t, [ResponseType::AllHistory], mock_client);
    let core = mock_worker(build_mock_pollers(mock));

    // First activation replays the recorded publish: lang reissues it, and core
    // has to match it to that event rather than calling it nondeterministic.
    let task = core.poll_workflow_activation().await.unwrap();
    core.complete_workflow_activation(WorkflowActivationCompletion::from_cmds(
        task.run_id,
        vec![publish_two("s1").into()],
    ))
    .await
    .unwrap();

    core.shutdown().await;
}

/// The subscribe command has to reach the server, which only a task that is
/// not being replayed will send.
#[tokio::test]
async fn subscribe_command_reaches_the_server() {
    let mut t = TestHistoryBuilder::default();
    t.add_by_type(EventType::WorkflowExecutionStarted);
    t.add_workflow_task_scheduled_and_started();

    let mut mock_client = mock_worker_client();
    mock_client
        .expect_complete_workflow_task()
        .times(1)
        .returning(|resp, _| {
            let cmd = resp.commands.first().expect("a command was sent");
            assert_eq!(cmd.command_type(), CommandType::SubscribeStream);
            match cmd.attributes.as_ref().unwrap() {
                command::Attributes::SubscribeStreamCommandAttributes(a) => {
                    assert_eq!(a.stream_name_or_id, "s1");
                    // Passed through unresolved: the server turns it into a
                    // real offset and records that.
                    assert_eq!(a.start_offset, -1);
                }
                other => panic!("wrong attributes: {other:?}"),
            }
            Ok(RespondWorkflowTaskCompletedResponse::default())
        });

    let mock = MockPollCfg::from_resp_batches("wfid", t, [ResponseType::AllHistory], mock_client);
    let core = mock_worker(build_mock_pollers(mock));

    let task = core.poll_workflow_activation().await.unwrap();
    core.complete_workflow_activation(WorkflowActivationCompletion::from_cmds(
        task.run_id,
        vec![
            SubscribeStream {
                stream_name_or_id: "s1".to_string(),
                start_offset: -1,
            }
            .into(),
        ],
    ))
    .await
    .unwrap();
    core.shutdown().await;
}

fn publish_two(stream_name: &str) -> AppendStreamRecords {
    AppendStreamRecords {
        stream_name: stream_name.to_string(),
        records: vec![
            StreamRecord {
                body: Some(b"one".to_vec().into()),
                ..Default::default()
            },
            StreamRecord {
                body: Some(b"two".to_vec().into()),
                ..Default::default()
            },
        ],
    }
}

/// A task that reads a range and publishes because of what it read.
///
/// This is the shape the product requires: workflow code observes stream input,
/// decides, and writes. Replaying it means the workflow has to be handed the
/// input again *before* core matches the command that input caused, otherwise
/// lang has nothing to decide from and reissues nothing.
///
/// The recorded range lives on the WorkflowTaskCompleted that closes the task,
/// which is the event *after* the one that started it. So the range for the
/// task about to be replayed is only visible by looking ahead, and a lookahead
/// that reads the completion for its flags but not for its cursors delivers the
/// input one activation too late.
#[tokio::test]
async fn read_then_publish_replays_when_the_range_is_only_visible_by_lookahead() {
    let mut t = TestHistoryBuilder::default();
    t.add_by_type(EventType::WorkflowExecutionStarted);
    t.add_workflow_task_scheduled_and_started();
    // The task that reads [0,1) and publishes because of it.
    let read_completed =
        t.add_workflow_task_completed_with_consumed_stream_ranges(vec![cursor("in", 0, 1)]);
    t.add_stream_records_appended("out", 0, 1);
    t.add_workflow_task_scheduled_and_started();

    let mut poll_resp = hist_to_poll_resp(&t, "wfid".to_owned(), ResponseType::AllHistory);
    poll_resp.add_stream_slice("in", read_completed, 0, &["go"]);

    let mut mock_client = mock_worker_client();
    mock_client
        .expect_complete_workflow_task()
        .returning(|_, _| Ok(RespondWorkflowTaskCompletedResponse::default()));
    mock_client
        .expect_fail_workflow_task()
        .returning(|_, _, f| panic!("core rejected the reissued read-caused publish: {f:?}"));

    let mock =
        MockPollCfg::from_resp_batches("wfid", t, [ResponseType::Raw(poll_resp.resp)], mock_client);
    let mut mock = build_mock_pollers(mock);
    // Cold: nothing cached, so this is reconstruction from History.
    mock.worker_cfg(|wc| wc.max_cached_workflows = 0);
    let core = mock_worker(mock);

    let task = core.poll_workflow_activation().await.unwrap();
    let got_input = task.jobs.iter().any(|j| {
        matches!(
            j.variant,
            Some(workflow_activation_job::Variant::DeliverStreamRecords(_))
        )
    });
    assert!(
        got_input,
        "the replayed task must receive the range it consumed before its \
         resulting publish is matched; jobs were {:?}",
        task.jobs
    );

    core.complete_workflow_activation(WorkflowActivationCompletion::from_cmds(
        task.run_id,
        vec![
            AppendStreamRecords {
                stream_name: "out".to_string(),
                records: vec![StreamRecord {
                    body: Some(b"accept".to_vec().into()),
                    ..Default::default()
                }],
            }
            .into(),
        ],
    ))
    .await
    .unwrap();

    core.shutdown().await;
}

/// The real shape: a subscribe task with no consumed range, then a task that
/// reads and publishes, then a third task replaying both.
///
/// The first completion carries no cursors, so the lookahead that finds the
/// range has to keep looking past it rather than stopping at the first
/// completion it sees.
#[tokio::test]
async fn read_then_publish_replays_after_a_task_that_consumed_nothing() {
    let mut t = TestHistoryBuilder::default();
    t.add_by_type(EventType::WorkflowExecutionStarted);
    t.add_workflow_task_scheduled_and_started();
    // Task 1 subscribes and consumes nothing.
    t.add_workflow_task_completed();
    t.add_stream_subscribed("in", 0);
    t.add_workflow_task_scheduled_and_started();
    // Task 2 reads [0,3) and publishes because of it.
    let read_completed =
        t.add_workflow_task_completed_with_consumed_stream_ranges(vec![cursor("in", 0, 3)]);
    t.add_stream_records_appended("out", 0, 1);
    t.add_workflow_task_scheduled_and_started();

    let mut poll_resp = hist_to_poll_resp(&t, "wfid".to_owned(), ResponseType::AllHistory);
    poll_resp.add_stream_slice("in", read_completed, 0, &["a", "b", "c"]);

    let mut mock_client = mock_worker_client();
    mock_client
        .expect_complete_workflow_task()
        .returning(|_, _| Ok(RespondWorkflowTaskCompletedResponse::default()));
    mock_client
        .expect_fail_workflow_task()
        .returning(|_, _, f| panic!("core rejected the replayed read-caused publish: {f:?}"));

    let mock =
        MockPollCfg::from_resp_batches("wfid", t, [ResponseType::Raw(poll_resp.resp)], mock_client);
    let mut mock = build_mock_pollers(mock);
    mock.worker_cfg(|wc| wc.max_cached_workflows = 0);
    let core = mock_worker(mock);

    // Task 1: subscribe, no input yet.
    let task = core.poll_workflow_activation().await.unwrap();
    core.complete_workflow_activation(WorkflowActivationCompletion::from_cmds(
        task.run_id,
        vec![
            SubscribeStream {
                stream_name_or_id: "in".to_string(),
                start_offset: 0,
            }
            .into(),
        ],
    ))
    .await
    .unwrap();

    // Task 2: the recorded range has to arrive before its publish is matched.
    let task = core.poll_workflow_activation().await.unwrap();
    let bodies: Vec<Vec<u8>> = task
        .jobs
        .iter()
        .filter(|j| {
            matches!(
                j.variant,
                Some(workflow_activation_job::Variant::DeliverStreamRecords(_))
            )
        })
        .flat_map(|j| delivered(j).3.into_iter().map(|b| b.to_vec()))
        .collect();
    assert_eq!(
        bodies,
        vec![b"a".to_vec(), b"b".to_vec(), b"c".to_vec()],
        "the replayed task must be handed the range it consumed; jobs were {:?}",
        task.jobs
    );
    core.complete_workflow_activation(WorkflowActivationCompletion::from_cmds(
        task.run_id,
        vec![
            AppendStreamRecords {
                stream_name: "out".to_string(),
                records: vec![StreamRecord {
                    body: Some(b"accept".to_vec().into()),
                    ..Default::default()
                }],
            }
            .into(),
        ],
    ))
    .await
    .unwrap();

    core.shutdown().await;
}

/// The reading task's range has to arrive in its own activation when History
/// comes in pages and a page boundary falls at that task.
///
/// The range is recorded on the completion that closes the task, and the
/// paginator hands the machines updates cut at WFT started events. Whether the
/// boundary lands right after the reading task's started event or right after
/// its completion, the completion is outside the update the machines are
/// replaying from, and a lookahead reading only that update would find nothing.
#[rstest::rstest]
#[tokio::test]
async fn read_then_publish_replays_across_a_page_boundary(
    #[values(12, 13)] second_page_end: usize,
) {
    let mut t = TestHistoryBuilder::default();
    t.add_by_type(EventType::WorkflowExecutionStarted);
    t.add_full_wf_task(); // 3
    t.add_we_signaled("go", vec![]);
    t.add_full_wf_task(); // 7
    // Task 2 subscribes. Its command event is what lets the paginator tell the
    // reading task's sequence is complete once it sees the started event.
    t.add_stream_subscribed("in", 0);
    t.add_we_signaled("go", vec![]);
    t.add_workflow_task_scheduled_and_started(); // 12
    // Task 3 reads [0,1) and publishes because of it.
    let read_completed =
        t.add_workflow_task_completed_with_consumed_stream_ranges(vec![cursor("in", 0, 1)]);
    t.add_stream_records_appended("out", 0, 1);
    t.add_workflow_task_scheduled_and_started(); // 16

    let events = t.get_full_history_info().unwrap().into_events();
    let mut poll_resp = hist_to_poll_resp(&t, "wfid".to_owned(), ResponseType::AllHistory);
    poll_resp.history.as_mut().unwrap().events.truncate(3);
    poll_resp.next_page_token = vec![1];
    // Two complete tasks past the previous started id fit in the first two
    // pages, so the paginator would hand them over before fetching the third.
    poll_resp.previous_started_event_id = 3;
    poll_resp.add_stream_slice("in", read_completed, 0, &["go"]);
    poll_resp.add_stream_slice("in", 0, 1, &["next"]);

    let second_page = history_page(&events[3..second_page_end], vec![2]);
    let third_page = history_page(&events[second_page_end..], vec![]);
    let mut mock_client = mock_worker_client();
    mock_client
        .expect_get_workflow_execution_history()
        .returning(move |_, _, token| match token.as_slice() {
            [1] => Ok(second_page.clone()),
            [2] => Ok(third_page.clone()),
            other => panic!("unexpected page token {other:?}"),
        });
    mock_client
        .expect_fail_workflow_task()
        .returning(|_, _, f| panic!("core rejected the replayed read-caused publish: {f:?}"));

    let mock =
        MockPollCfg::from_resp_batches("wfid", t, [ResponseType::Raw(poll_resp.resp)], mock_client);
    let core = mock_worker(build_mock_pollers(mock));

    let task = core.poll_workflow_activation().await.unwrap();
    assert_eq!(delivered_ranges(&task), vec![]);
    core.complete_workflow_activation(WorkflowActivationCompletion::empty(task.run_id))
        .await
        .unwrap();

    let task = core.poll_workflow_activation().await.unwrap();
    assert_eq!(delivered_ranges(&task), vec![]);
    core.complete_workflow_activation(WorkflowActivationCompletion::from_cmds(
        task.run_id,
        vec![
            SubscribeStream {
                stream_name_or_id: "in".to_string(),
                start_offset: 0,
            }
            .into(),
        ],
    ))
    .await
    .unwrap();

    // The reading task. Its signal and its range belong to the same activation.
    let task = core.poll_workflow_activation().await.unwrap();
    assert!(task.is_replaying);
    assert!(
        task.jobs.iter().any(|j| matches!(
            j.variant,
            Some(workflow_activation_job::Variant::SignalWorkflow(_))
        )),
        "expected the reading task's signal; jobs were {:?}",
        task.jobs
    );
    assert_eq!(
        delivered_ranges(&task),
        vec![("in".to_string(), 0, 1)],
        "the replayed task must be handed the range it consumed; jobs were {:?}",
        task.jobs
    );
    core.complete_workflow_activation(WorkflowActivationCompletion::from_cmds(
        task.run_id,
        vec![
            AppendStreamRecords {
                stream_name: "out".to_string(),
                records: vec![StreamRecord {
                    body: Some(b"accept".to_vec().into()),
                    ..Default::default()
                }],
            }
            .into(),
        ],
    ))
    .await
    .unwrap();

    // The live task gets only its own range.
    let task = core.poll_workflow_activation().await.unwrap();
    assert!(!task.is_replaying);
    assert_eq!(delivered_ranges(&task), vec![("in".to_string(), 1, 2)]);
    core.complete_workflow_activation(WorkflowActivationCompletion::empty(task.run_id))
        .await
        .unwrap();

    core.shutdown().await;
}

/// A publish reissued on replay is held against the recorded event, not only
/// against its type. Core sends no commands while replaying, so this check is
/// the only place a publish to the wrong stream can be noticed.
#[tokio::test]
async fn a_publish_reissued_to_a_different_stream_fails_the_task() {
    let mut t = TestHistoryBuilder::default();
    t.add_by_type(EventType::WorkflowExecutionStarted);
    t.add_full_wf_task();
    t.add_stream_records_appended("s1", 0, 2);
    t.add_full_wf_task();

    let (core, failures) = worker_expecting_one_nondeterminism_failure(t, ResponseType::AllHistory);

    let task = core.poll_workflow_activation().await.unwrap();
    core.complete_workflow_activation(WorkflowActivationCompletion::from_cmds(
        task.run_id,
        vec![publish_two("s2").into()],
    ))
    .await
    .unwrap();
    core.handle_eviction().await;
    assert_eq!(failures.load(Ordering::Relaxed), 1);
    core.shutdown().await;
}

/// The batch size is part of the record too: the event names the offset range
/// the batch landed at, so a replay that publishes fewer has diverged from the
/// original run.
#[tokio::test]
async fn a_publish_reissued_with_a_different_batch_size_fails_the_task() {
    let mut t = TestHistoryBuilder::default();
    t.add_by_type(EventType::WorkflowExecutionStarted);
    t.add_full_wf_task();
    t.add_stream_records_appended("s1", 0, 2);
    t.add_full_wf_task();

    let (core, failures) = worker_expecting_one_nondeterminism_failure(t, ResponseType::AllHistory);

    let task = core.poll_workflow_activation().await.unwrap();
    core.complete_workflow_activation(WorkflowActivationCompletion::from_cmds(
        task.run_id,
        vec![
            AppendStreamRecords {
                stream_name: "s1".to_string(),
                records: vec![StreamRecord {
                    body: Some(b"one".to_vec().into()),
                    ..Default::default()
                }],
            }
            .into(),
        ],
    ))
    .await
    .unwrap();
    core.handle_eviction().await;
    assert_eq!(failures.load(Ordering::Relaxed), 1);
    core.shutdown().await;
}

/// A reissued append that names no stream is held to the name the run's earlier
/// unnamed appends resolved to. Without that, an empty id would match any
/// recorded stream and a workflow that moved its output would go unnoticed.
#[tokio::test]
async fn a_default_publish_reissued_against_another_stream_fails_the_task() {
    let mut t = TestHistoryBuilder::default();
    t.add_by_type(EventType::WorkflowExecutionStarted);
    t.add_full_wf_task();
    // The server resolved the run's unnamed appends to this name.
    t.add_stream_records_appended("output", 0, 2);
    t.add_stream_records_appended("elsewhere", 0, 2);
    t.add_full_wf_task();

    let (core, failures) = worker_expecting_one_nondeterminism_failure(t, ResponseType::AllHistory);

    let task = core.poll_workflow_activation().await.unwrap();
    core.complete_workflow_activation(WorkflowActivationCompletion::from_cmds(
        task.run_id,
        vec![publish_two("").into(), publish_two("").into()],
    ))
    .await
    .unwrap();
    core.handle_eviction().await;
    assert_eq!(failures.load(Ordering::Relaxed), 1);
    core.shutdown().await;
}

/// The stream a subscription names is part of what the recorded event holds it
/// to.
#[tokio::test]
async fn a_subscribe_reissued_to_a_different_stream_fails_the_task() {
    let mut t = TestHistoryBuilder::default();
    t.add_by_type(EventType::WorkflowExecutionStarted);
    t.add_full_wf_task();
    t.add_stream_subscribed("s1", 4);
    t.add_full_wf_task();

    let (core, failures) = worker_expecting_one_nondeterminism_failure(t, ResponseType::AllHistory);

    let task = core.poll_workflow_activation().await.unwrap();
    core.complete_workflow_activation(WorkflowActivationCompletion::from_cmds(
        task.run_id,
        vec![
            SubscribeStream {
                stream_name_or_id: "s2".to_string(),
                start_offset: -1,
            }
            .into(),
        ],
    ))
    .await
    .unwrap();
    core.handle_eviction().await;
    assert_eq!(failures.load(Ordering::Relaxed), 1);
    core.shutdown().await;
}

/// The start offset is deliberately not compared, even when the command names
/// one itself. The comparison would only be sound for the run's first subscribe
/// to a stream, and a subscription made through the stream service leaves no
/// event, so which one is first cannot be told from history. Failing a run that
/// did nothing wrong costs more than the drift the check would catch.
#[tokio::test]
async fn a_subscribe_reissued_with_a_different_offset_is_accepted() {
    let mut t = TestHistoryBuilder::default();
    t.add_by_type(EventType::WorkflowExecutionStarted);
    t.add_full_wf_task();
    t.add_stream_subscribed("s1", 100);
    t.add_full_wf_task();

    let core = worker_rejecting_any_failure(t);

    let task = core.poll_workflow_activation().await.unwrap();
    core.complete_workflow_activation(WorkflowActivationCompletion::from_cmds(
        task.run_id,
        vec![
            SubscribeStream {
                stream_name_or_id: "s1".to_string(),
                start_offset: 0,
            }
            .into(),
        ],
    ))
    .await
    .unwrap();
    core.shutdown().await;
}

/// A second subscribe to the same stream registers nothing on the server, which
/// records the event at wherever the cursor has already reached. The reissued
/// command still has to be accepted against it.
#[tokio::test]
async fn a_repeat_subscribe_is_accepted() {
    let mut t = TestHistoryBuilder::default();
    t.add_by_type(EventType::WorkflowExecutionStarted);
    t.add_full_wf_task();
    t.add_stream_subscribed("s1", 100);
    // The cursor had moved on by the time the second command was handled.
    t.add_stream_subscribed("s1", 140);
    t.add_full_wf_task();

    let core = worker_rejecting_any_failure(t);

    let subscribe = SubscribeStream {
        stream_name_or_id: "s1".to_string(),
        start_offset: 100,
    };
    let task = core.poll_workflow_activation().await.unwrap();
    core.complete_workflow_activation(WorkflowActivationCompletion::from_cmds(
        task.run_id,
        vec![subscribe.clone().into(), subscribe.into()],
    ))
    .await
    .unwrap();
    core.shutdown().await;
}

/// A task that consumes a range and issues no command still ran as its own
/// activation, so replay hands each such range over in its own activation
/// rather than collapsing the run of them into one, the way it does for
/// heartbeats that did nothing.
#[tokio::test]
async fn data_only_tasks_replay_one_range_per_activation() {
    let mut t = TestHistoryBuilder::default();
    t.add_by_type(EventType::WorkflowExecutionStarted);
    t.add_workflow_task_scheduled_and_started();
    let first = t.add_workflow_task_completed_with_consumed_stream_ranges(vec![cursor("s1", 0, 1)]);
    t.add_workflow_task_scheduled_and_started();
    let second =
        t.add_workflow_task_completed_with_consumed_stream_ranges(vec![cursor("s1", 1, 2)]);
    t.add_workflow_task_scheduled_and_started();
    let third = t.add_workflow_task_completed_with_consumed_stream_ranges(vec![cursor("s1", 2, 3)]);
    t.add_workflow_task_scheduled_and_started();

    let mut poll_resp = hist_to_poll_resp(&t, "wfid".to_owned(), ResponseType::AllHistory);
    poll_resp.add_stream_slice("s1", first, 0, &["a"]);
    poll_resp.add_stream_slice("s1", second, 1, &["b"]);
    poll_resp.add_stream_slice("s1", third, 2, &["c"]);
    poll_resp.add_stream_slice("s1", 0, 3, &["d"]);

    let mock = MockPollCfg::from_resp_batches(
        "wfid",
        t,
        [ResponseType::Raw(poll_resp.resp)],
        mock_worker_client(),
    );
    let core = mock_worker(build_mock_pollers(mock));

    let mut per_activation = vec![];
    for _ in 0..4 {
        let task = core.poll_workflow_activation().await.unwrap();
        per_activation.push(delivered_ranges(&task));
        core.complete_workflow_activation(WorkflowActivationCompletion::empty(task.run_id))
            .await
            .unwrap();
    }
    assert_eq!(
        per_activation,
        vec![
            vec![("s1".to_string(), 0, 1)],
            vec![("s1".to_string(), 1, 2)],
            vec![("s1".to_string(), 2, 3)],
            vec![("s1".to_string(), 3, 4)],
        ],
        "each consumed range replays in the activation of the task that consumed it"
    );
    core.shutdown().await;
}

/// Ranges for several streams on one task arrive ordered by stream, however
/// the server laid them out. A workflow that waits on two streams with a
/// first-completed pattern would otherwise take whichever branch the server's
/// iteration order happened to pick, live and again differently on replay.
#[tokio::test]
async fn ranges_for_several_streams_arrive_in_stream_order() {
    let mut t = TestHistoryBuilder::default();
    t.add_by_type(EventType::WorkflowExecutionStarted);
    t.add_workflow_task_scheduled_and_started();
    let completed = t.add_workflow_task_completed_with_consumed_stream_ranges(vec![
        cursor("s2", 0, 1),
        cursor("s1", 0, 1),
    ]);
    t.add_workflow_task_scheduled_and_started();

    let mut poll_resp = hist_to_poll_resp(&t, "wfid".to_owned(), ResponseType::AllHistory);
    poll_resp.add_stream_slice("s2", completed, 0, &["two"]);
    poll_resp.add_stream_slice("s1", completed, 0, &["one"]);
    poll_resp.add_stream_slice("s2", 0, 1, &["four"]);
    poll_resp.add_stream_slice("s1", 0, 1, &["three"]);

    let mock = MockPollCfg::from_resp_batches(
        "wfid",
        t,
        [ResponseType::Raw(poll_resp.resp)],
        mock_worker_client(),
    );
    let core = mock_worker(build_mock_pollers(mock));

    let task = core.poll_workflow_activation().await.unwrap();
    assert_eq!(
        delivered_ranges(&task),
        vec![("s1".to_string(), 0, 1), ("s2".to_string(), 0, 1)],
        "re-supplied ranges follow stream order"
    );
    core.complete_workflow_activation(WorkflowActivationCompletion::empty(task.run_id))
        .await
        .unwrap();

    let task = core.poll_workflow_activation().await.unwrap();
    assert_eq!(
        delivered_ranges(&task),
        vec![("s1".to_string(), 1, 2), ("s2".to_string(), 1, 2)],
        "live ranges follow stream order"
    );
    core.complete_workflow_activation(WorkflowActivationCompletion::empty(task.run_id))
        .await
        .unwrap();
    core.shutdown().await;
}

/// A recorded range that observed nothing is rebuilt from the cursor alone. The
/// event already says everything the workflow needs, so replay does not depend
/// on the server sending an empty slice back for it.
#[tokio::test]
async fn an_empty_recorded_range_replays_without_a_slice_from_the_server() {
    let mut t = TestHistoryBuilder::default();
    t.add_by_type(EventType::WorkflowExecutionStarted);
    t.add_workflow_task_scheduled_and_started();
    t.add_workflow_task_completed_with_consumed_stream_ranges(vec![cursor("s1", 4, 4)]);
    t.add_workflow_task_scheduled_and_started();

    let mut poll_resp = hist_to_poll_resp(&t, "wfid".to_owned(), ResponseType::AllHistory);
    poll_resp.add_stream_slice("s1", 0, 4, &["e"]);

    let mock = MockPollCfg::from_resp_batches(
        "wfid",
        t,
        [ResponseType::Raw(poll_resp.resp)],
        mock_worker_client(),
    );
    let core = mock_worker(build_mock_pollers(mock));

    let task = core.poll_workflow_activation().await.unwrap();
    assert_eq!(delivered_ranges(&task), vec![("s1".to_string(), 4, 4)]);
    core.complete_workflow_activation(WorkflowActivationCompletion::empty(task.run_id))
        .await
        .unwrap();

    let task = core.poll_workflow_activation().await.unwrap();
    assert_eq!(delivered_ranges(&task), vec![("s1".to_string(), 4, 5)]);
    core.complete_workflow_activation(WorkflowActivationCompletion::empty(task.run_id))
        .await
        .unwrap();
    core.shutdown().await;
}

/// A re-supplied slice is checked against the cursor it claims to satisfy. The
/// event is the record of what the task saw, so bytes covering other offsets
/// would replay the task on different input than it ran on. Both sides of that
/// comparison come from the server, so the task fails as the worker's failure.
#[tokio::test]
async fn a_resupplied_slice_that_disagrees_with_the_record_fails_the_task() {
    let mut t = TestHistoryBuilder::default();
    t.add_by_type(EventType::WorkflowExecutionStarted);
    t.add_workflow_task_scheduled_and_started();
    let completed =
        t.add_workflow_task_completed_with_consumed_stream_ranges(vec![cursor("s1", 0, 2)]);
    t.add_workflow_task_scheduled_and_started();

    let mut poll_resp = hist_to_poll_resp(&t, "wfid".to_owned(), ResponseType::AllHistory);
    // One record where the event says two.
    poll_resp.add_stream_slice("s1", completed, 0, &["a"]);

    let (core, failures) = worker_expecting_one_failure(
        t,
        ResponseType::Raw(poll_resp.resp),
        WorkflowTaskFailedCause::WorkflowWorkerUnhandledFailure,
    );

    // The mismatch is found while the poll response is applied, so the first
    // activation is already the eviction.
    core.handle_eviction().await;
    assert_eq!(failures.load(Ordering::Relaxed), 1);
    core.shutdown().await;
}

/// A run that stays cached was handed its range as the task ran. When the next
/// task arrives, the completion recording that range is in its history, and the
/// server may re-supply the range tagged with it, as it would for a worker that
/// lost the run. This worker did not, so it must not process the same records
/// twice.
#[tokio::test]
async fn a_cached_run_is_not_handed_its_own_range_again() {
    let mut t = TestHistoryBuilder::default();
    t.add_by_type(EventType::WorkflowExecutionStarted);
    t.add_workflow_task_scheduled_and_started();
    let completed =
        t.add_workflow_task_completed_with_consumed_stream_ranges(vec![cursor("s1", 0, 2)]);
    t.add_workflow_task_scheduled_and_started();

    let mut first_poll = hist_to_poll_resp(&t, "wfid".to_owned(), ResponseType::ToTaskNum(1));
    first_poll.add_stream_slice("s1", 0, 0, &["a", "b"]);
    let mut second_poll = hist_to_poll_resp(&t, "wfid".to_owned(), ResponseType::OneTask(2));
    second_poll.add_stream_slice("s1", completed, 0, &["a", "b"]);
    second_poll.add_stream_slice("s1", 0, 2, &["c"]);

    let mock = MockPollCfg::from_resp_batches(
        "wfid",
        t,
        [
            ResponseType::Raw(first_poll.resp),
            ResponseType::Raw(second_poll.resp),
        ],
        mock_worker_client(),
    );
    let mut mock = build_mock_pollers(mock);
    mock.worker_cfg(|wc| wc.max_cached_workflows = 1);
    let core = mock_worker(mock);

    let task = core.poll_workflow_activation().await.unwrap();
    assert_eq!(delivered_ranges(&task), vec![("s1".to_string(), 0, 2)]);
    core.complete_workflow_activation(WorkflowActivationCompletion::empty(task.run_id))
        .await
        .unwrap();

    let task = core.poll_workflow_activation().await.unwrap();
    assert!(!task.is_replaying);
    assert_eq!(
        delivered_ranges(&task),
        vec![("s1".to_string(), 2, 3)],
        "only the new range; the first one was delivered live to this worker"
    );
    core.complete_workflow_activation(WorkflowActivationCompletion::empty(task.run_id))
        .await
        .unwrap();
    core.shutdown().await;
}

/// History says a task consumed a range with content and the server sent no
/// bytes for it. The bytes only travel on the response that carried the task,
/// so the lookahead fails the task as soon as it sees the range, before the
/// workflow runs on less input than it ran on. A sticky task handed to a worker
/// that no longer holds the run is the case that reaches this, and the server's
/// retry on the normal queue carries the records. The task is failed as the
/// worker's failure, not the workflow's: nothing the workflow did was wrong.
#[tokio::test]
async fn a_missing_resupply_for_a_consumed_range_fails_the_task() {
    let mut t = TestHistoryBuilder::default();
    t.add_by_type(EventType::WorkflowExecutionStarted);
    t.add_workflow_task_scheduled_and_started();
    t.add_workflow_task_completed_with_consumed_stream_ranges(vec![cursor("s1", 0, 2)]);
    t.add_workflow_task_scheduled_and_started();

    let (core, failures) = worker_expecting_one_failure(
        t,
        ResponseType::AllHistory,
        WorkflowTaskFailedCause::WorkflowWorkerUnhandledFailure,
    );

    // Found while the poll response is applied, so the first activation is
    // already the eviction.
    core.handle_eviction().await;
    assert_eq!(failures.load(Ordering::Relaxed), 1);
    core.shutdown().await;
}

/// A task that read two streams and a response that re-supplies only one of
/// them. The recorded range is the whole of what the task ran on, so a partial
/// re-supply leaves the workflow as short of input as none at all, and it is
/// the same worker failure: the history and the response both come from the
/// server, and the retry on the normal task queue carries every stream.
#[tokio::test]
async fn a_partial_resupply_for_a_consumed_task_fails_the_task() {
    let mut t = TestHistoryBuilder::default();
    t.add_by_type(EventType::WorkflowExecutionStarted);
    t.add_workflow_task_scheduled_and_started();
    let completed = t.add_workflow_task_completed_with_consumed_stream_ranges(vec![
        cursor("s1", 0, 1),
        cursor("s2", 0, 1),
    ]);
    t.add_workflow_task_scheduled_and_started();

    let mut poll_resp = hist_to_poll_resp(&t, "wfid".to_owned(), ResponseType::AllHistory);
    // Only one of the two subscribed streams came back.
    poll_resp.add_stream_slice("s1", completed, 0, &["one"]);

    let (core, failures) = worker_expecting_one_failure(
        t,
        ResponseType::Raw(poll_resp.resp),
        WorkflowTaskFailedCause::WorkflowWorkerUnhandledFailure,
    );

    // Found while the poll response is applied, so the first activation is
    // already the eviction.
    // The reason travels in the message, since an eviction for a failure found
    // before any activation ran reports none of its own.
    let task = core.poll_workflow_activation().await.unwrap();
    let evict = eviction(&task);
    assert!(
        evict.message.contains(
            "stream s2 was consumed from offset 0 to 1, but the server sent no \
                       messages for it"
        ),
        "eviction did not name the stream that went missing: {evict:?}"
    );
    core.complete_workflow_activation(WorkflowActivationCompletion::empty(task.run_id))
        .await
        .unwrap();
    assert_eq!(failures.load(Ordering::Relaxed), 1);
    core.shutdown().await;
}

/// A legacy query for a run this worker no longer holds arrives on the sticky
/// queue with partial history and no records, and the history the worker
/// fetches itself carries none either. Answering it from a replay on less
/// input would be wrong, and failing it would end the query: the server
/// retries a query it hears nothing about on the normal queue, where the
/// records travel with it. So the query goes unanswered, no task is failed,
/// and the run is given up so the retry starts from history.
#[tokio::test]
async fn a_legacy_query_owed_records_it_was_not_sent_goes_unanswered() {
    let mut t = TestHistoryBuilder::default();
    t.add_by_type(EventType::WorkflowExecutionStarted);
    t.add_workflow_task_scheduled_and_started();
    // Task 1 subscribes and consumes nothing, so the missing range is only
    // reached once the workflow has run its first activation.
    t.add_workflow_task_completed();
    t.add_stream_subscribed("in", 0);
    t.add_workflow_task_scheduled_and_started();
    t.add_workflow_task_completed_with_consumed_stream_ranges(vec![cursor("in", 0, 2)]);
    t.add_stream_records_appended("out", 0, 2);

    let mut poll_resp = hist_to_poll_resp(&t, "wfid".to_owned(), ResponseType::AllHistory);
    poll_resp.resp.query = Some(WorkflowQuery {
        query_type: "trace".to_string(),
        query_args: None,
        header: None,
    });
    // No slices: the mock plays the sticky queue, and the defaults of zero
    // expected task failures and zero legacy query responses are the assertion.
    let mock = MockPollCfg::from_resp_batches(
        "wfid",
        t,
        [ResponseType::Raw(poll_resp.resp)],
        mock_worker_client(),
    );
    let mut mock = build_mock_pollers(mock);
    mock.worker_cfg(|wc| {
        wc.max_cached_workflows = 10;
        wc.ignore_evicts_on_shutdown = false;
    });
    let core = mock_worker(mock);

    let task = core.poll_workflow_activation().await.unwrap();
    assert_eq!(delivered_ranges(&task), vec![]);
    core.complete_workflow_activation(WorkflowActivationCompletion::from_cmds(
        task.run_id,
        vec![
            SubscribeStream {
                stream_name_or_id: "in".to_string(),
                start_offset: 0,
            }
            .into(),
        ],
    ))
    .await
    .unwrap();

    // The missing range is found while the next task is applied. The run is
    // evicted as a fetch failure would evict it, and nothing is reported.
    let task = core.poll_workflow_activation().await.unwrap();
    let evict = eviction(&task);
    assert_eq!(evict.reason(), EvictionReason::PaginationOrHistoryFetch);
    assert!(
        evict
            .message
            .contains("but the server sent no records for it"),
        "eviction did not name the missing range: {evict:?}"
    );
    core.complete_workflow_activation(WorkflowActivationCompletion::empty(task.run_id))
        .await
        .unwrap();
    core.shutdown().await;
}

/// A slice as the server re-supplies it: the records of one recorded range,
/// tagged with the completion that recorded it.
fn replay_slice(
    stream_id: &str,
    completed_event_id: i64,
    from: i64,
    bodies: &[&str],
) -> StreamSlice {
    StreamSlice {
        stream_id: stream_id.to_string(),
        from_offset: from,
        to_offset: from + bodies.len() as i64,
        records: bodies
            .iter()
            .map(|b| StreamRecord {
                body: Some(b.as_bytes().to_vec().into()),
                ..Default::default()
            })
            .collect(),
        workflow_task_completed_event_id: completed_event_id,
        ..Default::default()
    }
}

/// A publish of one record per body.
fn publish(stream_name: &str, bodies: &[&str]) -> AppendStreamRecords {
    AppendStreamRecords {
        stream_name: stream_name.to_string(),
        records: bodies
            .iter()
            .map(|b| StreamRecord {
                body: Some(b.as_bytes().to_vec().into()),
                ..Default::default()
            })
            .collect(),
    }
}

fn eviction(task: &WorkflowActivation) -> &RemoveFromCache {
    match task.jobs.as_slice() {
        [
            WorkflowActivationJob {
                variant: Some(workflow_activation_job::Variant::RemoveFromCache(evict)),
            },
        ] => evict,
        other => panic!("expected an eviction, got {other:?}"),
    }
}

/// A recorded read-then-publish workflow: each task consumes one record and
/// publishes because of it, the second one also completes the run.
fn read_then_publish_history() -> (TestHistoryBuilder, i64, i64) {
    let mut t = TestHistoryBuilder::default();
    t.add_by_type(EventType::WorkflowExecutionStarted);
    t.add_workflow_task_scheduled_and_started();
    let first = t.add_workflow_task_completed_with_consumed_stream_ranges(vec![cursor("in", 0, 1)]);
    t.add_stream_records_appended("out", 0, 1);
    t.add_workflow_task_scheduled_and_started();
    let second =
        t.add_workflow_task_completed_with_consumed_stream_ranges(vec![cursor("in", 1, 2)]);
    t.add_stream_records_appended("out", 1, 2);
    t.add_workflow_execution_completed();
    (t, first, second)
}

/// A replay worker fed one history. The feeder is handed back so the history
/// stream stays open until the test drops it, as a language replayer keeps it
/// open: once the stream ends the worker closes, and an eviction still owed
/// for the last history would be lost to the shutdown.
async fn replay_worker(history: HistoryForReplay) -> (Worker, HistoryFeeder) {
    let (feeder, stream) = HistoryFeeder::new(1);
    feeder.feed(history).await.unwrap();
    let core = init_replay_worker(ReplayWorkerInput::new(
        test_worker_cfg().build().unwrap(),
        stream,
    ))
    .unwrap();
    (core, feeder)
}

/// A history pushed for replay can carry the ranges its tasks consumed, in the
/// shape the server re-supplies them. The replay worker puts them on its
/// synthetic poll response, so the ordinary delivery path runs: each range
/// reaches the activation of the task that consumed it, and the publish that
/// task reissues is matched against its event.
#[tokio::test]
async fn a_pushed_history_replays_with_the_slices_it_carries() {
    let (t, first, second) = read_then_publish_history();
    let history = HistoryForReplay::new(t.get_full_history_info().unwrap(), "wfid")
        .with_stream_slices([
            replay_slice("in", first, 0, &["go"]),
            replay_slice("in", second, 1, &["stop"]),
        ]);
    let (core, feeder) = replay_worker(history).await;

    let task = core.poll_workflow_activation().await.unwrap();
    assert_eq!(delivered_ranges(&task), vec![("in".to_string(), 0, 1)]);
    core.complete_workflow_activation(WorkflowActivationCompletion::from_cmds(
        task.run_id,
        vec![publish("out", &["accept"]).into()],
    ))
    .await
    .unwrap();

    let task = core.poll_workflow_activation().await.unwrap();
    assert_eq!(delivered_ranges(&task), vec![("in".to_string(), 1, 2)]);
    core.complete_workflow_activation(WorkflowActivationCompletion::from_cmds(
        task.run_id,
        vec![
            publish("out", &["done"]).into(),
            CompleteWorkflowExecution { result: None }.into(),
        ],
    ))
    .await
    .unwrap();

    // Replay is over and the worker lets the run go; a nondeterminism eviction
    // would say the reissued publishes did not match their events.
    let task = core.poll_workflow_activation().await.unwrap();
    assert_eq!(eviction(&task).reason(), EvictionReason::LangRequested);
    core.complete_workflow_activation(WorkflowActivationCompletion::empty(task.run_id))
        .await
        .unwrap();
    drop(feeder);
    core.shutdown().await;
}

/// A slice attached to a pushed history is held to the same check as one the
/// server sends: offsets other than the recorded range fail the task rather
/// than replay it on different input.
#[tokio::test]
async fn a_pushed_history_with_a_wrong_slice_fails_as_a_worker_failure() {
    let (t, first, second) = read_then_publish_history();
    let history = HistoryForReplay::new(t.get_full_history_info().unwrap(), "wfid")
        .with_stream_slices([
            // Two records where the event says one.
            replay_slice("in", first, 0, &["go", "extra"]),
            replay_slice("in", second, 1, &["stop"]),
        ]);
    let (core, feeder) = replay_worker(history).await;

    // The mismatch is found while the poll response is applied, so the first
    // activation is already the eviction. The task is failed as the worker's
    // failure (the mock-client test above checks the cause); the eviction
    // carries the reason in its message, since an eviction for a failure found
    // before any activation ran reports no reason of its own.
    let task = core.poll_workflow_activation().await.unwrap();
    let evict = eviction(&task);
    assert!(
        evict.message.contains(
            "Event 4 records that stream in was consumed from offset 0 to 1, but the server \
             sent offsets 0 to 2 for it"
        ),
        "eviction did not name the mismatch: {evict:?}"
    );
    core.complete_workflow_activation(WorkflowActivationCompletion::empty(task.run_id))
        .await
        .unwrap();
    drop(feeder);
    core.shutdown().await;
}

/// A recorded range with content and no slice for it is the same failure. A
/// language replayer that has no store to fetch from cannot replay a consuming
/// workflow, and the task says so before the workflow runs on less input.
#[tokio::test]
async fn a_pushed_history_without_slices_for_a_consumed_range_fails_as_a_worker_failure() {
    let (t, _, _) = read_then_publish_history();
    let history = HistoryForReplay::new(t.get_full_history_info().unwrap(), "wfid");
    let (core, feeder) = replay_worker(history).await;

    // Found while the poll response is applied, so the first activation is
    // already the eviction, which carries the reason in its message.
    let task = core.poll_workflow_activation().await.unwrap();
    let evict = eviction(&task);
    assert!(
        evict.message.contains(
            "Event 4 records that stream in was consumed from offset 0 to 1, but the server \
             sent no records for it"
        ),
        "eviction did not name the missing range: {evict:?}"
    );
    core.complete_workflow_activation(WorkflowActivationCompletion::empty(task.run_id))
        .await
        .unwrap();
    drop(feeder);
    core.shutdown().await;
}
