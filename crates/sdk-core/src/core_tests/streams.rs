//! Delivery of server-side stream ranges to a workflow.
//!
//! History records the offsets a task consumed and never the payloads, so the
//! server sends the bytes on the poll response: untagged for the task about to
//! run, and tagged with a WorkflowTaskCompleted event id when it is
//! re-supplying what an earlier task consumed. These tests pin that both
//! arrive, in the right order, and that an empty range is still delivered.

use crate::{
    replay::TestHistoryBuilder,
    test_help::{
        MockPollCfg, PollWFTRespExt, ResponseType, build_mock_pollers, hist_to_poll_resp,
        mock_worker,
    },
    worker::client::mocks::mock_worker_client,
};
use temporalio_common::protos::{
    coresdk::{
        workflow_activation::{WorkflowActivationJob, workflow_activation_job},
        workflow_completion::WorkflowActivationCompletion,
    },
    temporal::api::{enums::v1::EventType, stream::v1::StreamCursor},
};

fn cursor(stream_id: &str, from: i64, to: i64) -> StreamCursor {
    StreamCursor {
        stream_id: stream_id.to_string(),
        from_offset: from,
        to_offset: to,
    }
}

fn delivered(job: &WorkflowActivationJob) -> (&str, i64, i64, Vec<&[u8]>) {
    match job.variant.as_ref().unwrap() {
        workflow_activation_job::Variant::DeliverStreamMessages(d) => (
            d.stream_id.as_str(),
            d.from_offset,
            d.to_offset,
            d.messages
                .iter()
                .map(|m| m.body.as_ref().unwrap().data.as_slice())
                .collect(),
        ),
        other => panic!("expected a stream delivery, got {other:?}"),
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
                Some(workflow_activation_job::Variant::DeliverStreamMessages(_))
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
                Some(workflow_activation_job::Variant::DeliverStreamMessages(_))
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
        t.add_workflow_task_completed_with_stream_cursors(vec![cursor("s1", 0, 2)]);
    t.add_workflow_task_scheduled_and_started();
    let second_completed =
        t.add_workflow_task_completed_with_stream_cursors(vec![cursor("s1", 2, 3)]);
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

    let mut seen = vec![];
    loop {
        let task = core.poll_workflow_activation().await.unwrap();
        for job in &task.jobs {
            if matches!(
                job.variant,
                Some(workflow_activation_job::Variant::DeliverStreamMessages(_))
            ) {
                let (_, from, to, bodies) = delivered(job);
                seen.push((
                    from,
                    to,
                    bodies.iter().map(|b| b.to_vec()).collect::<Vec<_>>(),
                ));
            }
        }
        let done = seen.len() >= 3;
        core.complete_workflow_activation(WorkflowActivationCompletion::empty(task.run_id))
            .await
            .unwrap();
        if done {
            break;
        }
    }

    assert_eq!(
        seen,
        vec![
            (0, 2, vec![b"alpha".to_vec(), b"beta".to_vec()]),
            (2, 3, vec![b"gamma".to_vec()]),
            (3, 4, vec![b"delta".to_vec()]),
        ],
        "recorded ranges come back in event order, then the live one"
    );
}
