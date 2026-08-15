//! External Workflow Stream routing and entry points (C3, C4).
//!
//! These drive the real serialized local-input lane through a mock worker, so what is under test
//! is the routing -- that an input reaches the right run, that an acknowledgement comes back, and
//! that the read-only probe is genuinely read-only. The wait set's own transition logic is
//! covered by unit tests next to it.

use crate::{
    ExternalStreamReadyResult, ExternalStreamRunStatus,
    replay::canned_histories,
    test_help::{
        MockPollCfg, WorkerExt, build_fake_worker, build_mock_pollers, mock_worker, start_timer_cmd,
    },
    worker::client::mocks::mock_worker_client,
};
use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::Duration,
};
use temporalio_common::{
    protos::coresdk::{
        workflow_commands::{ExternalStreamWait, WorkflowStreamQuiescent, workflow_command},
        workflow_completion::WorkflowActivationCompletion,
    },
    worker::WorkerTaskTypes,
};

/// A worker holding one run in its cache, with the first activation left outstanding.
///
/// Leaving it outstanding is what keeps the run cached for the whole test: completing it would
/// let the mock's poll budget run out and the run be evicted, which is a different state from the
/// one these tests are about.
async fn worker_with_a_cached_run() -> (crate::Worker, String) {
    let t = canned_histories::single_timer("1");
    let worker = build_fake_worker("fake_wf_id", t, [1]);
    let activation = worker.poll_workflow_activation().await.unwrap();
    let run_id = activation.run_id.clone();
    (worker, run_id)
}

/// Completes the outstanding activation and shuts the worker down cleanly.
async fn finish(worker: crate::Worker, run_id: &str) {
    worker
        .complete_workflow_activation(WorkflowActivationCompletion::from_cmds(
            run_id.to_string(),
            vec![start_timer_cmd(1, Duration::from_secs(10))],
        ))
        .await
        .unwrap();
    worker.drain_pollers_and_shutdown().await;
}

// --- readiness ---------------------------------------------------------------

#[tokio::test]
async fn readiness_for_an_unknown_run_reports_run_not_found() {
    let (worker, run_id) = worker_with_a_cached_run().await;

    let result = worker
        .notify_external_stream_ready("no-such-run", 1, 0)
        .await;

    assert_eq!(result, ExternalStreamReadyResult::RunNotFound);
    finish(worker, &run_id).await;
}

#[tokio::test]
async fn readiness_for_a_cached_run_with_no_open_task_is_not_run_not_found() {
    // The distinction this asserts is the whole reason the two results are separate: a run cached
    // between Workflow Tasks is healthy, and telling the watcher it was evicted would make it
    // tear itself down while it is still needed.
    let (worker, run_id) = worker_with_a_cached_run().await;
    worker
        .seed_external_stream_waits(&run_id, vec![1], None, false)
        .await;

    let result = worker.notify_external_stream_ready(&run_id, 1, 0).await;

    assert_eq!(result, ExternalStreamReadyResult::NoOpenWorkflowTask);
    finish(worker, &run_id).await;
}

#[tokio::test]
async fn readiness_for_a_confirmed_park_reports_parked() {
    let (worker, run_id) = worker_with_a_cached_run().await;
    worker
        .seed_external_stream_waits(&run_id, vec![1], Some(7), false)
        .await;

    let result = worker.notify_external_stream_ready(&run_id, 1, 0).await;

    assert_eq!(result, ExternalStreamReadyResult::Parked);
    finish(worker, &run_id).await;
}

#[tokio::test]
async fn readiness_at_the_current_generation_is_accepted() {
    let (worker, run_id) = worker_with_a_cached_run().await;
    worker
        .seed_external_stream_waits(&run_id, vec![1, 2], None, true)
        .await;

    let result = worker.notify_external_stream_ready(&run_id, 1, 0).await;

    assert_eq!(result, ExternalStreamReadyResult::Accepted);
    finish(worker, &run_id).await;
}

#[tokio::test]
async fn a_stale_wait_generation_is_reported_as_stale() {
    let (worker, run_id) = worker_with_a_cached_run().await;
    worker
        .seed_external_stream_waits(&run_id, vec![1], None, true)
        .await;

    let result = worker.notify_external_stream_ready(&run_id, 1, 99).await;

    // Not `NoOpenWorkflowTask`: the wait exists, so the watcher should re-probe rather than
    // send a Signal for a block that has already been resolved.
    assert_eq!(result, ExternalStreamReadyResult::Stale);
    finish(worker, &run_id).await;
}

#[tokio::test]
async fn concurrent_readiness_calls_are_all_acknowledged() {
    // The call must be safe from several watcher tasks at once -- one per subscription is the
    // normal case.
    let (worker, run_id) = worker_with_a_cached_run().await;
    worker
        .seed_external_stream_waits(&run_id, (1..=8).collect(), None, true)
        .await;

    let results = futures_util::future::join_all(
        (1..=8u32).map(|wait_id| worker.notify_external_stream_ready(&run_id, wait_id, 0)),
    )
    .await;

    assert!(
        results
            .iter()
            .all(|r| *r == ExternalStreamReadyResult::Accepted),
        "expected every concurrent notification to be accepted, got {results:?}"
    );
    finish(worker, &run_id).await;
}

// --- the read-only status probe ----------------------------------------------

#[tokio::test]
async fn status_for_an_unknown_run_reports_run_not_found() {
    let (worker, run_id) = worker_with_a_cached_run().await;

    assert_eq!(
        worker.external_stream_run_status("no-such-run").await,
        ExternalStreamRunStatus::RunNotFound
    );
    finish(worker, &run_id).await;
}

#[tokio::test]
async fn status_distinguishes_the_three_cached_states() {
    let (worker, run_id) = worker_with_a_cached_run().await;

    // Cached with no waits at all -- nothing for the sweep to do.
    assert_eq!(
        worker.external_stream_run_status(&run_id).await,
        ExternalStreamRunStatus::NoOpenWorkflowTask
    );

    worker
        .seed_external_stream_waits(&run_id, vec![1], None, true)
        .await;
    assert_eq!(
        worker.external_stream_run_status(&run_id).await,
        ExternalStreamRunStatus::WftOpen
    );

    worker
        .seed_external_stream_waits(&run_id, vec![1], Some(3), false)
        .await;
    assert_eq!(
        worker.external_stream_run_status(&run_id).await,
        ExternalStreamRunStatus::Parked
    );

    finish(worker, &run_id).await;
}

#[tokio::test]
async fn the_status_probe_leaves_the_run_untouched() {
    // Probing must not be usable as a readiness claim by accident: no activation may be
    // manufactured and no state may move, however many times it is asked.
    let (worker, run_id) = worker_with_a_cached_run().await;
    worker
        .seed_external_stream_waits(&run_id, vec![1], None, true)
        .await;

    for _ in 0..5 {
        assert_eq!(
            worker.external_stream_run_status(&run_id).await,
            ExternalStreamRunStatus::WftOpen
        );
    }

    // Readiness still behaves as though nothing had happened -- in particular the wait is still
    // at generation 0 and still blocked, not consumed by the probes.
    assert_eq!(
        worker.notify_external_stream_ready(&run_id, 1, 0).await,
        ExternalStreamReadyResult::Accepted
    );

    finish(worker, &run_id).await;
}

// --- idle timeout and park result routing (C3) -------------------------------

#[tokio::test]
async fn an_idle_timeout_for_the_current_generation_reaches_the_run() {
    let (worker, run_id) = worker_with_a_cached_run().await;
    worker
        .seed_external_stream_waits(&run_id, vec![1, 2], None, true)
        .await;

    worker.notify_external_stream_idle_timeout(&run_id, 1);

    // Every wait moved to `Parking`, so readiness is still accepted -- and accepting it is what
    // aborts the parking attempt.
    assert_eq!(
        worker.notify_external_stream_ready(&run_id, 1, 0).await,
        ExternalStreamReadyResult::Accepted
    );
    finish(worker, &run_id).await;
}

#[tokio::test]
async fn a_confirmed_park_result_parks_the_set() {
    let (worker, run_id) = worker_with_a_cached_run().await;
    worker
        .seed_external_stream_waits(&run_id, vec![1], None, true)
        .await;

    worker.notify_external_stream_idle_timeout(&run_id, 1);
    worker.notify_external_stream_park_result(&run_id, 1, true);

    assert_eq!(
        worker.external_stream_run_status(&run_id).await,
        ExternalStreamRunStatus::Parked
    );
    assert_eq!(
        worker.notify_external_stream_ready(&run_id, 1, 0).await,
        ExternalStreamReadyResult::Parked
    );
    finish(worker, &run_id).await;
}

#[tokio::test]
async fn an_aborted_park_result_returns_the_set_to_blocked() {
    let (worker, run_id) = worker_with_a_cached_run().await;
    worker
        .seed_external_stream_waits(&run_id, vec![1], None, true)
        .await;

    worker.notify_external_stream_idle_timeout(&run_id, 1);
    worker.notify_external_stream_park_result(&run_id, 1, false);

    assert_eq!(
        worker.external_stream_run_status(&run_id).await,
        ExternalStreamRunStatus::WftOpen
    );
    // The abort bumped the wait generation, which is what makes a readiness notification issued
    // for the abandoned attempt stale rather than resolving against the new block.
    assert_eq!(
        worker.notify_external_stream_ready(&run_id, 1, 0).await,
        ExternalStreamReadyResult::Stale
    );
    assert_eq!(
        worker.notify_external_stream_ready(&run_id, 1, 1).await,
        ExternalStreamReadyResult::Accepted
    );
    finish(worker, &run_id).await;
}

#[tokio::test]
async fn an_idle_timeout_for_an_unknown_run_is_harmless() {
    let (worker, run_id) = worker_with_a_cached_run().await;

    worker.notify_external_stream_idle_timeout("no-such-run", 1);

    finish(worker, &run_id).await;
}

// --- the run-level rollover timer (C13) --------------------------------------

/// The gap ADR-017 names, as a test.
///
/// A worker registering workflows and no activities has no local-activity request sink, and the
/// old `sink_heartbeat_timeout_start` silently returned a handle to a timer that was never
/// started. That is exactly the worker external streams must support, so the rollover deadline
/// has to come from the run rather than from the local-activity subsystem.
#[tokio::test]
async fn the_rollover_deadline_fires_on_a_workflow_only_worker() {
    let t = canned_histories::single_timer("1");
    let mut mock_cfg = MockPollCfg::from_resp_batches("fakeid", t, [1], mock_worker_client());
    let saw_force_new_wft = Arc::new(AtomicBool::new(false));
    let recorder = saw_force_new_wft.clone();
    mock_cfg.completion_asserts_from_expectations(|mut asserts| {
        asserts.then(move |wft| {
            recorder.store(wft.force_create_new_workflow_task, Ordering::Relaxed);
        });
    });

    let mut mock = build_mock_pollers(mock_cfg);
    mock.worker_cfg(|w| {
        // No activity task types at all, so `enable_local_activities` is false and there is no
        // request sink for a timer to be pushed into.
        w.task_types = WorkerTaskTypes::workflow_only();
        w.max_cached_workflows = 1;
    });
    let worker = mock_worker(mock);
    assert!(
        !worker.get_config().task_types.enable_local_activities,
        "this test is only meaningful without a local-activity sink"
    );

    let activation = worker.poll_workflow_activation().await.unwrap();
    let run_id = activation.run_id.clone();

    worker
        .start_wft_rollover_timer(&run_id, Duration::from_millis(50))
        .await;
    // Long enough for the deadline (80% of 50ms) to have passed.
    tokio::time::sleep(Duration::from_millis(200)).await;

    worker
        .complete_workflow_activation(WorkflowActivationCompletion::from_cmds(
            run_id,
            vec![start_timer_cmd(1, Duration::from_secs(10))],
        ))
        .await
        .unwrap();

    assert!(
        saw_force_new_wft.load(Ordering::Relaxed),
        "the rollover deadline expired but the completion did not request a replacement task"
    );
    worker.drain_pollers_and_shutdown().await;
}

#[tokio::test]
async fn a_cancelled_rollover_timer_does_not_force_a_new_task() {
    let t = canned_histories::single_timer("1");
    let mut mock_cfg = MockPollCfg::from_resp_batches("fakeid", t, [1], mock_worker_client());
    let saw_force_new_wft = Arc::new(AtomicBool::new(false));
    let recorder = saw_force_new_wft.clone();
    mock_cfg.completion_asserts_from_expectations(|mut asserts| {
        asserts.then(move |wft| {
            recorder.store(wft.force_create_new_workflow_task, Ordering::Relaxed);
        });
    });

    let mut mock = build_mock_pollers(mock_cfg);
    mock.worker_cfg(|w| {
        w.task_types = WorkerTaskTypes::workflow_only();
        w.max_cached_workflows = 1;
    });
    let worker = mock_worker(mock);

    let activation = worker.poll_workflow_activation().await.unwrap();
    let run_id = activation.run_id.clone();

    // A deadline far enough out that it cannot have fired by the time we complete.
    worker
        .start_wft_rollover_timer(&run_id, Duration::from_secs(300))
        .await;

    worker
        .complete_workflow_activation(WorkflowActivationCompletion::from_cmds(
            run_id,
            vec![start_timer_cmd(1, Duration::from_secs(10))],
        ))
        .await
        .unwrap();

    assert!(
        !saw_force_new_wft.load(Ordering::Relaxed),
        "a rollover deadline that has not expired must not force a replacement task"
    );
    worker.drain_pollers_and_shutdown().await;
}

// --- quiescence retains the Workflow Task (C6) -------------------------------

fn quiescent_command(
    quiescence_generation: u64,
    wait_ids: &[u32],
    idle_timeout: Duration,
) -> workflow_command::Variant {
    workflow_command::Variant::WorkflowStreamQuiescent(WorkflowStreamQuiescent {
        quiescence_generation,
        waits: wait_ids
            .iter()
            .map(|id| ExternalStreamWait {
                wait_id: *id,
                generation: 0,
                immediately_parkable: false,
            })
            .collect(),
        idle_timeout: Some(idle_timeout.try_into().unwrap()),
    })
}

/// A worker whose completions are all recorded, so a test can assert that a retained task
/// reported *nothing* to the server.
fn worker_recording_completions(completions: Arc<AtomicUsize>) -> crate::Worker {
    let t = canned_histories::single_timer("1");
    let mut mock_cfg = MockPollCfg::from_resp_batches("fakeid", t, [1], mock_worker_client());
    mock_cfg.completion_asserts_from_expectations(|mut asserts| {
        for _ in 0..4 {
            let counter = completions.clone();
            asserts.then(move |_| {
                counter.fetch_add(1, Ordering::Relaxed);
            });
        }
    });
    let mut mock = build_mock_pollers(mock_cfg);
    mock.worker_cfg(|w| {
        w.task_types = WorkerTaskTypes::workflow_only();
        w.max_cached_workflows = 1;
    });
    mock_worker(mock)
}

#[tokio::test]
async fn quiescence_holds_the_workflow_task_open() {
    let completions = Arc::new(AtomicUsize::new(0));
    let worker = worker_recording_completions(completions.clone());

    let activation = worker.poll_workflow_activation().await.unwrap();
    let run_id = activation.run_id.clone();

    worker
        .complete_workflow_activation(WorkflowActivationCompletion::from_cmd(
            run_id.clone(),
            quiescent_command(1, &[1, 2], Duration::from_secs(30)),
        ))
        .await
        .unwrap();

    assert_eq!(
        completions.load(Ordering::Relaxed),
        0,
        "a retained Workflow Task must report nothing to the server"
    );
    assert_eq!(
        worker.external_stream_run_status(&run_id).await,
        ExternalStreamRunStatus::WftOpen
    );
    // And the wait set really is the one lang described, not an empty stand-in.
    assert_eq!(
        worker.notify_external_stream_ready(&run_id, 2, 0).await,
        ExternalStreamReadyResult::Accepted
    );

    worker.drain_pollers_and_shutdown().await;
}

#[tokio::test]
async fn the_idle_timer_fires_and_moves_the_set_to_parking() {
    let completions = Arc::new(AtomicUsize::new(0));
    let worker = worker_recording_completions(completions.clone());

    let activation = worker.poll_workflow_activation().await.unwrap();
    let run_id = activation.run_id.clone();

    worker
        .complete_workflow_activation(WorkflowActivationCompletion::from_cmd(
            run_id.clone(),
            quiescent_command(1, &[1], Duration::from_millis(30)),
        ))
        .await
        .unwrap();

    tokio::time::sleep(Duration::from_millis(300)).await;

    // The timer fired and the set entered `Parking`. Readiness is still accepted there, and
    // accepting it is what aborts the parking attempt -- the handshake that would confirm it is
    // C8's.
    assert_eq!(
        worker.notify_external_stream_ready(&run_id, 1, 0).await,
        ExternalStreamReadyResult::Accepted
    );

    worker.drain_pollers_and_shutdown().await;
}

#[tokio::test]
async fn readiness_does_not_cancel_a_timer_for_a_superseded_snapshot() {
    // A timer that fires for a generation the Workflow has already run past must be discarded
    // rather than parking the set it finds.
    let completions = Arc::new(AtomicUsize::new(0));
    let worker = worker_recording_completions(completions.clone());

    let activation = worker.poll_workflow_activation().await.unwrap();
    let run_id = activation.run_id.clone();

    worker
        .complete_workflow_activation(WorkflowActivationCompletion::from_cmd(
            run_id.clone(),
            quiescent_command(1, &[1], Duration::from_secs(30)),
        ))
        .await
        .unwrap();

    // A timer for a stale generation arrives late.
    worker.notify_external_stream_idle_timeout(&run_id, 99);

    assert_eq!(
        worker.external_stream_run_status(&run_id).await,
        ExternalStreamRunStatus::WftOpen,
        "a stale idle timeout must leave the wait set alone"
    );

    worker.drain_pollers_and_shutdown().await;
}

#[tokio::test]
async fn a_quiescent_command_with_a_non_positive_idle_timeout_is_malformed() {
    // Rejected rather than coerced: zero would park instantly and absent would hold the task
    // until it timed out, and neither is something a caller can have meant. The mock's
    // `num_expected_fails` is what asserts the failure actually reached the server.
    let t = canned_histories::single_timer("1");
    let mut mock_cfg = MockPollCfg::from_resp_batches("fakeid", t, [1], mock_worker_client());
    mock_cfg.num_expected_fails = 1_usize.into();
    let mut mock = build_mock_pollers(mock_cfg);
    mock.worker_cfg(|w| {
        w.task_types = WorkerTaskTypes::workflow_only();
        w.max_cached_workflows = 1;
    });
    let worker = mock_worker(mock);

    let activation = worker.poll_workflow_activation().await.unwrap();
    let run_id = activation.run_id.clone();

    worker
        .complete_workflow_activation(WorkflowActivationCompletion::from_cmd(
            run_id.clone(),
            workflow_command::Variant::WorkflowStreamQuiescent(WorkflowStreamQuiescent {
                quiescence_generation: 1,
                waits: vec![ExternalStreamWait {
                    wait_id: 1,
                    generation: 0,
                    immediately_parkable: false,
                }],
                idle_timeout: None,
            }),
        ))
        .await
        .unwrap();

    // Nothing was retained: no wait set was recorded for the malformed snapshot.
    assert_ne!(
        worker.external_stream_run_status(&run_id).await,
        ExternalStreamRunStatus::WftOpen,
        "a malformed quiescence command must not retain the Workflow Task"
    );

    worker.shutdown().await;
    worker.finalize_shutdown().await;
}

#[tokio::test]
async fn a_completion_with_server_bound_commands_does_not_retain() {
    // Subscriptions stay registered, but the task must be reported so the server can act on the
    // timer -- the wake Signal is what covers the window that leaves.
    let completions = Arc::new(AtomicUsize::new(0));
    let worker = worker_recording_completions(completions.clone());

    let activation = worker.poll_workflow_activation().await.unwrap();
    let run_id = activation.run_id.clone();

    worker
        .complete_workflow_activation(WorkflowActivationCompletion::from_cmds(
            run_id.clone(),
            vec![
                quiescent_command(1, &[1], Duration::from_secs(30)).into(),
                start_timer_cmd(1, Duration::from_secs(10)),
            ],
        ))
        .await
        .unwrap();

    assert_eq!(
        completions.load(Ordering::Relaxed),
        1,
        "a completion carrying a server-bound command must be reported, not retained"
    );

    worker.drain_pollers_and_shutdown().await;
}
