//! External Workflow Stream routing and entry points (C3, C4).
//!
//! These drive the real serialized local-input lane through a mock worker, so what is under test
//! is the routing -- that an input reaches the right run, that an acknowledgement comes back, and
//! that the read-only probe is genuinely read-only. The wait set's own transition logic is
//! covered by unit tests next to it.

use crate::{
    ExternalStreamReadyResult, ExternalStreamRunStatus,
    replay::{TestHistoryBuilder, canned_histories},
    test_help::{
        MockPollCfg, WorkerExt, build_fake_worker, build_mock_pollers, mock_worker, start_timer_cmd,
    },
    worker::client::{WorkflowTaskCompletion, mocks::mock_worker_client},
};
use parking_lot::Mutex;
use prost::Message as _;
use std::{
    collections::HashMap,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::Duration,
};
use temporalio_common::{
    protos::coresdk::{
        external_data::{ParkReason, extract_external_stream_marker_data},
        external_stream::{self, WakeSignal},
        workflow_activation::workflow_activation_job,
        workflow_commands::{
            CompleteWorkflowExecution, ExternalStreamWait, WorkflowStreamProgress,
            WorkflowStreamQuiescent, workflow_command,
        },
        workflow_completion::WorkflowActivationCompletion,
    },
    protos::{
        constants::EXTERNAL_STREAM_MARKER_NAME,
        temporal::api::{
            command::v1::command,
            common::v1::Payload,
            enums::v1::{CommandType, EventType},
        },
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

    consume_resolve_activation(&worker, &run_id).await;
    worker.drain_pollers_and_shutdown().await;
}

/// Polls the resolve activation readiness produced and completes it, leaving no task open.
async fn consume_resolve_activation(worker: &crate::Worker, run_id: &str) -> Vec<u32> {
    let activation = worker.poll_workflow_activation().await.unwrap();
    let hints = resolve_hints(&activation);
    worker
        .complete_workflow_activation(WorkflowActivationCompletion::from_cmds(
            run_id.to_string(),
            vec![start_timer_cmd(1, Duration::from_secs(10))],
        ))
        .await
        .unwrap();
    hints
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

    consume_resolve_activation(&worker, &run_id).await;
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

    // Release the retained task so shutdown can finish.
    worker.notify_external_stream_ready(&run_id, 1, 0).await;
    consume_resolve_activation(&worker, &run_id).await;
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

// --- readiness resolves the wait set (C7) ------------------------------------

#[tokio::test]
async fn simultaneous_readiness_ships_as_one_coalesced_activation() {
    // One activation, not one per wait: there is never more than one outstanding activation per
    // Run, and lang probes every active wait on receipt anyway -- the hints are hints.
    let completions = Arc::new(AtomicUsize::new(0));
    let worker = worker_recording_completions(completions.clone());

    let activation = worker.poll_workflow_activation().await.unwrap();
    let run_id = activation.run_id.clone();
    worker
        .complete_workflow_activation(WorkflowActivationCompletion::from_cmd(
            run_id.clone(),
            quiescent_command(1, &[1, 2, 3], Duration::from_secs(30)),
        ))
        .await
        .unwrap();

    // The first notification has nothing to coalesce with and ships on its own.
    assert_eq!(
        worker.notify_external_stream_ready(&run_id, 3, 0).await,
        ExternalStreamReadyResult::Accepted
    );
    let first = worker.poll_workflow_activation().await.unwrap();
    assert_eq!(resolve_hints(&first), vec![3]);

    // Everything arriving while that activation is outstanding accumulates -- including a repeat
    // for a wait already known ready, which must not ship twice.
    for wait_id in [1, 2, 1] {
        assert_eq!(
            worker
                .notify_external_stream_ready(&run_id, wait_id, 0)
                .await,
            ExternalStreamReadyResult::Accepted
        );
    }
    assert_eq!(
        completions.load(Ordering::Relaxed),
        0,
        "readiness must not complete the retained Workflow Task"
    );

    // Completing that activation ships everything accumulated as *one* more activation.
    worker
        .complete_workflow_activation(WorkflowActivationCompletion::from_cmd(
            run_id.clone(),
            quiescent_command(2, &[1, 2, 3], Duration::from_secs(30)),
        ))
        .await
        .unwrap();

    let hints = consume_resolve_activation(&worker, &run_id).await;
    assert_eq!(
        hints,
        vec![1, 2],
        "three notifications across two waits must coalesce into one activation"
    );

    worker.drain_pollers_and_shutdown().await;
}

/// The wait ids named by any resolve job in an activation.
fn resolve_hints(
    activation: &temporalio_common::protos::coresdk::workflow_activation::WorkflowActivation,
) -> Vec<u32> {
    let mut ids: Vec<u32> = activation
        .jobs
        .iter()
        .filter_map(|j| match &j.variant {
            Some(workflow_activation_job::Variant::ResolveExternalStreamWaits(r)) => {
                Some(r.ready_hints.iter().map(|w| w.wait_id).collect::<Vec<_>>())
            }
            _ => None,
        })
        .flatten()
        .collect();
    ids.sort_unstable();
    ids
}

#[tokio::test]
async fn readiness_arriving_during_an_outstanding_activation_is_accumulated() {
    // Readiness that lands while lang is still working cannot ship immediately, and dropping it
    // would leave a buffered record with nothing to deliver it.
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

    // First readiness produces an activation.
    worker.notify_external_stream_ready(&run_id, 1, 0).await;
    let outstanding = worker.poll_workflow_activation().await.unwrap();
    assert!(
        outstanding.jobs.iter().any(|j| matches!(
            j.variant,
            Some(workflow_activation_job::Variant::ResolveExternalStreamWaits(_))
        )),
        "expected a resolve activation, got {:?}",
        outstanding.jobs
    );

    // Second readiness arrives while that one is still outstanding.
    assert_eq!(
        worker.notify_external_stream_ready(&run_id, 2, 0).await,
        ExternalStreamReadyResult::Accepted
    );

    // Completing the outstanding activation with a fresh quiescent snapshot must surface the
    // accumulated readiness rather than losing it.
    worker
        .complete_workflow_activation(WorkflowActivationCompletion::from_cmd(
            run_id.clone(),
            quiescent_command(2, &[1, 2], Duration::from_secs(30)),
        ))
        .await
        .unwrap();
    worker.notify_external_stream_ready(&run_id, 2, 0).await;

    let hints = consume_resolve_activation(&worker, &run_id).await;
    assert_eq!(hints, vec![2]);

    worker.drain_pollers_and_shutdown().await;
}

#[tokio::test]
async fn readiness_before_the_idle_timer_expires_cancels_it() {
    // If the timer survived readiness it would fire against the *next* quiescent snapshot and
    // park a set that had just been told a record was waiting.
    let completions = Arc::new(AtomicUsize::new(0));
    let worker = worker_recording_completions(completions.clone());

    let activation = worker.poll_workflow_activation().await.unwrap();
    let run_id = activation.run_id.clone();
    worker
        .complete_workflow_activation(WorkflowActivationCompletion::from_cmd(
            run_id.clone(),
            quiescent_command(1, &[1], Duration::from_millis(60)),
        ))
        .await
        .unwrap();

    assert_eq!(
        worker.notify_external_stream_ready(&run_id, 1, 0).await,
        ExternalStreamReadyResult::Accepted
    );
    consume_resolve_activation(&worker, &run_id).await;

    // Well past when the cancelled timer would have fired.
    tokio::time::sleep(Duration::from_millis(300)).await;

    // Nothing parked: readiness ended that snapshot, and the timer measuring it went with it.
    assert_ne!(
        worker.external_stream_run_status(&run_id).await,
        ExternalStreamRunStatus::Parked,
        "a cancelled idle timer must not park a set readiness already resolved"
    );

    worker.drain_pollers_and_shutdown().await;
}

#[tokio::test]
async fn a_stale_generation_produces_no_activation() {
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

    assert_eq!(
        worker.notify_external_stream_ready(&run_id, 1, 42).await,
        ExternalStreamReadyResult::Stale
    );

    // Still retained, with no activation to poll -- the notification was for a block that had
    // already been resolved, so manufacturing an activation for it would run user code for
    // nothing.
    assert_eq!(
        worker.external_stream_run_status(&run_id).await,
        ExternalStreamRunStatus::WftOpen
    );
    assert_eq!(completions.load(Ordering::Relaxed), 0);

    worker.notify_external_stream_ready(&run_id, 1, 0).await;
    consume_resolve_activation(&worker, &run_id).await;
    worker.drain_pollers_and_shutdown().await;
}

// --- observation deltas accumulate (C14a) ------------------------------------

fn progress_command(delta: &[u8], request_rollover: bool) -> workflow_command::Variant {
    workflow_command::Variant::WorkflowStreamProgress(WorkflowStreamProgress {
        observation_delta: delta.to_vec(),
        request_rollover,
    })
}

#[tokio::test]
async fn deltas_accumulate_across_a_retained_task() {
    // Core is annotation-blind: it appends bytes and hands them back. What this asserts is that
    // the concatenation is exactly what lang emitted, in order -- which is the whole reason lang
    // can build the marker's annotation by concatenating its own deltas.
    let completions = Arc::new(AtomicUsize::new(0));
    let worker = worker_recording_completions(completions.clone());

    let activation = worker.poll_workflow_activation().await.unwrap();
    let run_id = activation.run_id.clone();

    worker
        .complete_workflow_activation(WorkflowActivationCompletion::from_cmds(
            run_id.clone(),
            vec![
                progress_command(b"first", false).into(),
                quiescent_command(1, &[1], Duration::from_secs(30)).into(),
            ],
        ))
        .await
        .unwrap();
    assert_eq!(worker.external_stream_annotation(&run_id).await, b"first");

    worker.notify_external_stream_ready(&run_id, 1, 0).await;
    let _ = worker.poll_workflow_activation().await.unwrap();
    worker
        .complete_workflow_activation(WorkflowActivationCompletion::from_cmds(
            run_id.clone(),
            vec![
                progress_command(b"second", false).into(),
                quiescent_command(2, &[1], Duration::from_secs(30)).into(),
            ],
        ))
        .await
        .unwrap();

    assert_eq!(
        worker.external_stream_annotation(&run_id).await,
        b"firstsecond",
        "successive deltas for one Workflow Task must concatenate in order"
    );
    assert_eq!(
        completions.load(Ordering::Relaxed),
        0,
        "accumulating a delta must not complete the retained task"
    );

    worker.notify_external_stream_ready(&run_id, 1, 0).await;
    consume_resolve_activation(&worker, &run_id).await;
    worker.drain_pollers_and_shutdown().await;
}

#[tokio::test]
async fn an_empty_delta_accumulates_like_any_other() {
    // An activation that observed nothing still observed: it recorded a drain that replay must
    // reproduce, and on a subscription's first observation it carries the header without which
    // replay has no starting point at all.
    let completions = Arc::new(AtomicUsize::new(0));
    let worker = worker_recording_completions(completions.clone());

    let activation = worker.poll_workflow_activation().await.unwrap();
    let run_id = activation.run_id.clone();

    worker
        .complete_workflow_activation(WorkflowActivationCompletion::from_cmds(
            run_id.clone(),
            vec![
                progress_command(b"", false).into(),
                quiescent_command(1, &[1], Duration::from_secs(30)).into(),
            ],
        ))
        .await
        .unwrap();

    // Accepted without complaint, and it did not disturb what was already there.
    assert_eq!(worker.external_stream_annotation(&run_id).await, b"");
    assert_eq!(
        worker.external_stream_run_status(&run_id).await,
        ExternalStreamRunStatus::WftOpen
    );

    worker.notify_external_stream_ready(&run_id, 1, 0).await;
    consume_resolve_activation(&worker, &run_id).await;
    worker.drain_pollers_and_shutdown().await;
}

#[tokio::test]
async fn a_delta_without_retention_still_accumulates() {
    // Progress never implies retention. A completion that consumed records and then produced a
    // server-bound command must still commit that consumption, or replay re-delivers the records
    // while the command they produced is already durable.
    //
    // Two task batches, so the run is still cached when the annotation is read -- with one, the
    // worker runs out of work and evicts before the assertion.
    let completions = Arc::new(AtomicUsize::new(0));
    let counter = completions.clone();
    let t = marker_then_timer_history(0, ParkReason::CommandsProduced, b"consumed");
    let markers: Arc<Mutex<Vec<(u64, ParkReason, Vec<u8>)>>> = Default::default();
    let mut mock_cfg = MockPollCfg::from_resp_batches("fakeid", t, [1, 2], mock_worker_client());
    mock_cfg.completion_asserts_from_expectations(|mut asserts| {
        for _ in 0..2 {
            let counter = counter.clone();
            let collected = markers.clone();
            asserts.then(move |wft| {
                counter.fetch_add(1, Ordering::Relaxed);
                collected.lock().extend(stream_markers(wft));
            });
        }
    });
    let mut mock = build_mock_pollers(mock_cfg);
    mock.worker_cfg(|w| {
        w.task_types = WorkerTaskTypes::workflow_only();
        w.max_cached_workflows = 1;
    });
    let worker = mock_worker(mock);

    let activation = worker.poll_workflow_activation().await.unwrap();
    let run_id = activation.run_id.clone();

    worker
        .complete_workflow_activation(WorkflowActivationCompletion::from_cmds(
            run_id.clone(),
            vec![
                progress_command(b"consumed", false).into(),
                start_timer_cmd(1, Duration::from_secs(10)),
            ],
        ))
        .await
        .unwrap();

    // The next task proves the run is still cached rather than evicted out from under us.
    let next = worker.poll_workflow_activation().await.unwrap();
    assert_eq!(next.run_id, run_id);

    assert_eq!(
        completions.load(Ordering::Relaxed),
        1,
        "a completion with a server-bound command is reported, not retained"
    );
    assert_eq!(
        *markers.lock(),
        vec![(0, ParkReason::CommandsProduced, b"consumed".to_vec())],
        "the delta must be committed even though nothing was retained"
    );
    assert!(
        worker.external_stream_annotation(&run_id).await.is_empty(),
        "the annotation is cleared once its marker is written"
    );

    worker
        .complete_workflow_activation(WorkflowActivationCompletion::from_cmds(
            run_id,
            vec![CompleteWorkflowExecution::default().into()],
        ))
        .await
        .unwrap();
    worker.drain_pollers_and_shutdown().await;
}

#[tokio::test]
async fn a_progress_command_after_another_command_is_malformed() {
    // Ordering is normative. On replay, a record's integrity must be validated before the command
    // derived from it is matched -- the other way round, a damaged stream is discovered only after
    // its consequences have been accepted as durable.
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
        .complete_workflow_activation(WorkflowActivationCompletion::from_cmds(
            run_id.clone(),
            vec![
                start_timer_cmd(1, Duration::from_secs(10)),
                progress_command(b"too late", false).into(),
            ],
        ))
        .await
        .unwrap();

    assert!(
        worker.external_stream_annotation(&run_id).await.is_empty(),
        "a misordered delta must be rejected, not accumulated"
    );

    worker.shutdown().await;
    worker.finalize_shutdown().await;
}

// --- rollover transport (C12a) -----------------------------------------------

#[tokio::test]
async fn a_retained_task_rolls_over_with_its_wait_set_intact() {
    // The half that can exist without markers: the task is replaced, and every subscription,
    // cursor, and readiness generation survives onto the replacement. No annotation is written,
    // because in this configuration there is none to write.
    let saw_force_new_wft = Arc::new(AtomicBool::new(false));
    let recorder = saw_force_new_wft.clone();
    let t = canned_histories::single_timer("1");
    let markers: Arc<Mutex<Vec<(u64, ParkReason, Vec<u8>)>>> = Default::default();
    let mut mock_cfg = MockPollCfg::from_resp_batches("fakeid", t, [1, 1], mock_worker_client());
    let collected = markers.clone();
    mock_cfg.completion_asserts_from_expectations(|mut asserts| {
        asserts.then(move |wft| {
            recorder.store(wft.force_create_new_workflow_task, Ordering::Relaxed);
            collected.lock().extend(stream_markers(wft));
        });
    });
    let mut mock = build_mock_pollers(mock_cfg);
    mock.worker_cfg(|w| {
        // Exactly the worker ADR-017 is about: no local activities, so no request sink.
        w.task_types = WorkerTaskTypes::workflow_only();
        w.max_cached_workflows = 1;
    });
    let worker = mock_worker(mock);

    let activation = worker.poll_workflow_activation().await.unwrap();
    let run_id = activation.run_id.clone();

    worker
        .complete_workflow_activation(WorkflowActivationCompletion::from_cmds(
            run_id.clone(),
            vec![
                progress_command(b"before-rollover", false).into(),
                quiescent_command(1, &[1, 2], Duration::from_secs(30)).into(),
            ],
        ))
        .await
        .unwrap();
    assert_eq!(
        worker.external_stream_run_status(&run_id).await,
        ExternalStreamRunStatus::WftOpen
    );

    worker
        .start_wft_rollover_timer(&run_id, Duration::from_millis(50))
        .await;

    // The rollover's autocompletion is produced inside the poll loop, so a poll must be in
    // flight for it to happen at all -- as is always the case on a running worker. This one
    // times out on purpose: the replacement task is retained too, so there is nothing to hand
    // back until readiness resolves it.
    assert!(
        tokio::time::timeout(
            Duration::from_millis(600),
            worker.poll_workflow_activation()
        )
        .await
        .is_err(),
        "the replacement task must be retained too, or the rollover undoes itself"
    );

    assert!(
        saw_force_new_wft.load(Ordering::Relaxed),
        "a rollover deadline that expires on a retained task must request a replacement"
    );
    assert_eq!(
        worker.external_stream_run_status(&run_id).await,
        ExternalStreamRunStatus::WftOpen,
        "the wait set retains the replacement task exactly as it retained its predecessor"
    );
    assert_eq!(
        worker.external_stream_annotation(&run_id).await,
        b"before-rollover",
        "rollover transport writes no marker, so the accumulated annotation is untouched"
    );
    assert_eq!(
        *markers.lock(),
        Vec::new(),
        "the marker half of rollover is C12b's; transport alone writes nothing"
    );

    // The readiness generation still matches: a notification issued before the rollover is not
    // turned stale by it.
    assert_eq!(
        worker.notify_external_stream_ready(&run_id, 2, 0).await,
        ExternalStreamReadyResult::Accepted,
        "wait 2 must still be registered at generation 0 across the rollover"
    );

    // That readiness releases the retained replacement, so shutdown can finish.
    let resolved = worker.poll_workflow_activation().await.unwrap();
    assert_eq!(resolve_hints(&resolved), vec![2]);
    worker
        .complete_workflow_activation(WorkflowActivationCompletion::from_cmds(
            run_id,
            vec![CompleteWorkflowExecution::default().into()],
        ))
        .await
        .unwrap();

    worker.drain_pollers_and_shutdown().await;
}

#[tokio::test]
async fn a_budget_rollover_forces_a_replacement_without_a_deadline() {
    // Lang decided this boundary, so it needs no finalization round trip -- the very command
    // carrying the request already carried the terminal.
    let saw_force_new_wft = Arc::new(AtomicBool::new(false));
    let recorder = saw_force_new_wft.clone();
    let t = marker_then_timer_history(0, ParkReason::BudgetRollover, b"at-the-budget");
    let markers: Arc<Mutex<Vec<(u64, ParkReason, Vec<u8>)>>> = Default::default();
    let mut mock_cfg = MockPollCfg::from_resp_batches("fakeid", t, [1, 2], mock_worker_client());
    let collected = markers.clone();
    mock_cfg.completion_asserts_from_expectations(|mut asserts| {
        asserts.then(move |wft| {
            recorder.store(wft.force_create_new_workflow_task, Ordering::Relaxed);
            collected.lock().extend(stream_markers(wft));
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

    worker
        .complete_workflow_activation(WorkflowActivationCompletion::from_cmds(
            run_id.clone(),
            vec![
                progress_command(b"at-the-budget", true).into(),
                start_timer_cmd(1, Duration::from_secs(10)),
            ],
        ))
        .await
        .unwrap();

    let next = worker.poll_workflow_activation().await.unwrap();
    assert_eq!(next.run_id, run_id);

    assert!(
        saw_force_new_wft.load(Ordering::Relaxed),
        "request_rollover must force a replacement task"
    );
    assert_eq!(
        *markers.lock(),
        vec![(0, ParkReason::BudgetRollover, b"at-the-budget".to_vec())],
        "a budget rollover writes its marker without a finalization round trip -- the command \
         that asked for it already carried the terminal"
    );

    worker
        .complete_workflow_activation(WorkflowActivationCompletion::from_cmds(
            run_id,
            vec![CompleteWorkflowExecution::default().into()],
        ))
        .await
        .unwrap();
    worker.drain_pollers_and_shutdown().await;
}

// --- the reserved wake Signal (C11) ------------------------------------------

/// A `WakeSignal` payload, serialized exactly the way a producer sends one.
///
/// Built with the protocol's own serialization rather than through a
/// `DataConverter`, because Core is the component that has to read it.
fn wake_payload(wake: WakeSignal) -> Payload {
    Payload {
        metadata: HashMap::from([
            ("encoding".to_string(), b"binary/protobuf".to_vec()),
            (
                "messageType".to_string(),
                external_stream::WAKE_SIGNAL_MESSAGE_TYPE
                    .as_bytes()
                    .to_vec(),
            ),
        ]),
        data: wake.encode_to_vec(),
        ..Default::default()
    }
}

fn wake(park_generation: u64, first_execution_run_id: &str) -> WakeSignal {
    WakeSignal {
        envelope_version: external_stream::WAKE_SIGNAL_ENVELOPE_VERSION,
        stream_name: "tokens".to_string(),
        wait_id: 1,
        park_generation,
        first_execution_run_id: first_execution_run_id.to_string(),
        producer_session_id: "producer-a".to_string(),
    }
}

/// The external stream markers a completion recorded, with their terminal boundaries.
///
/// Reading the envelope back out of the command is what makes "exactly one marker per Workflow
/// Task" and "never without a terminal" checkable rather than assumed.
fn stream_markers(wft: &WorkflowTaskCompletion) -> Vec<(u64, ParkReason, Vec<u8>)> {
    wft.commands
        .iter()
        .filter_map(|c| match &c.attributes {
            Some(command::Attributes::RecordMarkerCommandAttributes(m))
                if m.marker_name == EXTERNAL_STREAM_MARKER_NAME =>
            {
                extract_external_stream_marker_data(&m.details)
            }
            _ => None,
        })
        .map(|d| {
            (
                d.quiescence_generation,
                d.terminal_boundary(),
                d.replay_annotation,
            )
        })
        .collect()
}

/// A worker whose second Workflow Task carries one Signal.
fn worker_with_a_signal(signal_name: &str, payloads: Vec<Payload>) -> crate::Worker {
    let mut t = TestHistoryBuilder::default();
    t.add_by_type(EventType::WorkflowExecutionStarted);
    t.add_full_wf_task();
    t.add_we_signaled(signal_name, payloads);
    t.add_full_wf_task();
    t.add_workflow_execution_completed();

    let mut mock = build_mock_pollers(MockPollCfg::from_resp_batches(
        "fakeid",
        t,
        [1, 2],
        mock_worker_client(),
    ));
    mock.worker_cfg(|w| {
        w.task_types = WorkerTaskTypes::workflow_only();
        w.max_cached_workflows = 1;
    });
    mock_worker(mock)
}

/// Asserts that a wake Signal which failed validation changed nothing.
///
/// Two things must hold, and both are asserted against whatever Core actually produces rather
/// than against a particular activation shape: the Signal never reaches a user handler, and no
/// wait is resolved. Whether an activation arrives at all depends on the wait set's state -- a
/// still-blocked set retains the task and produces nothing, a parked one lets it complete -- and
/// neither outcome is the point.
async fn assert_the_wake_changed_nothing(worker: &crate::Worker, run_id: &str) {
    for _ in 0..4 {
        let polled = tokio::time::timeout(
            Duration::from_millis(200),
            worker.poll_workflow_activation(),
        )
        .await;
        let Ok(Ok(activation)) = polled else {
            // Nothing further to deliver: the task is retained, or the worker is done.
            return;
        };

        assert!(
            !activation.jobs.iter().any(|j| matches!(
                j.variant,
                Some(workflow_activation_job::Variant::SignalWorkflow(_))
            )),
            "the reserved Signal must be suppressed whether or not it validates, got {:?}",
            activation.jobs
        );
        assert_eq!(
            resolve_hints(&activation),
            Vec::<u32>::new(),
            "a wake Signal that failed validation must resolve no wait"
        );

        worker
            .complete_workflow_activation(WorkflowActivationCompletion::empty(activation.run_id))
            .await
            .unwrap();
    }
    let _ = run_id;
}

/// Completes the first (initialize) task and seeds the wait set for the second.
async fn advance_to_the_signal_task(
    worker: &crate::Worker,
    wait_ids: Vec<u32>,
    parked_at: Option<u64>,
) -> String {
    let first = worker.poll_workflow_activation().await.unwrap();
    let run_id = first.run_id.clone();
    worker
        .complete_workflow_activation(WorkflowActivationCompletion::empty(run_id.clone()))
        .await
        .unwrap();
    worker
        .seed_external_stream_waits(&run_id, wait_ids, parked_at, false)
        .await;
    run_id
}

#[tokio::test]
async fn an_unparked_wake_resumes_the_run() {
    // `park_generation = 0` is the unparked wake: the sender knows of no confirmed park and is
    // asking for a Workflow Task anyway. Core validates chain identity and otherwise accepts it
    // as a recheck request, because the runtime rechecks every subscription on wakeup regardless
    // and an unnecessary one costs at most one empty Workflow Task.
    let worker = worker_with_a_signal(
        external_stream::WAKE_SIGNAL_NAME,
        vec![wake_payload(wake(0, ""))],
    );
    advance_to_the_signal_task(&worker, vec![1, 2], None).await;

    let second = worker.poll_workflow_activation().await.unwrap();

    // The Signal itself never reaches a user handler, and every active wait is named -- the
    // Signal's stream is a hint, not an exhaustive claim.
    assert!(
        !second.jobs.iter().any(|j| matches!(
            j.variant,
            Some(workflow_activation_job::Variant::SignalWorkflow(_))
        )),
        "the reserved Signal must be suppressed from user handlers, got {:?}",
        second.jobs
    );
    assert_eq!(resolve_hints(&second), vec![1, 2]);

    worker
        .complete_workflow_activation(WorkflowActivationCompletion::from_cmds(
            second.run_id,
            vec![CompleteWorkflowExecution::default().into()],
        ))
        .await
        .unwrap();
    worker.drain_pollers_and_shutdown().await;
}

#[tokio::test]
async fn a_wake_naming_a_recognized_park_generation_resumes_the_run() {
    // The park handshake that would produce this generation live is C8, so it is injected here
    // -- what is under test is the classification, not how the generation came to exist.
    let worker = worker_with_a_signal(
        external_stream::WAKE_SIGNAL_NAME,
        vec![wake_payload(wake(7, ""))],
    );
    advance_to_the_signal_task(&worker, vec![1], Some(7)).await;

    let second = worker.poll_workflow_activation().await.unwrap();

    assert_eq!(resolve_hints(&second), vec![1]);
    worker
        .complete_workflow_activation(WorkflowActivationCompletion::from_cmds(
            second.run_id,
            vec![CompleteWorkflowExecution::default().into()],
        ))
        .await
        .unwrap();
    worker.drain_pollers_and_shutdown().await;
}

#[tokio::test]
async fn a_stale_generation_neither_resumes_nor_reaches_a_handler() {
    // A *non-zero* generation the Run does not recognise is a claim that turned out to be wrong,
    // so it is ignored -- but it is still suppressed, because a Signal that failed validation
    // reaching Workflow code as an unhandled Signal would be worse than dropping it.
    let worker = worker_with_a_signal(
        external_stream::WAKE_SIGNAL_NAME,
        vec![wake_payload(wake(99, ""))],
    );
    let run_id = advance_to_the_signal_task(&worker, vec![1], Some(7)).await;

    assert_the_wake_changed_nothing(&worker, &run_id).await;
    worker.drain_pollers_and_shutdown().await;
}

#[tokio::test]
async fn an_unknown_envelope_version_is_ignored_harmlessly() {
    // An old Core must not break because a newer producer learned a new field.
    let mut envelope = wake(0, "");
    envelope.envelope_version = 99;
    let worker = worker_with_a_signal(
        external_stream::WAKE_SIGNAL_NAME,
        vec![wake_payload(envelope)],
    );
    let run_id = advance_to_the_signal_task(&worker, vec![1], None).await;

    assert_the_wake_changed_nothing(&worker, &run_id).await;
    worker.drain_pollers_and_shutdown().await;
}

#[tokio::test]
async fn a_foreign_chain_neither_resumes_nor_reaches_a_handler() {
    // Same chain plus an unknown generation is harmless; a *different* chain is a mis-addressed
    // message, and honouring it would wake a Workflow on another Workflow's data.
    let worker = worker_with_a_signal(
        external_stream::WAKE_SIGNAL_NAME,
        vec![wake_payload(wake(0, "some-other-chain"))],
    );
    let run_id = advance_to_the_signal_task(&worker, vec![1], None).await;

    assert_the_wake_changed_nothing(&worker, &run_id).await;
    worker.drain_pollers_and_shutdown().await;
}

#[tokio::test]
async fn an_ordinary_signal_still_reaches_its_handler() {
    // The interception must be by name and nothing else.
    let worker = worker_with_a_signal("a-user-signal", vec![]);
    let first = worker.poll_workflow_activation().await.unwrap();
    worker
        .complete_workflow_activation(WorkflowActivationCompletion::empty(first.run_id))
        .await
        .unwrap();

    let second = worker.poll_workflow_activation().await.unwrap();
    assert!(
        second.jobs.iter().any(|j| matches!(
            &j.variant,
            Some(workflow_activation_job::Variant::SignalWorkflow(s))
                if s.signal_name == "a-user-signal"
        )),
        "an ordinary Signal must reach its handler, got {:?}",
        second.jobs
    );

    worker
        .complete_workflow_activation(WorkflowActivationCompletion::from_cmds(
            second.run_id,
            vec![CompleteWorkflowExecution::default().into()],
        ))
        .await
        .unwrap();
    worker.drain_pollers_and_shutdown().await;
}

// --- marker emission (C9, C14b) ----------------------------------------------

/// A history for a Workflow Task that records a marker and then starts a timer.
///
/// Commands are matched to history events in order, so the marker event has to be there and has
/// to come first -- a history missing it hands the marker machine whatever event is next.
fn marker_then_timer_history(
    quiescence_generation: u64,
    terminal: ParkReason,
    annotation: &[u8],
) -> TestHistoryBuilder {
    let mut t = TestHistoryBuilder::default();
    t.add_by_type(EventType::WorkflowExecutionStarted);
    t.add_full_wf_task();
    t.add_external_stream_marker(quiescence_generation, terminal, annotation);
    let timer_started = t.add_by_type(EventType::TimerStarted);
    t.add_timer_fired(timer_started, "1".to_string());
    t.add_workflow_task_scheduled_and_started();
    t
}

/// A worker that records the markers on every completion.
fn worker_recording_markers(
    markers: Arc<Mutex<Vec<(u64, ParkReason, Vec<u8>)>>>,
    batches: [usize; 2],
    history: TestHistoryBuilder,
) -> crate::Worker {
    let t = history;
    let mut mock_cfg = MockPollCfg::from_resp_batches("fakeid", t, batches, mock_worker_client());
    mock_cfg.completion_asserts_from_expectations(|mut asserts| {
        for _ in 0..4 {
            let collected = markers.clone();
            asserts.then(move |wft| {
                collected.lock().extend(stream_markers(wft));
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
async fn several_progress_reports_collapse_into_one_marker() {
    // The claim the design's History cost rests on: a retained Workflow Task may span many
    // activations, and it writes **one** marker however many of them reported progress.
    let markers: Arc<Mutex<Vec<(u64, ParkReason, Vec<u8>)>>> = Default::default();
    let worker = worker_recording_markers(
        markers.clone(),
        [1, 2],
        // Generation 3: the task became quiescent three times before it ended, and the marker
        // closes the last of those snapshots.
        marker_then_timer_history(3, ParkReason::CommandsProduced, b"onetwothreefour"),
    );

    let activation = worker.poll_workflow_activation().await.unwrap();
    let run_id = activation.run_id.clone();

    // Three activations under one Workflow Task, each reporting progress.
    worker
        .complete_workflow_activation(WorkflowActivationCompletion::from_cmds(
            run_id.clone(),
            vec![
                progress_command(b"one", false).into(),
                quiescent_command(1, &[1], Duration::from_secs(30)).into(),
            ],
        ))
        .await
        .unwrap();
    for delta in [b"two".as_slice(), b"three".as_slice()] {
        worker.notify_external_stream_ready(&run_id, 1, 0).await;
        let _ = worker.poll_workflow_activation().await.unwrap();
        worker
            .complete_workflow_activation(WorkflowActivationCompletion::from_cmds(
                run_id.clone(),
                vec![
                    progress_command(delta, false).into(),
                    quiescent_command(1, &[1], Duration::from_secs(30)).into(),
                ],
            ))
            .await
            .unwrap();
    }
    assert_eq!(
        *markers.lock(),
        Vec::new(),
        "a retained task writes nothing"
    );

    // Now the task ends.
    worker.notify_external_stream_ready(&run_id, 1, 0).await;
    let _ = worker.poll_workflow_activation().await.unwrap();
    worker
        .complete_workflow_activation(WorkflowActivationCompletion::from_cmds(
            run_id.clone(),
            vec![
                progress_command(b"four", false).into(),
                start_timer_cmd(1, Duration::from_secs(10)),
            ],
        ))
        .await
        .unwrap();
    let _ = worker.poll_workflow_activation().await.unwrap();

    let written = markers.lock().clone();
    assert_eq!(written.len(), 1, "exactly one marker per Workflow Task");
    let (_, terminal, annotation) = &written[0];
    assert_eq!(*terminal, ParkReason::CommandsProduced);
    assert_eq!(
        annotation, b"onetwothreefour",
        "the marker carries every delta the task accumulated, concatenated in order"
    );

    worker
        .complete_workflow_activation(WorkflowActivationCompletion::from_cmds(
            run_id,
            vec![CompleteWorkflowExecution::default().into()],
        ))
        .await
        .unwrap();
    worker.drain_pollers_and_shutdown().await;
}

#[tokio::test]
async fn a_terminal_command_writes_its_marker_ordered_before_it() {
    // Command ordering is normative: on replay this guarantees a record's integrity is validated
    // before the command derived from it is matched.
    let markers: Arc<Mutex<Vec<(u64, ParkReason, Vec<u8>)>>> = Default::default();
    let ordering: Arc<Mutex<Vec<i32>>> = Default::default();
    let recorded = ordering.clone();

    let t = canned_histories::single_timer("1");
    let mut mock_cfg = MockPollCfg::from_resp_batches("fakeid", t, [1], mock_worker_client());
    let collected = markers.clone();
    mock_cfg.completion_asserts_from_expectations(|mut asserts| {
        asserts.then(move |wft| {
            collected.lock().extend(stream_markers(wft));
            recorded
                .lock()
                .extend(wft.commands.iter().map(|c| c.command_type));
        });
    });
    let mut mock = build_mock_pollers(mock_cfg);
    mock.worker_cfg(|w| {
        w.task_types = WorkerTaskTypes::workflow_only();
        w.max_cached_workflows = 1;
    });
    let worker = mock_worker(mock);

    let activation = worker.poll_workflow_activation().await.unwrap();
    worker
        .complete_workflow_activation(WorkflowActivationCompletion::from_cmds(
            activation.run_id,
            vec![
                progress_command(b"consumed", false).into(),
                CompleteWorkflowExecution::default().into(),
            ],
        ))
        .await
        .unwrap();

    let written = markers.lock().clone();
    assert_eq!(written.len(), 1);
    assert_eq!(written[0].1, ParkReason::WorkflowCompleted);

    let commands = ordering.lock().clone();
    let marker_at = commands
        .iter()
        .position(|c| *c == CommandType::RecordMarker as i32)
        .expect("the marker must be in the completion");
    let terminal_at = commands
        .iter()
        .position(|c| *c == CommandType::CompleteWorkflowExecution as i32)
        .expect("the terminal command must be in the completion");
    assert!(
        marker_at < terminal_at,
        "the marker must precede the terminal command, got {commands:?}"
    );

    worker.drain_pollers_and_shutdown().await;
}

#[tokio::test]
async fn a_workflow_task_that_observed_nothing_writes_no_marker() {
    // Not an error: a Workflow Task that touched no stream is the ordinary case, and writing an
    // empty marker for it would put a History event on every task in the Workflow.
    let markers: Arc<Mutex<Vec<(u64, ParkReason, Vec<u8>)>>> = Default::default();
    let worker =
        worker_recording_markers(markers.clone(), [1, 2], canned_histories::single_timer("1"));

    let activation = worker.poll_workflow_activation().await.unwrap();
    let run_id = activation.run_id.clone();
    worker
        .complete_workflow_activation(WorkflowActivationCompletion::from_cmds(
            run_id.clone(),
            vec![start_timer_cmd(1, Duration::from_secs(10))],
        ))
        .await
        .unwrap();
    let _ = worker.poll_workflow_activation().await.unwrap();

    assert_eq!(*markers.lock(), Vec::new());

    worker
        .complete_workflow_activation(WorkflowActivationCompletion::from_cmds(
            run_id,
            vec![CompleteWorkflowExecution::default().into()],
        ))
        .await
        .unwrap();
    worker.drain_pollers_and_shutdown().await;
}

#[tokio::test]
async fn an_annotation_with_no_terminal_is_refused_rather_than_written() {
    // The refusal belongs to emission itself and needs no finalization job to state it: an
    // annotation without its terminal is durable and wrong, while an abandoned Workflow Task
    // commits no cursor and loses no record. So there is no best-effort path.
    //
    // Driven directly at the emission primitive, because no completion path can produce this --
    // which is the point: the guard exists for a future path that forgets.
    let markers: Arc<Mutex<Vec<(u64, ParkReason, Vec<u8>)>>> = Default::default();
    let worker =
        worker_recording_markers(markers.clone(), [1, 2], canned_histories::single_timer("1"));

    let activation = worker.poll_workflow_activation().await.unwrap();
    let run_id = activation.run_id.clone();

    assert!(
        worker
            .emit_terminal_less_external_stream_marker(&run_id)
            .await,
        "emitting an annotation with no terminal boundary must be refused"
    );

    // And nothing was written: the completion that follows carries no marker at all.
    worker
        .complete_workflow_activation(WorkflowActivationCompletion::from_cmds(
            run_id.clone(),
            vec![start_timer_cmd(1, Duration::from_secs(10))],
        ))
        .await
        .unwrap();
    let _ = worker.poll_workflow_activation().await.unwrap();

    assert_eq!(
        *markers.lock(),
        Vec::new(),
        "a refused emission must write nothing rather than writing a truncated annotation"
    );

    worker
        .complete_workflow_activation(WorkflowActivationCompletion::from_cmds(
            run_id,
            vec![CompleteWorkflowExecution::default().into()],
        ))
        .await
        .unwrap();
    worker.drain_pollers_and_shutdown().await;
}
