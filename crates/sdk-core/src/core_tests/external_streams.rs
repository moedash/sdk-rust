//! External Workflow Stream routing and entry points (C3, C4).
//!
//! These drive the real serialized local-input lane through a mock worker, so what is under test
//! is the routing -- that an input reaches the right run, that an acknowledgement comes back, and
//! that the read-only probe is genuinely read-only. The wait set's own transition logic is
//! covered by unit tests next to it.

use crate::{
    ExternalStreamReadyResult, ExternalStreamRunStatus, PollError,
    replay::{TestHistoryBuilder, canned_histories},
    test_help::{
        MockPollCfg, WorkerExt, build_fake_worker, build_mock_pollers, mock_worker,
        schedule_activity_cmd, start_timer_cmd,
    },
    worker::client::{WorkflowTaskCompletion, mocks::mock_worker_client},
};
use parking_lot::Mutex;
use prost::Message as _;
use std::{
    collections::{HashMap, HashSet},
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::Duration,
};
use temporalio_common::{
    protos::coresdk::{
        external_data::{
            ExternalStreamMarkerData, ParkReason, extract_external_stream_marker_data,
        },
        external_stream::{self, WakeSignal},
        workflow_activation::{WorkflowActivation, workflow_activation_job},
        workflow_commands::{
            ActivityCancellationType, CompleteWorkflowExecution, ContinueAsNewWorkflowExecution,
            ExternalStreamFinalized, ExternalStreamParkResult, ExternalStreamWait,
            FailWorkflowExecution, ParkSetConfirmed, StreamSetBecameReady, WorkflowStreamProgress,
            WorkflowStreamQuiescent, external_stream_park_result, workflow_command,
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

/// One recorded external stream marker: the snapshot it closed, its terminal, its annotation.
type StreamMarker = (u64, ParkReason, Vec<u8>);
/// Markers collected across a test's completions, in the order they were reported.
type StreamMarkers = Arc<Mutex<Vec<StreamMarker>>>;

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

// --- idle timeout routing (C3) -----------------------------------------------

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
async fn the_idle_timer_fires_and_starts_the_park_handshake() {
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

    // The timer fires and the set enters `Parking`, which Core expresses by asking lang to run
    // the handshake. Nothing was reported to the server: parking is what *ends* the task, and it
    // has not been confirmed yet.
    let park = worker.poll_workflow_activation().await.unwrap();
    assert_eq!(park_jobs(&park), vec![(1, ParkReason::Idle, vec![1])]);
    assert_eq!(
        completions.load(Ordering::Relaxed),
        0,
        "asking lang to park must not complete the Workflow Task"
    );

    // Answering `became_ready` releases the set, which is enough to let this test end without
    // parking; the handshake's own outcomes are C8's tests.
    worker
        .complete_workflow_activation(WorkflowActivationCompletion::from_cmd(
            run_id.clone(),
            park_became_ready_command(1),
        ))
        .await
        .unwrap();
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
    mock_cfg.num_expected_fails = 1;
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
                quiescent_command(1, &[1], Duration::from_secs(30)),
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

#[tokio::test]
async fn a_pending_timer_suppresses_retention_but_its_subscriptions_survive_it() {
    // Retention is the only thing a pending timer suppresses. The wait set is what a subscription
    // *is* on this side, so dropping it along with the retention would leave the Workflow blocked
    // on streams Core no longer knows about: every later readiness would answer `RunNotFound`, the
    // watcher would tear itself down, and nothing would ever deliver the next record.
    let markers: StreamMarkers = Default::default();
    let worker = worker_recording_markers(
        markers.clone(),
        vec![1, 2],
        marker_then_timer_history(1, ParkReason::CommandsProduced, b"onetwo"),
    );

    let activation = worker.poll_workflow_activation().await.unwrap();
    let run_id = activation.run_id.clone();

    // A quiescent snapshot registers both subscriptions.
    worker
        .complete_workflow_activation(WorkflowActivationCompletion::from_cmds(
            run_id.clone(),
            vec![
                progress_command(b"one", false),
                quiescent_command(1, &[1, 2], Duration::from_secs(30)),
            ],
        ))
        .await
        .unwrap();

    // The activation that drains a record also starts a timer, and that is what ends the task.
    worker.notify_external_stream_ready(&run_id, 1, 0).await;
    let resolved = worker.poll_workflow_activation().await.unwrap();
    assert_eq!(resolve_hints(&resolved), vec![1]);
    worker
        .complete_workflow_activation(WorkflowActivationCompletion::from_cmds(
            run_id.clone(),
            vec![
                progress_command(b"two", false),
                start_timer_cmd(1, Duration::from_secs(10)),
            ],
        ))
        .await
        .unwrap();

    assert_eq!(
        *markers.lock(),
        vec![(1, ParkReason::CommandsProduced, b"onetwo".to_vec())],
        "the task ended, so its consumption is committed with it"
    );

    // The next Workflow Task finds the same wait set at the same generations: nothing had to be
    // re-registered for readiness to be deliverable locally again. The window *between* the two
    // tasks is not asserted here -- the replacement arrives on the mock's own schedule, so which
    // side of it a probe lands on is a race; `readiness_for_a_cached_run_with_no_open_task_is_not\
    // _run_not_found` pins that state deterministically instead.
    let fired = worker.poll_workflow_activation().await.unwrap();
    assert_eq!(fired.run_id, run_id);
    assert_eq!(
        worker.external_stream_run_status(&run_id).await,
        ExternalStreamRunStatus::WftOpen,
        "the surviving wait set must hold the replacement task open too"
    );
    for wait_id in [1, 2] {
        assert_eq!(
            worker
                .notify_external_stream_ready(&run_id, wait_id, 0)
                .await,
            ExternalStreamReadyResult::Accepted,
            "wait {wait_id} must have survived the command-producing completion at generation 0"
        );
    }

    worker
        .complete_workflow_activation(WorkflowActivationCompletion::from_cmds(
            run_id,
            vec![CompleteWorkflowExecution::default().into()],
        ))
        .await
        .unwrap();
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
fn resolve_hints(activation: &WorkflowActivation) -> Vec<u32> {
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
                progress_command(b"first", false),
                quiescent_command(1, &[1], Duration::from_secs(30)),
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
                progress_command(b"second", false),
                quiescent_command(2, &[1], Duration::from_secs(30)),
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
                progress_command(b"", false),
                quiescent_command(1, &[1], Duration::from_secs(30)),
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
    let markers: StreamMarkers = Default::default();
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
                progress_command(b"consumed", false),
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
    mock_cfg.num_expected_fails = 1;
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
                progress_command(b"too late", false),
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

/// A history whose first Workflow Task records a marker and is then replaced.
///
/// Commands are matched to history events in order, so a rollover that writes a marker needs the
/// marker event present and first -- a history missing it hands the marker machine whatever event
/// happens to be next.
fn marker_then_replacement_history(
    quiescence_generation: u64,
    terminal: ParkReason,
    annotation: &[u8],
) -> TestHistoryBuilder {
    let mut t = TestHistoryBuilder::default();
    t.add_by_type(EventType::WorkflowExecutionStarted);
    t.add_full_wf_task();
    t.add_external_stream_marker(quiescence_generation, terminal, annotation);
    t.add_workflow_task_scheduled_and_started();
    t
}

/// The `FinalizeExternalStreams` jobs in an activation, as (generation, reason, wait ids).
fn finalization_jobs(activation: &WorkflowActivation) -> Vec<(u64, ParkReason, Vec<u32>)> {
    activation
        .jobs
        .iter()
        .filter_map(|j| match &j.variant {
            Some(workflow_activation_job::Variant::FinalizeExternalStreams(f)) => Some((
                f.quiescence_generation,
                f.reason(),
                f.waits.iter().map(|w| w.wait_id).collect(),
            )),
            _ => None,
        })
        .collect()
}

/// Lang's answer to a finalization job: the terminal Core cannot manufacture.
fn finalized_command(quiescence_generation: u64, delta: &[u8]) -> workflow_command::Variant {
    workflow_command::Variant::ExternalStreamFinalized(ExternalStreamFinalized {
        quiescence_generation,
        final_observation_delta: delta.to_vec(),
    })
}

/// A workflow-only worker recording both markers and `force_new_wft` per completion.
fn worker_recording_rollovers(
    markers: StreamMarkers,
    forced: Arc<Mutex<Vec<bool>>>,
    history: TestHistoryBuilder,
    batches: Vec<usize>,
) -> crate::Worker {
    let mut mock_cfg =
        MockPollCfg::from_resp_batches("fakeid", history, batches, mock_worker_client());
    mock_cfg.completion_asserts_from_expectations(|mut asserts| {
        for _ in 0..4 {
            let collected = markers.clone();
            let forced = forced.clone();
            asserts.then(move |wft| {
                collected.lock().extend(stream_markers(wft));
                forced.lock().push(wft.force_create_new_workflow_task);
            });
        }
    });
    let mut mock = build_mock_pollers(mock_cfg);
    mock.worker_cfg(|w| {
        // Exactly the worker ADR-017 is about: no local activities, so no request sink.
        w.task_types = WorkerTaskTypes::workflow_only();
        w.max_cached_workflows = 1;
    });
    mock_worker(mock)
}

/// Retains a task holding `annotation`, then fires the rollover deadline.
///
/// Returns the run id. On return the finalization job has not yet been polled.
async fn retain_then_fire_the_rollover_deadline(
    worker: &crate::Worker,
    annotation: &[u8],
) -> String {
    let activation = worker.poll_workflow_activation().await.unwrap();
    let run_id = activation.run_id.clone();
    worker
        .complete_workflow_activation(WorkflowActivationCompletion::from_cmds(
            run_id.clone(),
            vec![
                progress_command(annotation, false),
                quiescent_command(1, &[1, 2], Duration::from_secs(30)),
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
    run_id
}

#[tokio::test]
async fn a_core_decided_boundary_asks_for_a_terminal_before_writing_anything() {
    // The C15a protocol, end to end. Core decided this boundary from a timer, so it has no
    // terminal for it -- only lang can encode the blocked cursor snapshot. Core must therefore
    // ask, and must write nothing at all until the answer arrives.
    let markers: StreamMarkers = Default::default();
    let forced: Arc<Mutex<Vec<bool>>> = Default::default();
    let worker = worker_recording_rollovers(
        markers.clone(),
        forced.clone(),
        marker_then_replacement_history(1, ParkReason::Rollover, b"before-rollover-terminal"),
        vec![1, 2],
    );

    let run_id = retain_then_fire_the_rollover_deadline(&worker, b"before-rollover").await;

    // The deadline produced a finalization job covering the *complete* active wait set, not a
    // completion.
    let finalize = worker.poll_workflow_activation().await.unwrap();
    assert_eq!(
        finalization_jobs(&finalize),
        vec![(1, ParkReason::Rollover, vec![1, 2])],
        "a rollover deadline must ask lang to finalize, got {:?}",
        finalize.jobs
    );

    // Nothing has been written and nothing has been reported: the annotation is still held,
    // because a marker without its terminal is durable and wrong.
    assert_eq!(*markers.lock(), Vec::new());
    assert_eq!(
        *forced.lock(),
        Vec::<bool>::new(),
        "the task must stay open until its terminal exists"
    );
    assert_eq!(
        worker.external_stream_annotation(&run_id).await,
        b"before-rollover",
        "the accumulated annotation is held across the round trip, not discarded"
    );

    // Lang supplies the terminal, and only now is the marker written.
    worker
        .complete_workflow_activation(WorkflowActivationCompletion::from_cmd(
            run_id.clone(),
            finalized_command(1, b"-terminal"),
        ))
        .await
        .unwrap();

    assert_eq!(
        *markers.lock(),
        vec![(
            1,
            ParkReason::Rollover,
            b"before-rollover-terminal".to_vec()
        )],
        "the marker carries the accumulated annotation with lang's terminal appended"
    );
    assert_eq!(
        *forced.lock(),
        vec![true],
        "the completion carrying the marker is also the one that requests a replacement"
    );
    assert!(
        worker.external_stream_annotation(&run_id).await.is_empty(),
        "the annotation is cleared once its marker is written"
    );

    // And the wait set survived onto the replacement task, which is retained exactly as its
    // predecessor was -- the durable half of rollover does not cost the transport half. This poll
    // lets the replacement in and hands nothing back, which is itself the retention assertion.
    assert!(
        tokio::time::timeout(
            Duration::from_millis(400),
            worker.poll_workflow_activation()
        )
        .await
        .is_err(),
        "the replacement task must be retained too, or the rollover undoes itself"
    );
    assert_eq!(
        worker.notify_external_stream_ready(&run_id, 2, 0).await,
        ExternalStreamReadyResult::Accepted,
        "wait 2 must still be registered at generation 0 across the rollover"
    );
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
async fn a_finalization_answered_without_a_terminal_writes_no_marker() {
    // There is no best-effort path. If the terminal cannot be obtained, Core writes nothing and
    // the Workflow Task is retried -- an abandoned task commits no cursor and loses no record,
    // while a truncated annotation is durable and wrong.
    //
    // "Writes no marker" is asserted as *zero successful completions*: a failed Workflow Task
    // reports no commands at all, so a marker could only have escaped through a completion that
    // must not exist. Both counts are verified by the mock when it drops.
    let mut mock_cfg = MockPollCfg::from_resp_batches(
        "fakeid",
        marker_then_replacement_history(1, ParkReason::Rollover, b"never-written"),
        [1],
        mock_worker_client(),
    );
    mock_cfg.num_expected_fails = 1;
    mock_cfg.num_expected_completions = Some(0.into());
    let mut mock = build_mock_pollers(mock_cfg);
    mock.worker_cfg(|w| {
        w.task_types = WorkerTaskTypes::workflow_only();
        w.max_cached_workflows = 1;
    });
    let worker = mock_worker(mock);

    let run_id = retain_then_fire_the_rollover_deadline(&worker, b"before-rollover").await;
    let finalize = worker.poll_workflow_activation().await.unwrap();
    assert_eq!(
        finalization_jobs(&finalize).len(),
        1,
        "the deadline must have asked for a terminal, or this test proves nothing"
    );

    // Lang answers the finalization job with anything other than a terminal.
    worker
        .complete_workflow_activation(WorkflowActivationCompletion::empty(run_id.clone()))
        .await
        .unwrap();

    assert_ne!(
        worker.external_stream_run_status(&run_id).await,
        ExternalStreamRunStatus::WftOpen,
        "a finalization that produced no terminal must fail the Workflow Task"
    );

    worker.shutdown().await;
    worker.finalize_shutdown().await;
}

#[tokio::test]
async fn a_finalized_command_with_no_job_outstanding_is_refused() {
    // The paired negative. Without it, accepting `ExternalStreamFinalized` unconditionally would
    // let lang append a terminal to an annotation Core never asked it to close -- and the
    // "answered correctly" case above would pass either way.
    let mut mock_cfg = MockPollCfg::from_resp_batches(
        "fakeid",
        canned_histories::single_timer("1"),
        [1],
        mock_worker_client(),
    );
    mock_cfg.num_expected_fails = 1;
    mock_cfg.num_expected_completions = Some(0.into());
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
                progress_command(b"unprompted", false),
                finalized_command(1, b"-terminal"),
            ],
        ))
        .await
        .unwrap();

    assert_ne!(
        worker.external_stream_run_status(&run_id).await,
        ExternalStreamRunStatus::WftOpen,
        "an unprompted terminal must fail the Workflow Task rather than be accepted"
    );

    worker.shutdown().await;
    worker.finalize_shutdown().await;
}

#[tokio::test]
async fn a_rollover_with_nothing_accumulated_asks_for_no_terminal() {
    // C12a's half, still true: with no annotation there is no marker to write, so there is
    // nothing to finalize either. The task is still replaced -- rollover is about the deadline,
    // not about the stream having produced anything.
    let markers: StreamMarkers = Default::default();
    let forced: Arc<Mutex<Vec<bool>>> = Default::default();
    let worker = worker_recording_rollovers(
        markers.clone(),
        forced.clone(),
        canned_histories::single_timer("1"),
        vec![1, 1],
    );

    let activation = worker.poll_workflow_activation().await.unwrap();
    let run_id = activation.run_id.clone();
    // Retention with no progress at all: nothing was ever observed.
    worker
        .complete_workflow_activation(WorkflowActivationCompletion::from_cmd(
            run_id.clone(),
            quiescent_command(1, &[1], Duration::from_secs(30)),
        ))
        .await
        .unwrap();
    worker
        .start_wft_rollover_timer(&run_id, Duration::from_millis(50))
        .await;

    // No finalization job is issued, so the replacement task is simply retained again and the
    // poll has nothing to hand back.
    assert!(
        tokio::time::timeout(
            Duration::from_millis(600),
            worker.poll_workflow_activation()
        )
        .await
        .is_err(),
        "with nothing to finalize the replacement task is retained, not activated"
    );
    assert_eq!(*markers.lock(), Vec::new());
    assert_eq!(
        *forced.lock(),
        vec![true],
        "the deadline still requests a replacement even with no marker to write"
    );

    // The wait set survived onto the replacement exactly as C12a requires.
    assert_eq!(
        worker.notify_external_stream_ready(&run_id, 1, 0).await,
        ExternalStreamReadyResult::Accepted,
        "wait 1 must still be registered at generation 0 across the rollover"
    );
    let resolved = worker.poll_workflow_activation().await.unwrap();
    assert_eq!(resolve_hints(&resolved), vec![1]);
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
    let markers: StreamMarkers = Default::default();
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
                progress_command(b"at-the-budget", true),
                start_timer_cmd(1, Duration::from_secs(10)),
            ],
        ))
        .await
        .unwrap();

    let next = worker.poll_workflow_activation().await.unwrap();
    assert_eq!(next.run_id, run_id);
    assert_eq!(
        finalization_jobs(&next),
        Vec::new(),
        "a budget rollover needs no finalization round trip -- the command that asked for it \
         already carried the terminal"
    );

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

#[tokio::test]
async fn two_consecutive_rollovers_produce_two_markers_that_reassemble_in_order() {
    // A batch split across two Workflow Tasks must be recoverable, and the only thing that makes
    // it recoverable is that each marker carries its own task's deltas and the two concatenate in
    // task order. A marker that repeated or dropped a stretch would leave replay unable to say
    // what the original run actually saw.
    let markers: StreamMarkers = Default::default();
    let forced: Arc<Mutex<Vec<bool>>> = Default::default();

    let mut t = TestHistoryBuilder::default();
    t.add_by_type(EventType::WorkflowExecutionStarted);
    t.add_full_wf_task();
    t.add_external_stream_marker(1, ParkReason::Rollover, b"first-half.terminal-one");
    t.add_full_wf_task();
    t.add_external_stream_marker(2, ParkReason::Rollover, b"second-half.terminal-two");
    t.add_workflow_task_scheduled_and_started();

    let worker = worker_recording_rollovers(markers.clone(), forced.clone(), t, vec![1, 2]);

    let activation = worker.poll_workflow_activation().await.unwrap();
    let run_id = activation.run_id.clone();

    // --- rollover one, on the task the run started with ---
    worker
        .complete_workflow_activation(WorkflowActivationCompletion::from_cmds(
            run_id.clone(),
            vec![
                progress_command(b"first-half", false),
                quiescent_command(1, &[1], Duration::from_secs(30)),
            ],
        ))
        .await
        .unwrap();
    worker
        .start_wft_rollover_timer(&run_id, Duration::from_millis(50))
        .await;
    let finalize = worker.poll_workflow_activation().await.unwrap();
    assert_eq!(finalization_jobs(&finalize).len(), 1);
    worker
        .complete_workflow_activation(WorkflowActivationCompletion::from_cmd(
            run_id.clone(),
            finalized_command(1, b".terminal-one"),
        ))
        .await
        .unwrap();

    // --- rollover two, on the replacement task ---
    // The replacement is retained the moment it arrives, so this poll hands nothing back; it
    // exists to let the task in. Readiness is what activates lang on it.
    assert!(
        tokio::time::timeout(
            Duration::from_millis(400),
            worker.poll_workflow_activation()
        )
        .await
        .is_err(),
        "the replacement task must be retained, or the rollover undoes itself"
    );
    assert_eq!(
        worker.notify_external_stream_ready(&run_id, 1, 0).await,
        ExternalStreamReadyResult::Accepted
    );
    let resumed = worker.poll_workflow_activation().await.unwrap();
    assert_eq!(resolve_hints(&resumed), vec![1]);
    worker
        .complete_workflow_activation(WorkflowActivationCompletion::from_cmds(
            run_id.clone(),
            vec![
                progress_command(b"second-half", false),
                quiescent_command(2, &[1], Duration::from_secs(30)),
            ],
        ))
        .await
        .unwrap();
    worker
        .start_wft_rollover_timer(&run_id, Duration::from_millis(50))
        .await;
    let finalize = worker.poll_workflow_activation().await.unwrap();
    assert_eq!(finalization_jobs(&finalize).len(), 1);
    worker
        .complete_workflow_activation(WorkflowActivationCompletion::from_cmd(
            run_id.clone(),
            finalized_command(2, b".terminal-two"),
        ))
        .await
        .unwrap();

    let written = markers.lock().clone();
    assert_eq!(
        written.len(),
        2,
        "one marker per Workflow Task, not one per run"
    );
    assert_eq!(
        written[0],
        (1, ParkReason::Rollover, b"first-half.terminal-one".to_vec())
    );
    assert_eq!(
        written[1],
        (
            2,
            ParkReason::Rollover,
            b"second-half.terminal-two".to_vec()
        )
    );

    // Reassembled in Workflow Task order the two markers are the whole batch, with nothing
    // repeated and nothing lost.
    let reassembled: Vec<u8> = written.iter().flat_map(|(_, _, a)| a.clone()).collect();
    assert_eq!(
        reassembled,
        b"first-half.terminal-onesecond-half.terminal-two"
    );
    assert_eq!(
        *forced.lock(),
        vec![true, true],
        "each rollover requests its own replacement task"
    );

    worker.drain_pollers_and_shutdown().await;
}

#[tokio::test]
async fn a_continuously_fed_stream_still_rolls_over() {
    // The workload the rollover deadline exists for, and the one a deadline re-armed at each
    // quiescence silently stops covering. A stream whose gaps stay below the idle timeout never
    // reaches the parking path, so nothing but the deadline ends the Workflow Task -- and becoming
    // quiescent again is precisely what a delivered record *does*. Anchored at the moment it is
    // armed, the deadline is pushed out by every record, the idle timer is clamped below it and so
    // cannot fire either, and the retained task runs until the *server* times it out, which is a
    // Workflow Task failure rather than a rollover.
    //
    // The existing deadline test drives a stream with no traffic at all, so it cannot see this: a
    // snapshot that is never renewed is one the arming moment and the task's start agree about.
    let markers: StreamMarkers = Default::default();
    let forced: Arc<Mutex<Vec<bool>>> = Default::default();

    // 500ms puts the rollover deadline 400ms after the task started and clamps the idle timeout to
    // at most 360ms, so a record every 20ms keeps the idle timer from ever firing.
    let mut t = TestHistoryBuilder::default();
    t.add_wfe_started_with_wft_timeout(Duration::from_millis(500));
    t.add_full_wf_task();
    t.add_workflow_task_scheduled_and_started();

    let worker = worker_recording_rollovers(markers.clone(), forced.clone(), t, vec![1]);

    let activation = worker.poll_workflow_activation().await.unwrap();
    let run_id = activation.run_id.clone();
    worker
        .complete_workflow_activation(WorkflowActivationCompletion::from_cmds(
            run_id.clone(),
            vec![
                progress_command(b"fed", false),
                quiescent_command(1, &[1], Duration::from_secs(30)),
            ],
        ))
        .await
        .unwrap();

    // Feed the stream, one record at a time, until the deadline ends the task.
    let started = std::time::Instant::now();
    let mut generation = 1u64;
    let finalize = loop {
        assert!(
            started.elapsed() < Duration::from_secs(5),
            "the rollover deadline never fired against a stream that kept feeding it: {generation} \
             quiescent snapshots and no finalization"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
        assert_eq!(
            worker.notify_external_stream_ready(&run_id, 1, 0).await,
            ExternalStreamReadyResult::Accepted,
            "the wait must still be deliverable, or this loop is not feeding anything"
        );
        let activation = worker.poll_workflow_activation().await.unwrap();
        if !finalization_jobs(&activation).is_empty() {
            break activation;
        }
        assert_eq!(
            park_jobs(&activation),
            Vec::new(),
            "a fed stream must not reach the idle parking path, got {:?}",
            activation.jobs
        );
        generation += 1;
        worker
            .complete_workflow_activation(WorkflowActivationCompletion::from_cmds(
                run_id.clone(),
                vec![
                    progress_command(b"fed", false),
                    quiescent_command(generation, &[1], Duration::from_secs(30)),
                ],
            ))
            .await
            .unwrap();
    };

    // Which snapshot the deadline caught is Core's business, not the loop's -- a completion that
    // arrives with the rollover already pending is not retained, so the snapshot it carried is
    // never recorded and the loop's own count runs ahead. What matters is that the boundary is a
    // rollover, that it covers the wait set, and that the stream really did re-establish
    // quiescence many times over before it fired.
    let jobs = finalization_jobs(&finalize);
    assert_eq!(jobs.len(), 1, "got {:?}", finalize.jobs);
    let (closed, reason, waits) = jobs[0].clone();
    assert_eq!(reason, ParkReason::Rollover);
    assert_eq!(waits, vec![1]);
    assert!(
        closed >= 3,
        "the stream must have re-established quiescence several times before the deadline fired, \
         or the case under test never arose; it closed generation {closed}"
    );

    worker
        .complete_workflow_activation(WorkflowActivationCompletion::from_cmd(
            run_id,
            finalized_command(closed, b"|end"),
        ))
        .await
        .unwrap();

    let written = markers.lock().clone();
    assert_eq!(written.len(), 1, "one Workflow Task gets one marker");
    assert_eq!(written[0].1, ParkReason::Rollover);
    assert!(written[0].2.ends_with(b"|end"), "got {:?}", written[0].2);
    assert_eq!(
        *forced.lock(),
        vec![true],
        "a rollover hands the work to a replacement task"
    );

    worker.drain_pollers_and_shutdown().await;
}

/// A stand-in for the encoder's `MAX_ANNOTATION_BYTES`.
///
/// The real constant lives in lang's codec, on the far side of an annotation Core cannot read.
/// Core's half of the mechanism is `request_rollover`, and what the budget means *to Core* is the
/// bound below: no marker Core writes may exceed it, however many bytes the batch as a whole ran
/// to.
const ANNOTATION_BUDGET: usize = 1200;
/// The fraction of the budget at which lang asks for a rollover on its next report.
const ANNOTATION_HIGH_WATER: usize = ANNOTATION_BUDGET / 2;
/// One activation's worth of observed bytes.
const OBSERVED_CHUNK: usize = 500;

#[tokio::test]
async fn a_budget_driven_split_writes_two_markers_rather_than_one_oversized_one() {
    // The byte budget, as the split it exists to force. Both halves are asserted in *bytes*: no
    // marker exceeds the budget, and the two together exceed it -- so the batch really could not
    // have fitted in one marker and the rollover was not decoration. A run-count assertion would
    // say nothing about either.
    //
    // Budget-driven rather than deadline-driven, which is what makes this a different mechanism
    // from `two_consecutive_rollovers_produce_two_markers_that_reassemble_in_order`: lang decided
    // both boundaries, so no `FinalizeExternalStreams` may be issued for either.
    let markers: StreamMarkers = Default::default();
    let forced: Arc<Mutex<Vec<bool>>> = Default::default();

    let mut t = TestHistoryBuilder::default();
    t.add_by_type(EventType::WorkflowExecutionStarted);
    t.add_full_wf_task();
    // Two quiescent snapshots passed before the first task hit the mark, and two more before the
    // second did, so the markers close generations 2 and 4.
    t.add_external_stream_marker(2, ParkReason::BudgetRollover, b"first-marker");
    t.add_full_wf_task();
    t.add_external_stream_marker(4, ParkReason::BudgetRollover, b"second-marker");
    t.add_workflow_task_scheduled_and_started();

    let worker = worker_recording_rollovers(markers.clone(), forced.clone(), t, vec![1, 2]);
    let chunk = vec![b'x'; OBSERVED_CHUNK];

    let activation = worker.poll_workflow_activation().await.unwrap();
    let run_id = activation.run_id.clone();

    // --- the first Workflow Task fills up ---
    worker
        .complete_workflow_activation(WorkflowActivationCompletion::from_cmds(
            run_id.clone(),
            vec![
                progress_command(&chunk, false),
                quiescent_command(1, &[1], Duration::from_secs(30)),
            ],
        ))
        .await
        .unwrap();
    assert!(
        worker.external_stream_annotation(&run_id).await.len() < ANNOTATION_HIGH_WATER,
        "the first chunk must be below the high-water mark, or the split under test is the wrong \
         one"
    );

    worker.notify_external_stream_ready(&run_id, 1, 0).await;
    let _ = worker.poll_workflow_activation().await.unwrap();
    worker
        .complete_workflow_activation(WorkflowActivationCompletion::from_cmds(
            run_id.clone(),
            vec![
                progress_command(&chunk, false),
                quiescent_command(2, &[1], Duration::from_secs(30)),
            ],
        ))
        .await
        .unwrap();
    assert!(
        worker.external_stream_annotation(&run_id).await.len() > ANNOTATION_HIGH_WATER,
        "the accumulated annotation must have passed the high-water mark, which is what lang \
         reacts to on its next report"
    );
    assert_eq!(
        *markers.lock(),
        Vec::new(),
        "passing the high-water mark is not itself a boundary -- lang asks on its *next* report"
    );

    // Lang saw the mark go by and asks for the rollover on its next report, which -- because lang
    // decided this boundary -- already carries the terminal.
    worker.notify_external_stream_ready(&run_id, 1, 0).await;
    let _ = worker.poll_workflow_activation().await.unwrap();
    worker
        .complete_workflow_activation(WorkflowActivationCompletion::from_cmd(
            run_id.clone(),
            progress_command(b"|first-terminal", true),
        ))
        .await
        .unwrap();

    assert_eq!(markers.lock().len(), 1, "the first marker closes here");
    assert_eq!(*forced.lock(), vec![true]);

    // --- the second Workflow Task, which the split created ---
    // The replacement is retained the moment it arrives, so this poll hands nothing back; it
    // exists to let the task in and to prove no finalization was asked for.
    assert!(
        tokio::time::timeout(
            Duration::from_millis(400),
            worker.poll_workflow_activation()
        )
        .await
        .is_err(),
        "a budget rollover needs no finalization round trip and must retain its replacement task"
    );
    assert!(
        worker.external_stream_annotation(&run_id).await.is_empty(),
        "the next Workflow Task starts a fresh annotation, which is what bounds the marker"
    );

    for (generation, delta) in [(3u64, &chunk), (4, &chunk)] {
        worker.notify_external_stream_ready(&run_id, 1, 0).await;
        let _ = worker.poll_workflow_activation().await.unwrap();
        worker
            .complete_workflow_activation(WorkflowActivationCompletion::from_cmds(
                run_id.clone(),
                vec![
                    progress_command(delta, false),
                    quiescent_command(generation, &[1], Duration::from_secs(30)),
                ],
            ))
            .await
            .unwrap();
    }
    worker.notify_external_stream_ready(&run_id, 1, 0).await;
    let _ = worker.poll_workflow_activation().await.unwrap();
    worker
        .complete_workflow_activation(WorkflowActivationCompletion::from_cmd(
            run_id.clone(),
            progress_command(b"|second-terminal", true),
        ))
        .await
        .unwrap();

    let written = markers.lock().clone();
    assert_eq!(
        written.len(),
        2,
        "the batch was split across two Workflow Tasks, so it is two markers"
    );
    for (generation, terminal, annotation) in &written {
        assert_eq!(
            *terminal,
            ParkReason::BudgetRollover,
            "both boundaries were lang's, and the marker must say so"
        );
        assert!(
            annotation.len() <= ANNOTATION_BUDGET,
            "the marker closing generation {generation} grew to {} bytes, past the budget the \
             rollover exists to keep it under",
            annotation.len()
        );
    }

    let reassembled: Vec<u8> = written.iter().flat_map(|(_, _, a)| a.clone()).collect();
    assert!(
        reassembled.len() > ANNOTATION_BUDGET,
        "the batch must not fit in one marker, or nothing here needed splitting"
    );
    let mut expected = chunk.repeat(2);
    expected.extend_from_slice(b"|first-terminal");
    expected.extend_from_slice(&chunk.repeat(2));
    expected.extend_from_slice(b"|second-terminal");
    assert_eq!(
        reassembled, expected,
        "reassembled in Workflow Task order the markers are the whole batch, with nothing \
         repeated and nothing lost"
    );
    assert_eq!(
        *forced.lock(),
        vec![true, true],
        "each half is its own Workflow Task, so each asks for its own replacement"
    );

    worker.drain_pollers_and_shutdown().await;
}

// --- shutdown and eviction transitions (C15b) --------------------------------

#[tokio::test]
async fn shutdown_with_a_workflow_task_open_writes_its_marker_and_forces_a_replacement() {
    // ADR-009's first row. The Run holds a Workflow Task the Worker is about to stop serving, so
    // Core closes that boundary itself: it asks lang for the terminal it cannot manufacture,
    // writes the one marker for the task, and completes requesting a replacement task -- which is
    // what offers the Run back to the task queue for another Worker to pick up.
    let markers: StreamMarkers = Default::default();
    let forced: Arc<Mutex<Vec<bool>>> = Default::default();
    let worker = worker_recording_rollovers(
        markers.clone(),
        forced.clone(),
        marker_then_replacement_history(1, ParkReason::Shutdown, b"before-shutdown-terminal"),
        vec![1],
    );

    let activation = worker.poll_workflow_activation().await.unwrap();
    let run_id = activation.run_id.clone();
    worker
        .complete_workflow_activation(WorkflowActivationCompletion::from_cmds(
            run_id.clone(),
            vec![
                progress_command(b"before-shutdown", false),
                quiescent_command(1, &[1, 2], Duration::from_secs(30)),
            ],
        ))
        .await
        .unwrap();
    assert_eq!(
        worker.external_stream_run_status(&run_id).await,
        ExternalStreamRunStatus::WftOpen
    );

    worker.initiate_shutdown();
    // The probe lang's own shutdown sweep makes of every Run with active subscriptions. `WftOpen`
    // is what tells lang to leave this Run to Core, and it is the same classification Core's sweep
    // keys off -- if the two disagreed a Run would be swept twice or not at all.
    assert_eq!(
        worker.external_stream_run_status(&run_id).await,
        ExternalStreamRunStatus::WftOpen
    );

    let finalize = worker.poll_workflow_activation().await.unwrap();
    assert_eq!(
        finalization_jobs(&finalize),
        vec![(1, ParkReason::Shutdown, vec![1, 2])],
        "shutdown with a Workflow Task open must ask lang to finalize the complete wait set, \
         got {:?}",
        finalize.jobs
    );
    assert_eq!(
        *markers.lock(),
        Vec::new(),
        "nothing may be written before the terminal arrives"
    );
    assert_eq!(
        *forced.lock(),
        Vec::<bool>::new(),
        "the task must stay open until its terminal exists"
    );
    assert_eq!(
        worker.external_stream_annotation(&run_id).await,
        b"before-shutdown",
        "the accumulated annotation is held across the round trip, not discarded"
    );

    worker
        .complete_workflow_activation(WorkflowActivationCompletion::from_cmd(
            run_id.clone(),
            finalized_command(1, b"-terminal"),
        ))
        .await
        .unwrap();

    assert_eq!(
        *markers.lock(),
        vec![(
            1,
            ParkReason::Shutdown,
            b"before-shutdown-terminal".to_vec()
        )],
        "the marker carries the accumulated annotation with lang's terminal appended, and says \
         which boundary closed it"
    );
    assert_eq!(
        *forced.lock(),
        vec![true],
        "the completion carrying the marker is also the one that hands the Run back"
    );

    // And with the boundary closed the Run owes nothing, so shutdown can finish -- which it could
    // not while a Workflow Task was still open, since that counts as pending work.
    assert!(
        matches!(
            worker.poll_workflow_activation().await,
            Err(PollError::ShutDown)
        ),
        "the Run must be released once its boundary is closed"
    );
    worker.shutdown().await;
    worker.finalize_shutdown().await;
}

#[tokio::test]
async fn shutdown_with_no_open_workflow_task_writes_no_marker_and_completes_nothing() {
    // ADR-009's second row, and the reason the two transitions are separate deliverables. Here
    // there is no task token to set `force_new_wft` on and nothing accumulated to write, so the
    // *correct* behaviour is to do nothing at all. The server-visible replacement is lang's wake
    // sweep; Core reimplementing it here would send Signals for Runs it is not entitled to speak
    // for.
    let markers: StreamMarkers = Default::default();
    let forced: Arc<Mutex<Vec<bool>>> = Default::default();
    let worker = worker_recording_rollovers(
        markers.clone(),
        forced.clone(),
        canned_histories::single_timer("1"),
        vec![1],
    );

    // The first activation is left outstanding for the whole test, which is what keeps the Run
    // cached: a Run that goes idle here is dropped when the mock runs out of work, and an evicted
    // Run is a different state from the one this is about.
    let activation = worker.poll_workflow_activation().await.unwrap();
    let run_id = activation.run_id.clone();
    worker
        .seed_external_stream_waits(&run_id, vec![1, 2], None, false)
        .await;
    assert_eq!(
        worker.external_stream_run_status(&run_id).await,
        ExternalStreamRunStatus::NoOpenWorkflowTask,
        "this test is only meaningful with subscriptions active and no Workflow Task"
    );

    worker.initiate_shutdown();
    // The same probe as above, and this time its answer is what keeps Core's sweep off the Run.
    // Answering it also drives the stream, so the sweep really did get its chance to run.
    assert_eq!(
        worker.external_stream_run_status(&run_id).await,
        ExternalStreamRunStatus::NoOpenWorkflowTask
    );

    // Core asked lang for nothing: the only activation in flight is still the original one, which
    // lang now answers on its own terms.
    worker
        .complete_workflow_activation(WorkflowActivationCompletion::from_cmds(
            run_id.clone(),
            vec![start_timer_cmd(1, Duration::from_secs(10))],
        ))
        .await
        .unwrap();

    assert_eq!(
        *markers.lock(),
        Vec::new(),
        "nothing was accumulated, so no marker is written and none is missing"
    );
    assert_eq!(
        *forced.lock(),
        vec![false],
        "lang's own completion must not be turned into a hand-back: with no Workflow Task of its \
         own to close, this transition asks the server for nothing"
    );
    assert!(
        matches!(
            worker.poll_workflow_activation().await,
            Err(PollError::ShutDown)
        ),
        "a Run with no open Workflow Task must produce no activation on the way out"
    );

    worker.shutdown().await;
    worker.finalize_shutdown().await;
}

#[tokio::test]
async fn evicting_a_run_with_no_open_workflow_task_writes_no_marker() {
    // Eviction reaches the same two states as shutdown and must make the same choice. With no
    // Workflow Task there is nothing to finalize, so the eviction activation goes out directly.
    //
    // "Writes no marker" is asserted as *zero completions*, the same way C15a's negative case is:
    // a marker reaches History only on a completion, so a mock that refuses every completion
    // proves no marker escaped by any route at all. The count is verified when the mock drops.
    let mut mock_cfg = MockPollCfg::from_resp_batches(
        "fakeid",
        canned_histories::single_timer("1"),
        [1],
        mock_worker_client(),
    );
    mock_cfg.num_expected_completions = Some(0.into());
    let mut mock = build_mock_pollers(mock_cfg);
    mock.worker_cfg(|w| {
        w.task_types = WorkerTaskTypes::workflow_only();
        w.max_cached_workflows = 1;
    });
    let worker = mock_worker(mock);

    let activation = worker.poll_workflow_activation().await.unwrap();
    let run_id = activation.run_id.clone();
    // Retaining keeps the Run cached with its task unreported, which is what lets the wait set be
    // put into -- and observed in -- the state under test.
    worker
        .complete_workflow_activation(WorkflowActivationCompletion::from_cmd(
            run_id.clone(),
            quiescent_command(1, &[1], Duration::from_secs(30)),
        ))
        .await
        .unwrap();
    worker
        .seed_external_stream_waits(&run_id, vec![1], None, false)
        .await;
    assert_eq!(
        worker.external_stream_run_status(&run_id).await,
        ExternalStreamRunStatus::NoOpenWorkflowTask,
        "this test is only meaningful with subscriptions active and no Workflow Task"
    );

    worker.request_workflow_eviction(&run_id);

    let evict = worker.poll_workflow_activation().await.unwrap();
    assert!(
        evict.is_only_eviction(),
        "with no Workflow Task open the eviction goes out directly -- nothing may be asked of \
         lang first, got {:?}",
        evict.jobs
    );
    assert_eq!(
        finalization_jobs(&evict),
        Vec::new(),
        "there is no boundary to finalize, so no terminal may be requested"
    );
    worker
        .complete_workflow_activation(WorkflowActivationCompletion::empty(run_id.clone()))
        .await
        .unwrap();

    assert_eq!(
        worker.external_stream_run_status(&run_id).await,
        ExternalStreamRunStatus::RunNotFound,
        "the Run really was evicted, so the assertions above are about the state under test"
    );

    worker.shutdown().await;
    worker.finalize_shutdown().await;
}

#[tokio::test]
async fn evicting_a_run_holding_a_workflow_task_finalizes_before_the_eviction_activation() {
    // The sequencing the whole transition rests on. The marker rides the *finalization*
    // completion, never the eviction completion -- an eviction completion reports nothing and may
    // carry no commands, so a marker attached there would be dropped without a trace. Issuing the
    // finalization first is what guarantees it is answered before `RemoveFromCache` goes out.
    let markers: StreamMarkers = Default::default();
    let forced: Arc<Mutex<Vec<bool>>> = Default::default();
    let mut mock_cfg = MockPollCfg::from_resp_batches(
        "fakeid",
        marker_then_replacement_history(1, ParkReason::Shutdown, b"before-eviction-terminal"),
        [1],
        mock_worker_client(),
    );
    let collected = markers.clone();
    let recorder = forced.clone();
    mock_cfg.completion_asserts_from_expectations(|mut asserts| {
        for _ in 0..4 {
            let collected = collected.clone();
            let recorder = recorder.clone();
            asserts.then(move |wft| {
                collected.lock().extend(stream_markers(wft));
                recorder.lock().push(wft.force_create_new_workflow_task);
            });
        }
    });
    let mut mock = build_mock_pollers(mock_cfg);
    mock.worker_cfg(|w| {
        w.task_types = WorkerTaskTypes::workflow_only();
        w.max_cached_workflows = 1;
        // Core's own default, which Python does not override: a pending eviction and its reply
        // count as pending work. The test helper flips it, and with it flipped the Run would be
        // dropped at shutdown before its eviction activation was ever issued -- which is the very
        // ordering under test here.
        w.ignore_evicts_on_shutdown = false;
    });
    let worker = mock_worker(mock);

    let activation = worker.poll_workflow_activation().await.unwrap();
    let run_id = activation.run_id.clone();
    worker
        .complete_workflow_activation(WorkflowActivationCompletion::from_cmds(
            run_id.clone(),
            vec![
                progress_command(b"before-eviction", false),
                quiescent_command(1, &[1], Duration::from_secs(30)),
            ],
        ))
        .await
        .unwrap();

    worker.request_workflow_eviction(&run_id);

    let finalize = worker.poll_workflow_activation().await.unwrap();
    assert_eq!(
        finalization_jobs(&finalize),
        vec![(1, ParkReason::Shutdown, vec![1])],
        "the finalization must be issued before the eviction, got {:?}",
        finalize.jobs
    );
    assert!(
        !finalize.is_only_eviction(),
        "the eviction must not have overtaken the finalization"
    );

    worker
        .complete_workflow_activation(WorkflowActivationCompletion::from_cmd(
            run_id.clone(),
            finalized_command(1, b"-terminal"),
        ))
        .await
        .unwrap();
    assert_eq!(
        *markers.lock(),
        vec![(
            1,
            ParkReason::Shutdown,
            b"before-eviction-terminal".to_vec()
        )],
        "the marker rides the finalization completion"
    );
    assert_eq!(*forced.lock(), vec![true]);

    // Only now does the eviction go out, and it adds nothing to what was already written.
    let evict = worker.poll_workflow_activation().await.unwrap();
    assert!(
        evict.is_only_eviction(),
        "the eviction follows the finalization, got {:?}",
        evict.jobs
    );
    worker
        .complete_workflow_activation(WorkflowActivationCompletion::empty(run_id))
        .await
        .unwrap();
    assert_eq!(
        markers.lock().len(),
        1,
        "one Workflow Task gets one marker, and the eviction completion writes none"
    );
    assert_eq!(*forced.lock(), vec![true]);

    worker.shutdown().await;
    worker.finalize_shutdown().await;
}

/// Runs a Worker that shuts down holding a Workflow Task, returning the marker it wrote.
///
/// The transition itself is asserted by
/// `shutdown_with_a_workflow_task_open_writes_its_marker_and_forces_a_replacement`; this exists so
/// a *second* Worker can be handed the very bytes the first one wrote, rather than a marker a test
/// composed by hand and hoped was the same shape.
async fn a_marker_written_on_the_way_out() -> ExternalStreamMarkerData {
    let recorded: Arc<Mutex<Vec<ExternalStreamMarkerData>>> = Default::default();
    let mut mock_cfg = MockPollCfg::from_resp_batches(
        "fakeid",
        marker_then_replacement_history(1, ParkReason::Shutdown, b"before-shutdown-terminal"),
        [1],
        mock_worker_client(),
    );
    let collected = recorded.clone();
    mock_cfg.completion_asserts_from_expectations(|mut asserts| {
        asserts.then(move |wft| {
            collected.lock().extend(stream_marker_data(wft));
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
                progress_command(b"before-shutdown", false),
                quiescent_command(1, &[1, 2], Duration::from_secs(30)),
            ],
        ))
        .await
        .unwrap();

    worker.initiate_shutdown();
    let finalize = worker.poll_workflow_activation().await.unwrap();
    assert_eq!(
        finalization_jobs(&finalize),
        vec![(1, ParkReason::Shutdown, vec![1, 2])],
        "shutdown must ask lang to finalize before anything is written, got {:?}",
        finalize.jobs
    );
    worker
        .complete_workflow_activation(WorkflowActivationCompletion::from_cmd(
            run_id,
            finalized_command(1, b"-terminal"),
        ))
        .await
        .unwrap();
    worker.shutdown().await;
    worker.finalize_shutdown().await;

    let mut written = recorded.lock().clone();
    assert_eq!(written.len(), 1, "one Workflow Task gets one marker");
    written.pop().unwrap()
}

#[tokio::test]
async fn a_second_worker_reconstructs_the_subscription_from_the_shutdown_marker() {
    // The half of the shutdown row that says what `force_new_wft` was *for*. Handing the Run back
    // to the task queue is only worth doing if whoever picks it up can carry on, and the
    // replacement Worker starts with no wait set, no cursor, and no watcher: everything it resumes
    // from has to come out of the marker the first Worker wrote.
    //
    // The marker is the one the first Worker actually produced, bytes and wait list alike -- a
    // hand-composed stand-in would prove only that the reader agrees with the test.
    let recorded = a_marker_written_on_the_way_out().await;
    assert_eq!(recorded.terminal_boundary(), ParkReason::Shutdown);
    assert_eq!(
        recorded.replay_annotation, b"before-shutdown-terminal",
        "the marker under test must be the finalized one, or the replacement has nothing to \
         reconstruct from"
    );
    let waits: Vec<(u32, u64)> = recorded
        .waits
        .iter()
        .map(|w| (w.wait_id, w.generation))
        .collect();
    assert_eq!(
        waits,
        vec![(1, 0), (2, 0)],
        "the marker must name the subscriptions it closed"
    );

    // The replacement task, as a second Worker sees it: the first Workflow Task is history now,
    // and the marker sits inside it.
    let mut t = TestHistoryBuilder::default();
    t.add_by_type(EventType::WorkflowExecutionStarted);
    t.add_full_wf_task();
    t.add_external_stream_marker_covering(
        recorded.quiescence_generation,
        recorded.terminal_boundary(),
        &recorded.replay_annotation,
        &waits,
    );
    t.add_workflow_task_scheduled_and_started();

    let markers: StreamMarkers = Default::default();
    let worker = worker_recording_markers(markers.clone(), vec![2], t);

    let replayed = worker.poll_workflow_activation().await.unwrap();
    let run_id = replayed.run_id.clone();
    assert!(
        replayed.is_replaying,
        "the replacement Worker reaches the marker by replaying the task that wrote it, got \
         {replayed:?}"
    );
    assert_eq!(
        replay_jobs(&replayed),
        vec![(
            recorded.quiescence_generation,
            ParkReason::Shutdown,
            waits,
            recorded.replay_annotation.clone()
        )],
        "the second Worker must be handed the recorded snapshot, waits, terminal and annotation \
         unchanged, got {:?}",
        replayed.jobs
    );
    // And no readiness path is entered on the way: the replacement reconstructs from the marker
    // rather than by consulting a backend it has never spoken to.
    assert_eq!(resolve_hints(&replayed), Vec::<u32>::new());
    assert_eq!(park_jobs(&replayed), Vec::new());

    worker
        .complete_workflow_activation(WorkflowActivationCompletion::empty(run_id))
        .await
        .unwrap();
    assert_eq!(
        *markers.lock(),
        Vec::new(),
        "the replacement reads the marker; it must not write a second copy of it"
    );

    worker.shutdown().await;
    worker.finalize_shutdown().await;
}

/// A workflow-only worker recording every marker, which hands out its eviction activations.
///
/// `ignore_evicts_on_shutdown = false` is Core's own default, which Python does not override. The
/// test helper flips it, and with it flipped a Run is dropped the moment the mock runs out of work
/// -- before the eviction activation any test about eviction is waiting for.
fn worker_recording_markers_through_eviction(
    markers: StreamMarkers,
    batches: Vec<usize>,
    history: TestHistoryBuilder,
) -> crate::Worker {
    let mut mock_cfg =
        MockPollCfg::from_resp_batches("fakeid", history, batches, mock_worker_client());
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
        w.ignore_evicts_on_shutdown = false;
    });
    mock_worker(mock)
}

#[tokio::test]
async fn an_unwritten_annotation_exists_only_while_a_workflow_task_is_open() {
    // The invariant the whole eviction split rests on, asserted in both states an eviction can
    // find a Run in. It is what makes "no open Workflow Task, so no marker" safe rather than
    // lossy: with a task open an unwritten annotation may exist and eviction must finalize it,
    // and without one there is provably nothing to finalize -- not because nothing was consumed,
    // but because the completion that ended the task took the bytes with it.
    //
    // --- state one: a Workflow Task is open ---
    let held: StreamMarkers = Default::default();
    let worker = worker_recording_markers_through_eviction(
        held.clone(),
        vec![1],
        marker_then_replacement_history(1, ParkReason::Shutdown, b"held-terminal"),
    );

    let activation = worker.poll_workflow_activation().await.unwrap();
    let run_id = activation.run_id.clone();
    worker
        .complete_workflow_activation(WorkflowActivationCompletion::from_cmds(
            run_id.clone(),
            vec![
                progress_command(b"held", false),
                quiescent_command(1, &[1], Duration::from_secs(30)),
            ],
        ))
        .await
        .unwrap();
    assert_eq!(
        worker.external_stream_run_status(&run_id).await,
        ExternalStreamRunStatus::WftOpen
    );
    assert_eq!(
        worker.external_stream_annotation(&run_id).await,
        b"held",
        "an unwritten annotation may exist here, and this is the only state in which it may"
    );

    worker.request_workflow_eviction(&run_id);
    let finalize = worker.poll_workflow_activation().await.unwrap();
    assert_eq!(
        finalization_jobs(&finalize),
        vec![(1, ParkReason::Shutdown, vec![1])],
        "an eviction finding an open task must close its boundary, got {:?}",
        finalize.jobs
    );
    worker
        .complete_workflow_activation(WorkflowActivationCompletion::from_cmd(
            run_id.clone(),
            finalized_command(1, b"-terminal"),
        ))
        .await
        .unwrap();
    assert_eq!(
        *held.lock(),
        vec![(1, ParkReason::Shutdown, b"held-terminal".to_vec())],
        "the held bytes leave as a marker rather than with the Run"
    );
    assert!(
        worker.external_stream_annotation(&run_id).await.is_empty(),
        "and the annotation is cleared by the write, so nothing is held into the eviction"
    );

    let evict = worker.poll_workflow_activation().await.unwrap();
    assert!(evict.is_only_eviction(), "got {:?}", evict.jobs);
    worker
        .complete_workflow_activation(WorkflowActivationCompletion::empty(run_id.clone()))
        .await
        .unwrap();
    assert_eq!(
        held.lock().len(),
        1,
        "one boundary gets one marker: the eviction completion writes none of its own"
    );
    worker.shutdown().await;
    worker.finalize_shutdown().await;

    // --- state two: no Workflow Task is open ---
    // A Run that consumed records on an earlier task and is now sitting between tasks. The
    // distinction that matters is that this Run has *not* done nothing: an empty annotation here
    // is a fact about the completion path, not about there having been nothing to write.
    let reported: StreamMarkers = Default::default();
    let worker = worker_recording_markers_through_eviction(
        reported.clone(),
        vec![1, 2],
        marker_then_timer_history(0, ParkReason::CommandsProduced, b"consumed"),
    );

    let activation = worker.poll_workflow_activation().await.unwrap();
    let run_id = activation.run_id.clone();
    worker
        .complete_workflow_activation(WorkflowActivationCompletion::from_cmds(
            run_id.clone(),
            vec![
                progress_command(b"consumed", false),
                start_timer_cmd(1, Duration::from_secs(10)),
            ],
        ))
        .await
        .unwrap();

    // The next task registers the subscription again and is held open, which is what keeps the Run
    // observable while its wait set is put into the state under test.
    let fired = worker.poll_workflow_activation().await.unwrap();
    assert_eq!(fired.run_id, run_id);
    worker
        .complete_workflow_activation(WorkflowActivationCompletion::from_cmd(
            run_id.clone(),
            quiescent_command(1, &[1], Duration::from_secs(30)),
        ))
        .await
        .unwrap();
    assert_eq!(
        *reported.lock(),
        vec![(0, ParkReason::CommandsProduced, b"consumed".to_vec())],
        "everything this Run consumed left with the task that consumed it"
    );

    // Pinned rather than waited for: seeding clears the open flag without touching the annotation,
    // so a completion path that had failed to write its bytes would still show them here.
    worker
        .seed_external_stream_waits(&run_id, vec![1], None, false)
        .await;
    assert_eq!(
        worker.external_stream_run_status(&run_id).await,
        ExternalStreamRunStatus::NoOpenWorkflowTask
    );
    assert!(
        worker.external_stream_annotation(&run_id).await.is_empty(),
        "with no Workflow Task open there is nothing unwritten, which is what makes writing \
         nothing safe"
    );

    worker.request_workflow_eviction(&run_id);
    let evict = worker.poll_workflow_activation().await.unwrap();
    assert!(
        evict.is_only_eviction(),
        "with nothing to finalize the eviction goes out directly, got {:?}",
        evict.jobs
    );
    assert_eq!(
        finalization_jobs(&evict),
        Vec::new(),
        "there is no boundary to finalize, so no terminal may be requested"
    );
    worker
        .complete_workflow_activation(WorkflowActivationCompletion::empty(run_id.clone()))
        .await
        .unwrap();
    assert_eq!(
        reported.lock().len(),
        1,
        "the eviction wrote no marker, and none was missing"
    );
    assert_eq!(
        worker.external_stream_run_status(&run_id).await,
        ExternalStreamRunStatus::RunNotFound,
        "the Run really was evicted, so the assertions above are about the state under test"
    );

    worker.shutdown().await;
    worker.finalize_shutdown().await;
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
fn stream_markers(wft: &WorkflowTaskCompletion) -> Vec<StreamMarker> {
    stream_marker_data(wft)
        .into_iter()
        .map(|d| {
            (
                d.quiescence_generation,
                d.terminal_boundary(),
                d.replay_annotation,
            )
        })
        .collect()
}

/// The whole marker envelope a completion recorded, wait list included.
///
/// [`stream_markers`] drops the waits because most assertions do not need them. A test that hands
/// a marker one Worker wrote to a *second* Worker does: the wait list is what the replacement
/// reconstructs the subscription from.
fn stream_marker_data(wft: &WorkflowTaskCompletion) -> Vec<ExternalStreamMarkerData> {
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
        .collect()
}

/// A worker whose second Workflow Task carries one Signal.
///
/// The Workflow Task timeout is long on purpose: a wake that fails validation leaves the seeded
/// wait set retaining the replacement task, which re-arms the idle deadline. With the default
/// five-second timeout that deadline expires inside the test and parks the set, which is correct
/// behaviour but not what these tests are about.
fn worker_with_a_signal(signal_name: &str, payloads: Vec<Payload>) -> crate::Worker {
    let mut t = TestHistoryBuilder::default();
    t.add_wfe_started_with_wft_timeout(Duration::from_secs(300));
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

/// Drives a run whose wait set is still retaining its Workflow Task to completion.
///
/// A wake that failed validation leaves the set blocked and the task retained -- correctly, since
/// nothing resolved it. Left that way the run would hold its idle deadline until the Worker shut
/// down, so the test resolves the set for real and lets the Workflow finish.
async fn release_a_retained_run(worker: &crate::Worker, run_id: &str) {
    if worker.external_stream_run_status(run_id).await != ExternalStreamRunStatus::WftOpen {
        return;
    }
    assert_eq!(
        worker.notify_external_stream_ready(run_id, 1, 0).await,
        ExternalStreamReadyResult::Accepted
    );
    let resolved = worker.poll_workflow_activation().await.unwrap();
    worker
        .complete_workflow_activation(WorkflowActivationCompletion::from_cmds(
            resolved.run_id,
            vec![CompleteWorkflowExecution::default().into()],
        ))
        .await
        .unwrap();
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
async fn an_intercepted_wake_does_not_flag_its_activation_replaying() {
    // The wake Signal is suppressed from user handlers, so the Workflow Task it arrives on
    // carries no job of its own and the resolve job is Core's alone. `is_replaying` is derived
    // from the activation's job list -- "every job is a query" -- and that test is vacuously true
    // over an empty list, so an activation whose only job is queued after it is built comes out
    // marked as replay. Lang emits neither stream progress nor quiescence while replaying, so the
    // wait generation would never advance, every later readiness report would be answered `Stale`
    // against a watcher cursor that had already moved past those records, and the Workflow would
    // stall with data sitting in its buffer. This asserts the flag over the same history an
    // ordinary Signal is not-replaying on, so it catches the flag and not the history.
    let worker = worker_with_a_signal(
        external_stream::WAKE_SIGNAL_NAME,
        vec![wake_payload(wake(0, ""))],
    );
    advance_to_the_signal_task(&worker, vec![1], None).await;

    let second = worker.poll_workflow_activation().await.unwrap();

    assert_eq!(
        resolve_hints(&second),
        vec![1],
        "the wake must produce the resolve job this test is about, got {:?}",
        second.jobs
    );
    assert!(
        !second.is_replaying,
        "an activation carrying a resolve job produced by an intercepted wake Signal is live \
         work, not replay, got jobs {:?}",
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

#[tokio::test]
async fn an_ordinary_signal_on_the_same_history_is_not_replaying_either() {
    // The control for the test above: identical history, an ordinary Signal in place of the
    // reserved one. If this ever starts failing, the replay flag on the wake path is not the
    // thing at fault -- the history these tests are built on stopped being live work.
    let worker = worker_with_a_signal("a-user-signal", vec![]);
    let first = worker.poll_workflow_activation().await.unwrap();
    worker
        .complete_workflow_activation(WorkflowActivationCompletion::empty(first.run_id))
        .await
        .unwrap();

    let second = worker.poll_workflow_activation().await.unwrap();
    assert!(
        !second.is_replaying,
        "the second Workflow Task of this history is live work, got jobs {:?}",
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
    release_a_retained_run(&worker, &run_id).await;
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
    release_a_retained_run(&worker, &run_id).await;
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
    release_a_retained_run(&worker, &run_id).await;
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
    markers: StreamMarkers,
    batches: Vec<usize>,
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
    let markers: StreamMarkers = Default::default();
    let worker = worker_recording_markers(
        markers.clone(),
        vec![1, 2],
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
                progress_command(b"one", false),
                quiescent_command(1, &[1], Duration::from_secs(30)),
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
                    progress_command(delta, false),
                    quiescent_command(1, &[1], Duration::from_secs(30)),
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
                progress_command(b"four", false),
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
    let markers: StreamMarkers = Default::default();
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
                progress_command(b"consumed", false),
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
async fn a_normal_completion_commits_its_marker() {
    // The table row with nothing else on it: records were consumed, no stream wait remained
    // pending, and the completion carries a `WorkflowStreamProgress` and *nothing more*. Nothing
    // else on it could have triggered the marker, which is the point -- committing consumption is
    // not a side effect of some other command riding along. Its terminal says `TaskCompleted`
    // rather than `CommandsProduced`, so the marker does not imply commands that never existed.
    let markers: StreamMarkers = Default::default();
    // Only the quiescence generation is reconciled against the recorded marker, so what this
    // history's own annotation says does not matter; what is read below is what the *completion*
    // carried. The Signal is there to give the second task an activation, which is what keeps the
    // run cached long enough to be asked what it is still holding.
    let worker =
        worker_recording_markers(markers.clone(), vec![1, 2], marker_then_signal_history(0));

    let activation = worker.poll_workflow_activation().await.unwrap();
    let run_id = activation.run_id.clone();

    worker
        .complete_workflow_activation(WorkflowActivationCompletion::from_cmd(
            run_id.clone(),
            progress_command(b"consumed", false),
        ))
        .await
        .unwrap();

    let next = worker.poll_workflow_activation().await.unwrap();
    assert_eq!(next.run_id, run_id);
    assert_eq!(
        *markers.lock(),
        vec![(0, ParkReason::TaskCompleted, b"consumed".to_vec())],
        "a completion carrying only progress must still commit its marker"
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

/// Completes one activation with a consumed record followed by `command`.
///
/// Returns the markers the completion reported and the command types it reported, in order --
/// everything an ordering assertion needs and nothing else.
async fn markers_and_command_order(
    command: workflow_command::Variant,
) -> (Vec<StreamMarker>, Vec<i32>) {
    let markers: StreamMarkers = Default::default();
    let ordering: Arc<Mutex<Vec<i32>>> = Default::default();

    let t = canned_histories::single_timer("1");
    let mut mock_cfg = MockPollCfg::from_resp_batches("fakeid", t, [1], mock_worker_client());
    let collected = markers.clone();
    let recorded = ordering.clone();
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
            vec![progress_command(b"consumed", false), command],
        ))
        .await
        .unwrap();
    worker.drain_pollers_and_shutdown().await;

    let m = markers.lock().clone();
    let o = ordering.lock().clone();
    (m, o)
}

/// Asserts the completion's marker was reported ahead of `dependent`.
fn assert_the_marker_came_first(commands: &[i32], dependent: CommandType) {
    let marker_at = commands
        .iter()
        .position(|c| *c == CommandType::RecordMarker as i32)
        .expect("the marker must be in the completion");
    let dependent_at = commands
        .iter()
        .position(|c| *c == dependent as i32)
        .unwrap_or_else(|| panic!("{dependent:?} must be in the completion, got {commands:?}"));
    assert!(
        marker_at < dependent_at,
        "the marker must precede {dependent:?}, got {commands:?}"
    );
}

#[tokio::test]
async fn an_activity_command_writes_its_marker_ordered_before_it() {
    // The same normative ordering as the terminal case, on the command that leaves the Workflow
    // running. Reversed, replay would match the Activity against history before the record it was
    // derived from had been validated, so a damaged stream would be discovered only after its
    // consequences were already durable.
    let (markers, commands) = markers_and_command_order(schedule_activity_cmd(
        1,
        "q",
        "act-1",
        ActivityCancellationType::TryCancel,
        Duration::from_secs(10),
        Duration::from_secs(10),
    ))
    .await;

    assert_eq!(
        markers,
        vec![(0, ParkReason::CommandsProduced, b"consumed".to_vec())],
        "an Activity ends the Workflow Task, so the task's consumption is committed with it"
    );
    assert_the_marker_came_first(&commands, CommandType::ScheduleActivityTask);
}

#[tokio::test]
async fn a_failed_workflow_writes_its_marker_ordered_before_the_failure() {
    // `CompleteWorkflowExecution` is not the only terminal command, and the ordering rule is
    // stated about all four. A path that ordered the marker only ahead of a *successful*
    // completion would leave a failing Workflow's last consumption uncommitted -- and a failure is
    // exactly when a Workflow is most likely to be retried and replayed.
    let (markers, commands) = markers_and_command_order(
        FailWorkflowExecution {
            failure: Some(
                temporalio_common::protos::temporal::api::failure::v1::Failure {
                    message: "deliberate".to_string(),
                    ..Default::default()
                },
            ),
        }
        .into(),
    )
    .await;

    assert_eq!(
        markers,
        vec![(0, ParkReason::WorkflowCompleted, b"consumed".to_vec())],
        "a failing Workflow commits its consumption exactly as a completing one does"
    );
    assert_the_marker_came_first(&commands, CommandType::FailWorkflowExecution);
}

#[tokio::test]
async fn a_continue_as_new_writes_its_marker_ordered_before_it() {
    // The terminal command that matters most here: the next Run resumes its subscriptions from the
    // committed continuation state, so a marker written *after* the Continue-As-New -- or not at
    // all -- would hand the successor a cursor that predates records this Run already delivered.
    let (markers, commands) = markers_and_command_order(
        ContinueAsNewWorkflowExecution {
            workflow_type: "successor".to_string(),
            ..Default::default()
        }
        .into(),
    )
    .await;

    assert_eq!(
        markers,
        vec![(0, ParkReason::WorkflowCompleted, b"consumed".to_vec())],
        "continue-as-new is a terminal command like any other, and commits its marker like one"
    );
    assert_the_marker_came_first(&commands, CommandType::ContinueAsNewWorkflowExecution);
}

#[tokio::test]
async fn replaying_a_marker_before_an_activity_delivers_its_record_exactly_once() {
    // The replay half of the Activity case. The marker written ahead of the Activity is read back
    // ahead of it, and *once*: a lookahead that re-issued the recorded observations on the second
    // Workflow Task -- the one the Activity's result arrives on -- would deliver every record
    // twice while the command they produced is already durable.
    //
    // Driven through `init_replay_worker`, which is the path a whole-history replay actually
    // takes.
    let mut t = TestHistoryBuilder::default();
    t.add_by_type(EventType::WorkflowExecutionStarted);
    t.add_full_wf_task();
    t.add_external_stream_marker_covering(
        1,
        ParkReason::CommandsProduced,
        b"header.segment.terminal",
        &[(1, 0)],
    );
    let scheduled = t.add_activity_task_scheduled("act-1");
    let started = t.add_activity_task_started(scheduled);
    t.add_activity_task_completed(scheduled, started, b"result".into());
    t.add_full_wf_task();
    t.add_workflow_execution_completed();

    let worker = crate::init_replay_worker(crate::replay::ReplayWorkerInput::new(
        crate::test_help::test_worker_cfg().build().unwrap(),
        futures_util::stream::iter([crate::replay::HistoryForReplay::from(t)]),
    ))
    .unwrap();

    let replayed = worker.poll_workflow_activation().await.unwrap();
    assert!(replayed.is_replaying);
    assert_eq!(
        replay_jobs(&replayed),
        vec![(
            1,
            ParkReason::CommandsProduced,
            vec![(1, 0)],
            b"header.segment.terminal".to_vec()
        )],
        "the recorded observations must reach lang on the task that consumed them, got {:?}",
        replayed.jobs
    );
    worker
        .complete_workflow_activation(WorkflowActivationCompletion::from_cmds(
            replayed.run_id.clone(),
            vec![schedule_activity_cmd(
                1,
                "q",
                "act-1",
                ActivityCancellationType::TryCancel,
                Duration::from_secs(10),
                Duration::from_secs(10),
            )],
        ))
        .await
        .unwrap();

    // The Activity resolves on the next Workflow Task, and that task replays no records: they
    // belong to the marker of the task before it, which has already been delivered.
    let resolved = worker.poll_workflow_activation().await.unwrap();
    assert!(
        resolved.jobs.iter().any(|j| matches!(
            j.variant,
            Some(workflow_activation_job::Variant::ResolveActivity(_))
        )),
        "the Activity that followed the marker must still match its event, got {:?}",
        resolved.jobs
    );
    assert_eq!(
        replay_jobs(&resolved),
        Vec::new(),
        "the record was delivered on the task that consumed it, and must not be delivered again \
         on the task its Activity resolves on, got {:?}",
        resolved.jobs
    );
    worker
        .complete_workflow_activation(WorkflowActivationCompletion::from_cmds(
            resolved.run_id.clone(),
            vec![CompleteWorkflowExecution::default().into()],
        ))
        .await
        .unwrap();

    // The replay worker ends its stream only when the history ran to completion without a
    // nondeterminism failure, so this is what makes the whole replay the thing under test.
    assert!(
        matches!(
            worker.poll_workflow_activation().await,
            Err(PollError::ShutDown)
        ),
        "replay must run the history to its end"
    );
}

#[tokio::test]
async fn a_workflow_task_that_observed_nothing_writes_no_marker() {
    // Not an error: a Workflow Task that touched no stream is the ordinary case, and writing an
    // empty marker for it would put a History event on every task in the Workflow.
    let markers: StreamMarkers = Default::default();
    let worker = worker_recording_markers(
        markers.clone(),
        vec![1, 2],
        canned_histories::single_timer("1"),
    );

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
    let markers: StreamMarkers = Default::default();
    let worker = worker_recording_markers(
        markers.clone(),
        vec![1, 2],
        canned_histories::single_timer("1"),
    );

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

// --- the complete-set park handshake (C8) ------------------------------------

/// The `PrepareExternalStreamPark` jobs in an activation, as (generation, reason, wait ids).
///
/// Reading the reason back out is what separates the two triggers: an idle expiry and an
/// all-fenced snapshot produce the same shape of job and the same shape of marker, and only this
/// field says which of them actually happened.
fn park_jobs(activation: &WorkflowActivation) -> Vec<(u64, ParkReason, Vec<u32>)> {
    activation
        .jobs
        .iter()
        .filter_map(|j| match &j.variant {
            Some(workflow_activation_job::Variant::PrepareExternalStreamPark(p)) => Some((
                p.quiescence_generation,
                p.reason(),
                p.waits.iter().map(|w| w.wait_id).collect(),
            )),
            _ => None,
        })
        .collect()
}

/// Lang's `ParkSetConfirmed`: every stream was still empty after the intents went in.
fn park_confirmed_command(
    quiescence_generation: u64,
    terminal: &[u8],
) -> workflow_command::Variant {
    workflow_command::Variant::ExternalStreamParkResult(ExternalStreamParkResult {
        quiescence_generation,
        outcome: Some(external_stream_park_result::Outcome::Confirmed(
            ParkSetConfirmed {},
        )),
        final_observation_delta: terminal.to_vec(),
    })
}

/// Lang's `StreamSetBecameReady`: the final recheck found records, so this generation is abandoned.
fn park_became_ready_command(quiescence_generation: u64) -> workflow_command::Variant {
    workflow_command::Variant::ExternalStreamParkResult(ExternalStreamParkResult {
        quiescence_generation,
        outcome: Some(external_stream_park_result::Outcome::BecameReady(
            StreamSetBecameReady {},
        )),
        // Deliberately empty: an abandoned park reached no boundary, so there is no terminal to
        // carry and nothing for Core to write.
        final_observation_delta: vec![],
    })
}

/// A quiescent snapshot whose waits carry individual write-fence states.
fn fenced_quiescent_command(
    quiescence_generation: u64,
    waits: &[(u32, bool)],
    idle_timeout: Duration,
) -> workflow_command::Variant {
    workflow_command::Variant::WorkflowStreamQuiescent(WorkflowStreamQuiescent {
        quiescence_generation,
        waits: waits
            .iter()
            .map(|(wait_id, immediately_parkable)| ExternalStreamWait {
                wait_id: *wait_id,
                generation: 0,
                immediately_parkable: *immediately_parkable,
            })
            .collect(),
        idle_timeout: Some(idle_timeout.try_into().unwrap()),
    })
}

/// Reports `annotation` and the given quiescent snapshot, leaving the task retained.
///
/// This is the state every park begins from: lang has reported what it observed and asked to be
/// held open, Core is holding the task, and no activation is outstanding.
async fn retain_a_quiescent_task(
    worker: &crate::Worker,
    annotation: &[u8],
    quiescence: workflow_command::Variant,
) -> String {
    let activation = worker.poll_workflow_activation().await.unwrap();
    let run_id = activation.run_id.clone();
    worker
        .complete_workflow_activation(WorkflowActivationCompletion::from_cmds(
            run_id.clone(),
            vec![progress_command(annotation, false), quiescence],
        ))
        .await
        .unwrap();
    assert_eq!(
        worker.external_stream_run_status(&run_id).await,
        ExternalStreamRunStatus::WftOpen,
        "the quiescent snapshot must have been accepted, or this test proves nothing"
    );
    run_id
}

/// A history whose first Workflow Task records a marker and whose second carries a Signal.
///
/// The marker event has to be present and first, because commands are matched to events in order.
/// The Signal is there only to give the replacement task an activation -- a run with nothing left
/// to do is evicted, and a test cannot ask an evicted run whether it parked.
fn marker_then_signal_history(quiescence_generation: u64) -> TestHistoryBuilder {
    let mut t = TestHistoryBuilder::default();
    // Long enough that no rollover deadline or clamped idle deadline can fire inside the test.
    t.add_wfe_started_with_wft_timeout(Duration::from_secs(300));
    t.add_full_wf_task();
    t.add_external_stream_marker(quiescence_generation, ParkReason::Idle, b"recorded");
    t.add_we_signaled("keep-the-run-cached", vec![]);
    t.add_workflow_task_scheduled_and_started();
    t
}

/// A workflow-only worker recording every completion's markers and `force_new_wft`.
fn worker_for_a_park(
    markers: StreamMarkers,
    forced: Arc<Mutex<Vec<bool>>>,
    history: TestHistoryBuilder,
    batches: Vec<usize>,
) -> crate::Worker {
    worker_recording_rollovers(markers, forced, history, batches)
}

#[tokio::test]
async fn a_confirmed_idle_park_writes_one_marker_and_completes_the_task() {
    // The confirmation is the only place Core can obtain this boundary's terminal: park issues no
    // finalization job, so if the result's `final_observation_delta` were dropped the marker would
    // be a truncated annotation -- durable and wrong.
    let markers: StreamMarkers = Default::default();
    let forced: Arc<Mutex<Vec<bool>>> = Default::default();
    let worker = worker_for_a_park(
        markers.clone(),
        forced.clone(),
        marker_then_signal_history(1),
        vec![1, 2],
    );

    let run_id = retain_a_quiescent_task(
        &worker,
        b"observed",
        quiescent_command(1, &[1, 2], Duration::from_millis(30)),
    )
    .await;

    // The idle timer expired and Core asked for the complete set, not just the wait that went
    // quiet: parking is all-or-nothing.
    let park = worker.poll_workflow_activation().await.unwrap();
    assert_eq!(
        park_jobs(&park),
        vec![(1, ParkReason::Idle, vec![1, 2])],
        "an idle expiry must ask lang to park the complete set, got {:?}",
        park.jobs
    );
    assert_eq!(
        *markers.lock(),
        Vec::new(),
        "nothing may be written before the terminal exists"
    );

    worker
        .complete_workflow_activation(WorkflowActivationCompletion::from_cmd(
            run_id.clone(),
            park_confirmed_command(1, b"-parked"),
        ))
        .await
        .unwrap();

    assert_eq!(
        *markers.lock(),
        vec![(1, ParkReason::Idle, b"observed-parked".to_vec())],
        "the park's marker carries the accumulated annotation with the confirmation's terminal"
    );
    assert_eq!(
        *forced.lock(),
        vec![false],
        "parking is the opposite of asking for a replacement task"
    );

    // The set is parked: readiness can no longer be delivered locally, and a producer now has a
    // generation to name in its wake Signal.
    assert_eq!(
        worker.external_stream_run_status(&run_id).await,
        ExternalStreamRunStatus::Parked
    );
    assert_eq!(
        worker.notify_external_stream_ready(&run_id, 1, 0).await,
        ExternalStreamReadyResult::Parked
    );

    let replacement = worker.poll_workflow_activation().await.unwrap();
    worker
        .complete_workflow_activation(WorkflowActivationCompletion::from_cmds(
            replacement.run_id,
            vec![CompleteWorkflowExecution::default().into()],
        ))
        .await
        .unwrap();
    worker.drain_pollers_and_shutdown().await;
}

#[tokio::test]
async fn an_all_fenced_snapshot_parks_without_waiting_out_the_idle_timeout() {
    // The fence says no later record is coming, so waiting out the idle delay could only ever end
    // the same way. The idle timeout here is five minutes: if the park were coming from the timer
    // rather than from the fences, this test would hang rather than fail.
    let markers: StreamMarkers = Default::default();
    let forced: Arc<Mutex<Vec<bool>>> = Default::default();
    let worker = worker_for_a_park(
        markers.clone(),
        forced.clone(),
        marker_then_signal_history(2),
        vec![1, 2],
    );

    // One fenced stream out of two does not qualify. The set is retained and nothing is asked.
    let run_id = retain_a_quiescent_task(
        &worker,
        b"half",
        fenced_quiescent_command(1, &[(1, true), (2, false)], Duration::from_secs(300)),
    )
    .await;
    assert!(
        tokio::time::timeout(
            Duration::from_millis(300),
            worker.poll_workflow_activation()
        )
        .await
        .is_err(),
        "one fenced stream must not park a set another stream is still driving"
    );

    // Now the second stream fences too, and the set parks immediately.
    worker.notify_external_stream_ready(&run_id, 2, 0).await;
    let resolved = worker.poll_workflow_activation().await.unwrap();
    assert_eq!(resolve_hints(&resolved), vec![2]);
    worker
        .complete_workflow_activation(WorkflowActivationCompletion::from_cmds(
            run_id.clone(),
            vec![
                progress_command(b"-rest", false),
                fenced_quiescent_command(2, &[(1, true), (2, true)], Duration::from_secs(300)),
            ],
        ))
        .await
        .unwrap();

    let park = worker.poll_workflow_activation().await.unwrap();
    assert_eq!(
        park_jobs(&park),
        vec![(2, ParkReason::AllWriteFenced, vec![1, 2])],
        "an all-fenced snapshot must park on its own, got {:?}",
        park.jobs
    );

    worker
        .complete_workflow_activation(WorkflowActivationCompletion::from_cmd(
            run_id.clone(),
            park_confirmed_command(2, b"-fenced"),
        ))
        .await
        .unwrap();

    assert_eq!(
        *markers.lock(),
        vec![(2, ParkReason::AllWriteFenced, b"half-rest-fenced".to_vec())],
        "the marker must say which trigger actually parked the set"
    );
    assert_eq!(
        worker.external_stream_run_status(&run_id).await,
        ExternalStreamRunStatus::Parked
    );

    let replacement = worker.poll_workflow_activation().await.unwrap();
    worker
        .complete_workflow_activation(WorkflowActivationCompletion::from_cmds(
            replacement.run_id,
            vec![CompleteWorkflowExecution::default().into()],
        ))
        .await
        .unwrap();
    worker.drain_pollers_and_shutdown().await;
}

#[tokio::test]
async fn readiness_accepted_before_the_confirmation_wins_and_no_marker_is_written() {
    // The first ordering of the race. A record arrived while the handshake was in flight, so the
    // confirmation that follows is closing a boundary that was never reached. Writing its marker
    // would commit a cursor past a record the Workflow has not seen.
    let markers: StreamMarkers = Default::default();
    let forced: Arc<Mutex<Vec<bool>>> = Default::default();
    let worker = worker_for_a_park(
        markers.clone(),
        forced.clone(),
        canned_histories::single_timer("1"),
        vec![1],
    );

    let run_id = retain_a_quiescent_task(
        &worker,
        b"observed",
        quiescent_command(1, &[1], Duration::from_millis(30)),
    )
    .await;
    let park = worker.poll_workflow_activation().await.unwrap();
    assert_eq!(park_jobs(&park).len(), 1);

    // Readiness lands while lang is still installing its intents.
    assert_eq!(
        worker.notify_external_stream_ready(&run_id, 1, 0).await,
        ExternalStreamReadyResult::Accepted,
        "readiness during the handshake must still be accepted -- accepting it is the abort"
    );

    // Lang's recheck saw nothing and confirms anyway. Core knows better.
    worker
        .complete_workflow_activation(WorkflowActivationCompletion::from_cmd(
            run_id.clone(),
            park_confirmed_command(1, b"-parked"),
        ))
        .await
        .unwrap();

    assert_eq!(
        *markers.lock(),
        Vec::new(),
        "a confirmation readiness already beat must write no marker"
    );
    assert_ne!(
        worker.external_stream_run_status(&run_id).await,
        ExternalStreamRunStatus::Parked,
        "the set must not be parked with a record buffered for it"
    );
    assert_eq!(
        worker.external_stream_run_status(&run_id).await,
        ExternalStreamRunStatus::WftOpen,
        "the task stays open so the resolve Core owes lang has somewhere to arrive"
    );
    // And the annotation is still held, unwritten, for whatever boundary does eventually close it.
    assert_eq!(
        worker.external_stream_annotation(&run_id).await,
        b"observed"
    );

    let resolved = worker.poll_workflow_activation().await.unwrap();
    assert_eq!(resolve_hints(&resolved), vec![1]);

    worker
        .complete_workflow_activation(WorkflowActivationCompletion::from_cmds(
            run_id,
            vec![start_timer_cmd(1, Duration::from_secs(10))],
        ))
        .await
        .unwrap();
    worker.drain_pollers_and_shutdown().await;
}

#[tokio::test]
async fn a_recheck_that_became_ready_resolves_the_waits_rather_than_running_user_code() {
    // The other way the handshake can end. `PrepareExternalStreamPark` runs no user Workflow
    // code, so a recheck that found records cannot deliver them from inside the park path: it
    // abandons the generation and Core issues an ordinary resolve, which is the job that does run
    // user code.
    let markers: StreamMarkers = Default::default();
    let forced: Arc<Mutex<Vec<bool>>> = Default::default();
    let worker = worker_for_a_park(
        markers.clone(),
        forced.clone(),
        canned_histories::single_timer("1"),
        vec![1],
    );

    let run_id = retain_a_quiescent_task(
        &worker,
        b"observed",
        quiescent_command(1, &[1, 2], Duration::from_millis(30)),
    )
    .await;
    let park = worker.poll_workflow_activation().await.unwrap();
    assert_eq!(park_jobs(&park).len(), 1);

    worker
        .complete_workflow_activation(WorkflowActivationCompletion::from_cmd(
            run_id.clone(),
            park_became_ready_command(1),
        ))
        .await
        .unwrap();

    assert_eq!(
        *markers.lock(),
        Vec::new(),
        "an aborted park reached no boundary, so it writes no marker"
    );
    assert_eq!(
        worker.external_stream_run_status(&run_id).await,
        ExternalStreamRunStatus::WftOpen
    );

    // A normal resolve activation naming *every* wait: the recheck covered all of them, so any of
    // them may now have data.
    let resolved = worker.poll_workflow_activation().await.unwrap();
    assert_eq!(resolve_hints(&resolved), vec![1, 2]);
    assert_eq!(
        park_jobs(&resolved),
        Vec::new(),
        "the abort must not start a second handshake"
    );

    // The abort bumped every wait generation, which is what makes a readiness notification issued
    // against the abandoned block stale rather than resolving against the new one.
    assert_eq!(
        worker.notify_external_stream_ready(&run_id, 1, 0).await,
        ExternalStreamReadyResult::Stale
    );
    assert_eq!(
        worker.notify_external_stream_ready(&run_id, 1, 1).await,
        ExternalStreamReadyResult::Accepted
    );

    worker
        .complete_workflow_activation(WorkflowActivationCompletion::from_cmds(
            run_id.clone(),
            vec![start_timer_cmd(1, Duration::from_secs(10))],
        ))
        .await
        .unwrap();
    worker.drain_pollers_and_shutdown().await;
}

#[tokio::test]
async fn a_confirmation_naming_a_generation_that_is_not_parking_is_discarded() {
    // A stale confirmation is neither a park nor an abort. Without the generation check it would
    // park a set whose snapshot has moved on, stranding whatever the current one is holding.
    let markers: StreamMarkers = Default::default();
    let forced: Arc<Mutex<Vec<bool>>> = Default::default();
    let worker = worker_for_a_park(
        markers.clone(),
        forced.clone(),
        canned_histories::single_timer("1"),
        vec![1],
    );

    let run_id = retain_a_quiescent_task(
        &worker,
        b"observed",
        quiescent_command(1, &[1], Duration::from_millis(30)),
    )
    .await;
    let park = worker.poll_workflow_activation().await.unwrap();
    assert_eq!(park_jobs(&park), vec![(1, ParkReason::Idle, vec![1])]);

    worker
        .complete_workflow_activation(WorkflowActivationCompletion::from_cmd(
            run_id.clone(),
            park_confirmed_command(99, b"-parked"),
        ))
        .await
        .unwrap();

    assert_eq!(*markers.lock(), Vec::new());
    assert_ne!(
        worker.external_stream_run_status(&run_id).await,
        ExternalStreamRunStatus::Parked,
        "a confirmation for a generation that is not parking must park nothing"
    );
    assert_eq!(
        worker.external_stream_annotation(&run_id).await,
        b"observed"
    );

    // Still retained, so the wait is still deliverable locally -- the discarded confirmation
    // changed nothing at all.
    assert_eq!(
        worker.notify_external_stream_ready(&run_id, 1, 0).await,
        ExternalStreamReadyResult::Accepted
    );
    consume_resolve_activation(&worker, &run_id).await;
    worker.drain_pollers_and_shutdown().await;
}

#[tokio::test]
async fn a_park_job_answered_without_a_result_writes_no_marker() {
    // The paired negative of the confirmed case. Core asked for a terminal and did not get one,
    // and there is no best-effort path from there: an abandoned Workflow Task commits no cursor
    // and loses no record, while a truncated annotation is durable and wrong.
    let mut mock_cfg = MockPollCfg::from_resp_batches(
        "fakeid",
        canned_histories::single_timer("1"),
        [1],
        mock_worker_client(),
    );
    mock_cfg.num_expected_fails = 1;
    mock_cfg.num_expected_completions = Some(0.into());
    let mut mock = build_mock_pollers(mock_cfg);
    mock.worker_cfg(|w| {
        w.task_types = WorkerTaskTypes::workflow_only();
        w.max_cached_workflows = 1;
    });
    let worker = mock_worker(mock);

    let run_id = retain_a_quiescent_task(
        &worker,
        b"observed",
        quiescent_command(1, &[1], Duration::from_millis(30)),
    )
    .await;
    let park = worker.poll_workflow_activation().await.unwrap();
    assert_eq!(park_jobs(&park).len(), 1);

    worker
        .complete_workflow_activation(WorkflowActivationCompletion::empty(run_id.clone()))
        .await
        .unwrap();

    assert_ne!(
        worker.external_stream_run_status(&run_id).await,
        ExternalStreamRunStatus::WftOpen,
        "a park answered without a result must fail the Workflow Task"
    );

    worker.shutdown().await;
    worker.finalize_shutdown().await;
}

#[tokio::test]
async fn an_unprompted_park_result_is_refused() {
    // Without this the "answered correctly" case above would pass either way: accepting an
    // unrequested result would let lang append a terminal to an annotation Core never asked it to
    // close, and park a set Core never asked it to park.
    let mut mock_cfg = MockPollCfg::from_resp_batches(
        "fakeid",
        canned_histories::single_timer("1"),
        [1],
        mock_worker_client(),
    );
    mock_cfg.num_expected_fails = 1;
    mock_cfg.num_expected_completions = Some(0.into());
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
                progress_command(b"unprompted", false),
                park_confirmed_command(1, b"-parked"),
            ],
        ))
        .await
        .unwrap();

    assert_ne!(
        worker.external_stream_run_status(&run_id).await,
        ExternalStreamRunStatus::WftOpen,
        "an unprompted park result must fail the Workflow Task rather than be accepted"
    );

    worker.shutdown().await;
    worker.finalize_shutdown().await;
}

// --- replay marker lookahead (C10) -------------------------------------------

/// The `ReplayExternalStreams` jobs in an activation, as (generation, terminal, waits, annotation).
///
/// Everything a replayed Workflow Task needs to reproduce what the live one saw. Reading it back
/// whole is what makes "the recorded observations reach lang unchanged" checkable rather than
/// assumed -- Core is annotation-blind, so a copy that mangled the bytes would look like success.
#[allow(clippy::type_complexity)]
fn replay_jobs(
    activation: &WorkflowActivation,
) -> Vec<(u64, ParkReason, Vec<(u32, u64)>, Vec<u8>)> {
    activation
        .jobs
        .iter()
        .filter_map(|j| match &j.variant {
            Some(workflow_activation_job::Variant::ReplayExternalStreams(r)) => Some((
                r.quiescence_generation,
                r.terminal_boundary(),
                r.waits.iter().map(|w| (w.wait_id, w.generation)).collect(),
                r.replay_annotation.clone(),
            )),
            _ => None,
        })
        .collect()
}

/// A history whose first Workflow Task parked on a marker and whose second fires a timer.
fn replayable_marker_history() -> TestHistoryBuilder {
    let mut t = TestHistoryBuilder::default();
    t.add_by_type(EventType::WorkflowExecutionStarted);
    t.add_full_wf_task();
    t.add_external_stream_marker_covering(
        2,
        ParkReason::Idle,
        b"header.segment.terminal",
        &[(1, 3), (2, 0)],
    );
    let timer_started = t.add_by_type(EventType::TimerStarted);
    t.add_timer_fired(timer_started, "1".to_string());
    t.add_workflow_task_scheduled_and_started();
    t
}

#[tokio::test]
async fn a_replayed_marker_resolves_the_wait_set_with_no_readiness_path() {
    // The whole point of the marker. On replay the backend is not consulted for *what* was seen --
    // no watcher fires, no readiness is accepted, no idle timer runs and no park is proposed --
    // and yet lang is handed the observations, in the same activation that consumed them live.
    let markers: StreamMarkers = Default::default();
    // One batch containing the whole history, so the first Workflow Task is replayed rather than
    // executed. That is what puts the marker in the *next* sequence, where lookahead finds it.
    let worker = worker_recording_markers(markers.clone(), vec![2], replayable_marker_history());

    let replayed = worker.poll_workflow_activation().await.unwrap();
    let run_id = replayed.run_id.clone();
    assert!(
        replayed.is_replaying,
        "this test is only meaningful while replaying, got {replayed:?}"
    );
    assert_eq!(
        replay_jobs(&replayed),
        vec![(
            2,
            ParkReason::Idle,
            vec![(1, 3), (2, 0)],
            b"header.segment.terminal".to_vec()
        )],
        "the recorded snapshot, waits, terminal and annotation must all reach lang, got {:?}",
        replayed.jobs
    );
    // Not a live resolve and not a park: replay reproduces the recorded boundary rather than
    // re-deciding it, so neither of the paths that need a backend is entered.
    assert_eq!(resolve_hints(&replayed), Vec::<u32>::new());
    assert_eq!(park_jobs(&replayed), Vec::new());
    assert_eq!(
        worker.external_stream_run_status(&run_id).await,
        ExternalStreamRunStatus::NoOpenWorkflowTask,
        "replay must register no wait set, so no idle or rollover deadline can be running"
    );

    worker
        .complete_workflow_activation(WorkflowActivationCompletion::from_cmds(
            run_id.clone(),
            vec![start_timer_cmd(1, Duration::from_secs(10))],
        ))
        .await
        .unwrap();

    // The marker in history settled the machine lookahead created for it, and nothing was written
    // back: re-issuing the command would be matched against the very event it was read from.
    let fired = worker.poll_workflow_activation().await.unwrap();
    assert!(
        fired.jobs.iter().any(|j| matches!(
            j.variant,
            Some(workflow_activation_job::Variant::FireTimer(_))
        )),
        "history after the marker must keep matching, got {:?}",
        fired.jobs
    );
    assert_eq!(
        *markers.lock(),
        Vec::new(),
        "a replayed marker must not be written a second time"
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
async fn a_completion_while_replaying_writes_no_second_marker() {
    // The paired negative for the assertion above. If lang reports progress on a replayed
    // activation -- a bug, but a plausible one -- Core must not turn it into a command: the marker
    // for that Workflow Task is already durable, and a second one would be matched against the
    // event the first produced.
    let markers: StreamMarkers = Default::default();
    let worker = worker_recording_markers(markers.clone(), vec![2], replayable_marker_history());

    let replayed = worker.poll_workflow_activation().await.unwrap();
    let run_id = replayed.run_id.clone();
    assert_eq!(replay_jobs(&replayed).len(), 1);

    worker
        .complete_workflow_activation(WorkflowActivationCompletion::from_cmds(
            run_id.clone(),
            vec![
                progress_command(b"re-observed", false),
                start_timer_cmd(1, Duration::from_secs(10)),
            ],
        ))
        .await
        .unwrap();

    let fired = worker.poll_workflow_activation().await.unwrap();
    assert!(
        fired.jobs.iter().any(|j| matches!(
            j.variant,
            Some(workflow_activation_job::Variant::FireTimer(_))
        )),
        "a replayed progress report must not disturb command matching, got {:?}",
        fired.jobs
    );
    assert_eq!(
        *markers.lock(),
        Vec::new(),
        "replay writes nothing: the marker it would write is the one it just read"
    );
    assert!(
        worker.external_stream_annotation(&run_id).await.is_empty(),
        "and the delta is not left accumulated to leak into the next task's marker"
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
async fn a_marker_no_machine_expects_is_reported_as_nondeterminism() {
    // Handled the way local activities handle the same case: history and the machines disagree
    // about what this run did, and that is nondeterminism, not something to skip past. Core writes
    // exactly one external stream marker per Workflow Task, so a marker that neither a command nor
    // a lookahead accounts for cannot be reconciled with anything.
    //
    // Delivered in two batches so the first task is *executed* rather than replayed: with only the
    // first task's events in hand, lookahead has nothing to find, and the marker then arrives on
    // the next task with no machine expecting it.
    let mut t = TestHistoryBuilder::default();
    t.add_by_type(EventType::WorkflowExecutionStarted);
    t.add_full_wf_task();
    t.add_external_stream_marker(1, ParkReason::Idle, b"written-by-nobody");
    t.add_workflow_task_scheduled_and_started();

    let mut mock_cfg = MockPollCfg::from_resp_batches("fakeid", t, [1, 2], mock_worker_client());
    mock_cfg.num_expected_fails = 1;
    // Asserting the *reason* the task failed, not merely that it did: without this the test would
    // pass on any failure at all, including the generic "no command scheduled for event" a marker
    // would hit if Core simply had no idea what an external stream marker was.
    let saw_the_right_failure = Arc::new(AtomicBool::new(false));
    let recorder = saw_the_right_failure.clone();
    mock_cfg.expect_fail_wft_matcher = Box::new(move |_, cause, failure| {
        let message = failure
            .as_ref()
            .map(|f| f.message.clone())
            .unwrap_or_default();
        recorder.store(
            message.contains("no state machine expecting it")
                && matches!(
                    cause,
                    temporalio_common::protos::temporal::api::enums::v1::WorkflowTaskFailedCause::NonDeterministicError
                ),
            Ordering::Relaxed,
        );
        true
    });
    let mut mock = build_mock_pollers(mock_cfg);
    mock.worker_cfg(|w| {
        w.task_types = WorkerTaskTypes::workflow_only();
        w.max_cached_workflows = 1;
    });
    let worker = mock_worker(mock);

    let activation = worker.poll_workflow_activation().await.unwrap();
    let run_id = activation.run_id.clone();
    assert_eq!(
        replay_jobs(&activation),
        Vec::new(),
        "lookahead cannot have found the marker, or this test proves nothing"
    );

    // Lang produces nothing, so no marker command exists for the event that follows.
    worker
        .complete_workflow_activation(WorkflowActivationCompletion::empty(run_id.clone()))
        .await
        .unwrap();

    // The next task carries the unexplained marker and fails. `num_expected_fails` is what asserts
    // the failure actually reached the server.
    let next = tokio::time::timeout(
        Duration::from_millis(500),
        worker.poll_workflow_activation(),
    )
    .await;
    if let Ok(Ok(act)) = next {
        assert!(
            act.is_only_eviction(),
            "the unexplained marker must fail the task rather than activate lang, got {:?}",
            act.jobs
        );
        worker
            .complete_workflow_activation(WorkflowActivationCompletion::empty(act.run_id))
            .await
            .unwrap();
    }

    assert!(
        saw_the_right_failure.load(Ordering::Relaxed),
        "the marker must fail the task as nondeterminism naming the missing machine"
    );

    worker.shutdown().await;
    worker.finalize_shutdown().await;
}

#[tokio::test]
async fn the_replay_worker_claims_a_marker_followed_by_langs_own_command() {
    // The shape a real Worker actually writes, driven through the entry point lang's `Replayer`
    // actually uses.
    //
    // Core emits the marker command *before* lang's commands are pushed into the machines, so in
    // History the marker sits between the Workflow Task's completion and the command that task
    // produced. On replay the lookahead has to claim that marker; if it does not, the marker event
    // reaches the next machine in the command queue -- lang's timer -- and replay fails with a
    // nondeterminism error naming a machine that has nothing to do with streams.
    //
    // `init_replay_worker` rather than a mock poller because that is the path a whole-history
    // replay takes: one poll response carrying every event, a previous-started id from the *last*
    // Workflow Task, and no live task ever arriving. The mock-poller tests reach the same machines
    // by a different route and would not have caught a difference between the two.
    let mut t = TestHistoryBuilder::default();
    t.add_by_type(EventType::WorkflowExecutionStarted);
    t.add_full_wf_task();
    t.add_external_stream_marker_covering(
        2,
        ParkReason::CommandsProduced,
        b"header.segment.terminal",
        &[(1, 0)],
    );
    let timer_started = t.add_by_type(EventType::TimerStarted);
    t.add_timer_fired(timer_started, "1".to_string());
    t.add_full_wf_task();
    t.add_workflow_execution_completed();

    let worker = crate::init_replay_worker(crate::replay::ReplayWorkerInput::new(
        crate::test_help::test_worker_cfg().build().unwrap(),
        futures_util::stream::iter([crate::replay::HistoryForReplay::from(t)]),
    ))
    .unwrap();

    let replayed = worker.poll_workflow_activation().await.unwrap();
    assert!(replayed.is_replaying);
    assert_eq!(
        replay_jobs(&replayed),
        vec![(
            2,
            ParkReason::CommandsProduced,
            vec![(1, 0)],
            b"header.segment.terminal".to_vec()
        )],
        "the lookahead must claim the marker even though lang's own command follows it in the \
         same Workflow Task, got {:?}",
        replayed.jobs
    );
    worker
        .complete_workflow_activation(WorkflowActivationCompletion::from_cmds(
            replayed.run_id.clone(),
            vec![start_timer_cmd(1, Duration::from_secs(3))],
        ))
        .await
        .unwrap();

    // Reaching the timer at all is the second half of the assertion: the marker was consumed by
    // the machine lookahead created for it, so the command queue still lines up with history.
    let fired = worker.poll_workflow_activation().await.unwrap();
    assert!(
        fired.jobs.iter().any(|j| matches!(
            j.variant,
            Some(workflow_activation_job::Variant::FireTimer(_))
        )),
        "the timer that followed the marker must still match its event, got {:?}",
        fired.jobs
    );
    worker
        .complete_workflow_activation(WorkflowActivationCompletion::from_cmds(
            fired.run_id.clone(),
            vec![CompleteWorkflowExecution::default().into()],
        ))
        .await
        .unwrap();

    // The replay worker ends the stream only when the history ran to completion without a
    // nondeterminism failure, so this is what makes the whole replay -- not just the two
    // activations above -- the thing under test.
    assert!(
        matches!(
            worker.poll_workflow_activation().await,
            Err(PollError::ShutDown)
        ),
        "replay must run the history to its end"
    );
}

// --- the finalization-ownership table, as one assertion ----------------------
//
// Every per-path integration is tested above, in the section that owns it. What none of them can
// say is what the table says as a whole: that *each* completion path writes exactly one marker and
// that each such marker ends with a terminal -- and that the two paths which reach no boundary
// write none. A path that quietly wrote nothing, or wrote a marker whose annotation stops short of
// its terminal, would leave every test above green.

/// The suffix each path's terminal carries, whichever round trip supplied it.
///
/// Core is annotation-blind, so "ends with a terminal" is only checkable if the test knows what a
/// terminal looks like. Every path below ends its annotation with this and nothing else does.
const TERMINAL_SUFFIX: &[u8] = b"|end";

/// Row one: records consumed, no wait pending, and nothing else on the completion.
async fn markers_from_a_normal_completion() -> Vec<StreamMarker> {
    let markers: StreamMarkers = Default::default();
    let worker = worker_recording_markers(
        markers.clone(),
        vec![1],
        canned_histories::single_timer("1"),
    );

    let activation = worker.poll_workflow_activation().await.unwrap();
    worker
        .complete_workflow_activation(WorkflowActivationCompletion::from_cmd(
            activation.run_id,
            progress_command(b"observed|end", false),
        ))
        .await
        .unwrap();

    let written = markers.lock().clone();
    worker.drain_pollers_and_shutdown().await;
    written
}

/// Row two: a server-bound command rides along, so the task is reported rather than retained.
async fn markers_from_a_command_producing_completion() -> Vec<StreamMarker> {
    let markers: StreamMarkers = Default::default();
    let worker = worker_recording_markers(
        markers.clone(),
        vec![1],
        canned_histories::single_timer("1"),
    );

    let activation = worker.poll_workflow_activation().await.unwrap();
    worker
        .complete_workflow_activation(WorkflowActivationCompletion::from_cmds(
            activation.run_id,
            vec![
                progress_command(b"observed|end", false),
                start_timer_cmd(1, Duration::from_secs(10)),
            ],
        ))
        .await
        .unwrap();

    let written = markers.lock().clone();
    worker.drain_pollers_and_shutdown().await;
    written
}

/// Row three: a terminal command ends the Workflow.
async fn markers_from_a_terminal_command() -> Vec<StreamMarker> {
    let markers: StreamMarkers = Default::default();
    let worker = worker_recording_markers(
        markers.clone(),
        vec![1],
        canned_histories::single_timer("1"),
    );

    let activation = worker.poll_workflow_activation().await.unwrap();
    worker
        .complete_workflow_activation(WorkflowActivationCompletion::from_cmds(
            activation.run_id,
            vec![
                progress_command(b"observed|end", false),
                CompleteWorkflowExecution::default().into(),
            ],
        ))
        .await
        .unwrap();

    let written = markers.lock().clone();
    worker.drain_pollers_and_shutdown().await;
    written
}

/// Row four: the idle timer expired and lang confirmed the park, carrying the terminal.
async fn markers_from_a_confirmed_park() -> Vec<StreamMarker> {
    let markers: StreamMarkers = Default::default();
    let worker = worker_recording_markers(markers.clone(), vec![1], marker_then_signal_history(1));

    let run_id = retain_a_quiescent_task(
        &worker,
        b"observed",
        quiescent_command(1, &[1], Duration::from_millis(30)),
    )
    .await;
    let park = worker.poll_workflow_activation().await.unwrap();
    assert_eq!(park_jobs(&park), vec![(1, ParkReason::Idle, vec![1])]);
    worker
        .complete_workflow_activation(WorkflowActivationCompletion::from_cmd(
            run_id,
            park_confirmed_command(1, b"|end"),
        ))
        .await
        .unwrap();

    let written = markers.lock().clone();
    worker.drain_pollers_and_shutdown().await;
    written
}

/// Row five: every wait was fenced, so the set parked without waiting the idle delay out.
async fn markers_from_an_all_fenced_park() -> Vec<StreamMarker> {
    let markers: StreamMarkers = Default::default();
    let worker = worker_recording_markers(markers.clone(), vec![1], marker_then_signal_history(1));

    let run_id = retain_a_quiescent_task(
        &worker,
        b"observed",
        // Five minutes: if this parked from the timer rather than from the fence the helper would
        // hang rather than return the wrong answer.
        fenced_quiescent_command(1, &[(1, true)], Duration::from_secs(300)),
    )
    .await;
    let park = worker.poll_workflow_activation().await.unwrap();
    assert_eq!(
        park_jobs(&park),
        vec![(1, ParkReason::AllWriteFenced, vec![1])]
    );
    worker
        .complete_workflow_activation(WorkflowActivationCompletion::from_cmd(
            run_id,
            park_confirmed_command(1, b"|end"),
        ))
        .await
        .unwrap();

    let written = markers.lock().clone();
    worker.drain_pollers_and_shutdown().await;
    written
}

/// Row six: the rollover deadline expired, and lang returned the terminal Core cannot encode.
async fn markers_from_a_deadline_rollover() -> Vec<StreamMarker> {
    let markers: StreamMarkers = Default::default();
    let forced: Arc<Mutex<Vec<bool>>> = Default::default();
    let worker = worker_recording_rollovers(
        markers.clone(),
        forced,
        marker_then_replacement_history(1, ParkReason::Rollover, b"observed|end"),
        vec![1],
    );

    let run_id = retain_then_fire_the_rollover_deadline(&worker, b"observed").await;
    let finalize = worker.poll_workflow_activation().await.unwrap();
    assert_eq!(finalization_jobs(&finalize).len(), 1);
    worker
        .complete_workflow_activation(WorkflowActivationCompletion::from_cmd(
            run_id,
            finalized_command(1, b"|end"),
        ))
        .await
        .unwrap();

    let written = markers.lock().clone();
    worker.drain_pollers_and_shutdown().await;
    written
}

/// Row seven: lang decided the boundary at the byte budget, so its own report carried the terminal.
async fn markers_from_a_budget_rollover() -> Vec<StreamMarker> {
    let markers: StreamMarkers = Default::default();
    let worker = worker_recording_markers(
        markers.clone(),
        vec![1],
        canned_histories::single_timer("1"),
    );

    let activation = worker.poll_workflow_activation().await.unwrap();
    worker
        .complete_workflow_activation(WorkflowActivationCompletion::from_cmd(
            activation.run_id,
            progress_command(b"observed|end", true),
        ))
        .await
        .unwrap();

    let written = markers.lock().clone();
    worker.drain_pollers_and_shutdown().await;
    written
}

/// Row eight: the Worker shut down while this Run still held its Workflow Task.
async fn markers_from_a_shutdown_with_a_task_open() -> Vec<StreamMarker> {
    let markers: StreamMarkers = Default::default();
    let forced: Arc<Mutex<Vec<bool>>> = Default::default();
    let worker = worker_recording_rollovers(
        markers.clone(),
        forced,
        marker_then_replacement_history(1, ParkReason::Shutdown, b"observed|end"),
        vec![1],
    );

    let run_id = retain_a_quiescent_task(
        &worker,
        b"observed",
        quiescent_command(1, &[1], Duration::from_secs(30)),
    )
    .await;
    worker.initiate_shutdown();
    let finalize = worker.poll_workflow_activation().await.unwrap();
    assert_eq!(finalization_jobs(&finalize).len(), 1);
    worker
        .complete_workflow_activation(WorkflowActivationCompletion::from_cmd(
            run_id,
            finalized_command(1, b"|end"),
        ))
        .await
        .unwrap();

    let written = markers.lock().clone();
    worker.shutdown().await;
    worker.finalize_shutdown().await;
    written
}

/// The first row that writes nothing: lang's recheck found records, so no boundary was reached.
async fn markers_from_an_aborted_park() -> Vec<StreamMarker> {
    let markers: StreamMarkers = Default::default();
    let worker = worker_recording_markers(
        markers.clone(),
        vec![1],
        canned_histories::single_timer("1"),
    );

    let run_id = retain_a_quiescent_task(
        &worker,
        b"observed",
        quiescent_command(1, &[1], Duration::from_millis(30)),
    )
    .await;
    let park = worker.poll_workflow_activation().await.unwrap();
    assert_eq!(park_jobs(&park).len(), 1);
    worker
        .complete_workflow_activation(WorkflowActivationCompletion::from_cmd(
            run_id.clone(),
            park_became_ready_command(1),
        ))
        .await
        .unwrap();

    // Snapshotted before the run is released: the annotation the abort left unwritten belongs to
    // whatever boundary does eventually close it, and that later marker is another row's business.
    let written = markers.lock().clone();
    let resolved = worker.poll_workflow_activation().await.unwrap();
    worker
        .complete_workflow_activation(WorkflowActivationCompletion::from_cmds(
            resolved.run_id,
            vec![CompleteWorkflowExecution::default().into()],
        ))
        .await
        .unwrap();
    worker.drain_pollers_and_shutdown().await;
    written
}

/// The second row that writes nothing: the Worker shut down between this Run's Workflow Tasks.
async fn markers_from_a_shutdown_with_no_open_task() -> Vec<StreamMarker> {
    let markers: StreamMarkers = Default::default();
    let worker = worker_recording_markers(
        markers.clone(),
        vec![1],
        canned_histories::single_timer("1"),
    );

    // The first activation is left outstanding for the whole helper, which is what keeps the Run
    // cached: a Run that goes idle here is dropped when the mock runs out of work.
    let activation = worker.poll_workflow_activation().await.unwrap();
    let run_id = activation.run_id.clone();
    worker
        .seed_external_stream_waits(&run_id, vec![1], None, false)
        .await;
    assert_eq!(
        worker.external_stream_run_status(&run_id).await,
        ExternalStreamRunStatus::NoOpenWorkflowTask
    );

    worker.initiate_shutdown();
    worker
        .complete_workflow_activation(WorkflowActivationCompletion::from_cmds(
            run_id,
            vec![start_timer_cmd(1, Duration::from_secs(10))],
        ))
        .await
        .unwrap();

    let written = markers.lock().clone();
    worker.shutdown().await;
    worker.finalize_shutdown().await;
    written
}

#[tokio::test]
async fn every_completion_path_writes_exactly_one_marker_ending_in_a_terminal() {
    // The cross-path gate. Each row is driven end to end and only its *marker* is looked at, so
    // what is under test is the property the table as a whole claims rather than any one path's
    // mechanics.
    let table: Vec<(&str, Option<ParkReason>, Vec<StreamMarker>)> = vec![
        (
            "normal completion",
            Some(ParkReason::TaskCompleted),
            markers_from_a_normal_completion().await,
        ),
        (
            "command-producing completion",
            Some(ParkReason::CommandsProduced),
            markers_from_a_command_producing_completion().await,
        ),
        (
            "terminal command",
            Some(ParkReason::WorkflowCompleted),
            markers_from_a_terminal_command().await,
        ),
        (
            "park confirmation",
            Some(ParkReason::Idle),
            markers_from_a_confirmed_park().await,
        ),
        (
            "all-fenced immediate park",
            Some(ParkReason::AllWriteFenced),
            markers_from_an_all_fenced_park().await,
        ),
        (
            "deadline rollover",
            Some(ParkReason::Rollover),
            markers_from_a_deadline_rollover().await,
        ),
        (
            "budget rollover",
            Some(ParkReason::BudgetRollover),
            markers_from_a_budget_rollover().await,
        ),
        (
            "shutdown with a Workflow Task open",
            Some(ParkReason::Shutdown),
            markers_from_a_shutdown_with_a_task_open().await,
        ),
        ("aborted park", None, markers_from_an_aborted_park().await),
        (
            "shutdown with no open Workflow Task",
            None,
            markers_from_a_shutdown_with_no_open_task().await,
        ),
    ];

    for (path, expected, written) in &table {
        match expected {
            Some(reason) => {
                assert_eq!(
                    written.len(),
                    1,
                    "{path} must write exactly one marker, got {written:?}"
                );
                assert_eq!(
                    written[0].1, *reason,
                    "{path} recorded the wrong terminal boundary"
                );
                assert!(
                    written[0].2.ends_with(TERMINAL_SUFFIX),
                    "{path} wrote a marker whose annotation stops short of its terminal: {:?}",
                    written[0].2
                );
            }
            None => assert!(
                written.is_empty(),
                "{path} reached no boundary and must write no marker, got {written:?}"
            ),
        }
    }

    // Every marker-writing row records a *different* boundary. Without this the table would still
    // pass if two paths collapsed into one -- and the reason is the only thing in the marker that
    // says which path closed it.
    let reasons: Vec<ParkReason> = table.iter().filter_map(|(_, e, _)| *e).collect();
    assert_eq!(
        reasons.iter().collect::<HashSet<_>>().len(),
        reasons.len(),
        "two completion paths recorded the same terminal boundary: {reasons:?}"
    );
    assert_eq!(
        reasons.len(),
        8,
        "the table has eight paths that write a marker and two that do not"
    );
}
