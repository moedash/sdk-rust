use crate::{
    MetricsContext, WorkerConfig,
    abstractions::dbg_panic,
    internal_flags::CoreInternalFlags,
    protosext::WorkflowActivationExt,
    telemetry::metrics,
    worker::{
        LEGACY_QUERY_ID, LocalActRequest, WorkflowErrorType,
        workflow::{
            ActivationAction, ActivationCompleteOutcome, ActivationCompleteResult,
            ActivationOrAuto, BufferedTasks, DrivenWorkflow, EvictionRequestResult,
            ExternalStreamIdleTimeoutMsg, FailedActivationWFTReport, HistoryUpdate,
            LocalActivityRequestSink, LocalResolution, NextPageReq, OutstandingActivation,
            OutstandingTask, PermittedWFT, RequestEvictMsg, RunBasics, RunTimerSink,
            ServerCommandsWithWorkflowInfo, TaskStorageMetrics, WFCommand, WFCommandVariant,
            WFMachinesError, WFT_HEARTBEAT_TIMEOUT_FRACTION, WFTReportStatus, WorkflowTaskInfo,
            external_streams::{
                ExternalStreamReadyResult, ExternalStreamRunStatus, ExternalWaitSet,
                ExternalWaitState, ParkResolution, ParkStartOutcome, ParkTrigger, ReadinessOutcome,
            },
            history_update::HistoryPaginator,
            machines::{MachinesWFTResponseContent, WorkflowMachines},
            workflow_stream::LocalInputs,
        },
    },
};
use futures_util::future::AbortHandle;
use std::{
    collections::HashSet,
    mem,
    ops::{Add, Sub},
    rc::Rc,
    sync::{Arc, mpsc::Sender},
    time::{Duration, Instant},
};
use temporalio_common::protos::{
    TaskToken,
    coresdk::{
        common::ExternalStorageMetrics,
        external_data::{ExternalStreamMarkerData, ExternalWaitMarker, ParkReason},
        workflow_activation::{
            FinalizeExternalStreams, PrepareExternalStreamPark, ResolveExternalStreamWaits,
            WorkflowActivation, create_evict_activation, query_to_job,
            remove_from_cache::EvictionReason, workflow_activation_job,
        },
        workflow_commands::{
            ExternalStreamFinalized, ExternalStreamParkResult, ExternalStreamWait,
            FailWorkflowExecution, QueryResult, WorkflowOutputStreamCommit, WorkflowStreamProgress,
            external_stream_park_result,
        },
        workflow_completion,
    },
    temporal::api::{
        enums::v1::{VersioningBehavior, WorkflowTaskFailedCause},
        failure::v1::Failure,
    },
};
use tokio::sync::oneshot;
use tracing::Span;

type Result<T, E = WFMachinesError> = std::result::Result<T, E>;
pub(super) type RunUpdateAct = Option<ActivationOrAuto>;

/// Manages access to a specific workflow run. Everything inside is entirely synchronous and should
/// remain that way.
#[derive(derive_more::Debug)]
#[debug(
    "ManagedRun {{ wft: {:?}, activation: {:?}, task_buffer: {:?} \
           trying_to_evict: {} }}",
    wft,
    activation,
    task_buffer,
    "trying_to_evict.is_some()"
)]
pub(super) struct ManagedRun {
    wfm: WorkflowManager,
    /// Called when the machines need to produce local activity requests. This can't be lifted up
    /// easily as return values, because sometimes local activity requests trigger immediate
    /// resolutions (ex: too many attempts). Thus lifting it up creates a lot of unneeded complexity
    /// pushing things out and then directly back in. The downside is this is the only "impure" part
    /// of the in/out nature of workflow state management. If there's ever a sensible way to lift it
    /// up, that'd be nice.
    ///
    /// This field is `None` when `WorkerTaskTypes.enable_local_activities` is false.
    local_activity_request_sink: Option<Rc<dyn LocalActivityRequestSink>>,
    /// Schedules this run's timers without going through the local-activity sink, so a
    /// workflow-only worker still gets its deadlines.
    run_timers: RunTimerSink,
    /// Local work that may retain the open workflow task -- today, outstanding local activities.
    waiting_on_local_work: WaitingOnLocalWork,
    /// Is set to true if the machines encounter an error and the only subsequent thing we should
    /// do is be evicted.
    am_broken: bool,
    /// If set, the WFT this run is currently/will be processing.
    wft: Option<OutstandingTask>,
    /// An outstanding activation to lang
    activation: Option<OutstandingActivation>,
    /// Contains buffered poll responses from the server that apply to this run. This can happen
    /// when:
    ///   * Lang takes too long to complete a task and the task times out
    ///   * Many queries are submitted concurrently and reach this worker (in this case, multiple
    ///     tasks can be outstanding)
    ///   * Multiple speculative tasks (ex: for updates) may also exist at once (but only the
    ///     latest one will matter).
    task_buffer: BufferedTasks,
    /// Is set if an eviction has been requested for this run
    trying_to_evict: Option<RequestEvictMsg>,

    /// We track if we have recorded useful debugging values onto a certain span yet, to overcome
    /// duplicating field values. Remove this once https://github.com/tokio-rs/tracing/issues/2334
    /// is fixed.
    recorded_span_ids: HashSet<tracing::Id>,
    metrics: MetricsContext,
    /// We store the paginator used for our own run's history fetching
    paginator: Option<HistoryPaginator>,
    completion_waiting_on_page_fetch: Option<RunActivationCompletion>,
    config: Arc<WorkerConfig>,
}
impl ManagedRun {
    pub(super) fn new(
        basics: RunBasics,
        wft: PermittedWFT,
        local_activity_request_sink: Option<Rc<dyn LocalActivityRequestSink>>,
        run_timers: RunTimerSink,
    ) -> (Self, RunUpdateAct) {
        let metrics = basics.metrics.clone();
        let config = basics.worker_config.clone();
        let wfm = WorkflowManager::new(basics);
        let mut me = Self {
            wfm,
            local_activity_request_sink,
            run_timers,
            waiting_on_local_work: Default::default(),
            am_broken: false,
            wft: None,
            activation: None,
            task_buffer: Default::default(),
            trying_to_evict: None,
            recorded_span_ids: Default::default(),
            metrics,
            paginator: None,
            completion_waiting_on_page_fetch: None,
            config,
        };
        let rua = me.incoming_wft(wft);
        (me, rua)
    }

    /// Returns true if there are pending jobs that need to be sent to lang.
    pub(super) fn more_pending_work(&self) -> bool {
        // We don't want to consider there to be more local-only work to be done if there is
        // no workflow task associated with the run right now. This can happen if, ex, we
        // complete a local activity while waiting for server to send us the next WFT.
        // Activating lang would be harmful at this stage, as there might be work returned
        // in that next WFT which should be part of the next activation.
        self.wft.is_some() && self.wfm.machines.has_pending_jobs()
    }

    pub(super) fn waiting_on_local_activities(&self) -> bool {
        self.waiting_on_local_work.local_activities.is_some()
    }

    pub(super) fn retains_task_for_external_streams(&self) -> bool {
        self.wft.is_some()
            && (self.waiting_on_local_work.output_buffered
                || matches!(
                    self.external_stream_run_status(),
                    ExternalStreamRunStatus::WftOpen
                ))
    }

    pub(super) fn have_seen_terminal_event(&self) -> bool {
        self.wfm.machines.have_seen_terminal_event
    }

    pub(super) fn workflow_is_finished(&self) -> bool {
        self.wfm.machines.workflow_is_finished()
    }

    /// Returns a ref to info about the currently tracked workflow task, if any.
    pub(super) fn wft(&self) -> Option<&OutstandingTask> {
        self.wft.as_ref()
    }

    /// Returns a ref to info about the currently tracked workflow activation, if any.
    pub(super) fn activation(&self) -> Option<&OutstandingActivation> {
        self.activation.as_ref()
    }

    /// Returns this run's eviction reason if it is going to be evicted
    pub(super) fn trying_to_evict(&self) -> Option<&RequestEvictMsg> {
        self.trying_to_evict.as_ref()
    }

    /// Called whenever a new workflow task is obtained for this run
    pub(super) fn incoming_wft(&mut self, pwft: PermittedWFT) -> RunUpdateAct {
        let res = self._incoming_wft(pwft);
        self.update_to_acts(res.map(Into::into))
    }

    fn _incoming_wft(
        &mut self,
        pwft: PermittedWFT,
    ) -> Result<Option<ActivationOrAuto>, RunUpdateErr> {
        if self.wft.is_some() {
            dbg_panic!("Trying to send a new WFT for a run which already has one!");
            // `dbg_panic!` deliberately logs and continues in release builds.
            // Continuing into the ordinary admission path would overwrite the
            // outstanding task and lose ownership of its task token. Retaining
            // the replacement here gives release builds the same safe ordering
            // the caller's admission guard normally establishes, while debug
            // builds still fail loudly at the violated invariant above.
            self.task_buffer.buffer(pwft);
            return Ok(None);
        }
        let start_time = Instant::now();

        let work = pwft.work;
        debug!(
            task_token = %&work.task_token,
            update = ?work.update,
            has_legacy_query = %work.legacy_query.is_some(),
            messages = ?work.messages,
            attempt = %work.attempt,
            "Applying new workflow task from server"
        );
        let is_incremental = work.is_incremental();
        let wft_info = WorkflowTaskInfo {
            attempt: work.attempt,
            task_token: work.task_token,
            wf_id: work.execution.workflow_id.clone(),
        };

        let legacy_query_from_poll = work
            .legacy_query
            .map(|q| query_to_job(LEGACY_QUERY_ID.to_string(), q));

        let mut pending_queries = work.query_requests;
        if !pending_queries.is_empty() && legacy_query_from_poll.is_some() {
            error!(
                "Server issued both normal and legacy queries. This should not happen. Please \
                 file a bug report."
            );
            return Err(RunUpdateErr {
                source: WFMachinesError::Fatal(
                    "Server issued both normal and legacy query".to_string(),
                ),
                complete_resp_chan: None,
            });
        }
        let was_legacy_query = legacy_query_from_poll.is_some();
        if let Some(lq) = legacy_query_from_poll {
            pending_queries.push(lq);
        }

        self.paginator = Some(pwft.paginator);
        // A Workflow Task is open from here until it is reported. The unwritten-annotation
        // invariant is stated against *that*, not against quiescence -- a task can accumulate a
        // delta and complete without ever asking to be retained.
        self.waiting_on_local_work
            .external_wait_set
            .set_wft_open(true);
        // A finalization or park handshake still outstanding here belongs to a task that failed
        // rather than answering. That boundary is gone with the task, and leaving the expectation
        // set would make the next ordinary completion look like a lang protocol violation.
        self.waiting_on_local_work.pending_finalization = None;
        self.waiting_on_local_work.pending_park = None;
        self.waiting_on_local_work.pending_output_commit = None;
        self.clear_external_output_buffered();
        self.waiting_on_local_work.output_latency_pending = false;
        self.waiting_on_local_work.park_rollback_resolve_pending = false;
        // Same reasoning for a query answer held across that finalization: the task it was going
        // to be reported on is gone, and the server re-delivers the query on the replacement if it
        // is still outstanding. Reporting it on *this* task would answer a query nobody asked.
        self.waiting_on_local_work.deferred_query_responses.clear();
        self.wft = Some(OutstandingTask {
            info: wft_info,
            pending_queries,
            start_time,
            permit: pwft.permit,
        });
        if let Some(waiting) = self.waiting_on_local_work.local_activities.as_mut() {
            waiting.hb_timeout_handle.abort();
            waiting.heartbeat_timeout_pending = false;
        }

        if was_legacy_query
            && work.update.wft_started_id == 0
            && work.update.previous_wft_started_id < self.wfm.machines.get_last_wft_started_id()
        {
            return Ok(Some(ActivationOrAuto::AutoFail {
                run_id: self.run_id().to_string(),
                machines_err: WFMachinesError::Fatal("Query expired".to_string()),
            }));
        }

        // The update field is only populated in the event we hit the cache
        let update_was_real = work.update.is_real();
        if update_was_real {
            if is_incremental {
                self.metrics.sticky_cache_hit();
            }
            self.wfm
                .machines
                .new_work_from_server(work.update, work.messages)?;
        }

        // A wake Signal reaches Core as a history event, so it can only be classified once that
        // history has been applied. The first valid one creates or accompanies this task.
        //
        // This has to happen *before* the activation is built, not after it. `get_wf_activation`
        // derives `is_replaying` from the job list it drains, and an empty list satisfies the
        // "every job is a query" test vacuously, so an activation built with no jobs is flagged
        // replaying. The reserved wake Signal is suppressed from user handlers and therefore
        // produces no job of its own, which is exactly that case: appending the resolve job after
        // the build would hand lang a replacement task marked as replay, lang would report
        // neither stream progress nor quiescence while replaying, the wait generation would never
        // advance, and every later readiness report would be answered `Stale` while the watcher's
        // cursor had already moved past those records -- a silent stall. Queueing the job first
        // lets the flag be computed over a job list that reflects the work being sent.
        if self.apply_external_stream_wakes() {
            self.waiting_on_local_work
                .external_wait_set
                .set_wft_open(true);
            self.maybe_issue_external_stream_resolve();
        }

        // Output capacity is backpressure inside the Workflow, rather than an external event the
        // server can put on the replacement task. The completion that staged the full batch asked
        // the server for this task specifically so lang could enter a new activation and release
        // its capacity waiters. Carry that one-shot intent across the report; any real job already
        // guarantees the activation, otherwise materialize the existing runtime resume job. An
        // empty hint set is intentional and safe -- hints have never been exhaustive, and lang
        // probes every active input wait while its ordinary activation turn also releases the
        // output waiters.
        let output_capacity_activation_pending = mem::take(
            &mut self
                .waiting_on_local_work
                .output_capacity_activation_pending,
        );
        if output_capacity_activation_pending && !self.wfm.machines.has_pending_jobs() {
            self.wfm.machines.send_core_generated_job(
                workflow_activation_job::Variant::ResolveExternalStreamWaits(
                    ResolveExternalStreamWaits {
                        quiescence_generation: self
                            .waiting_on_local_work
                            .external_wait_set
                            .quiescence_generation(),
                        ready_hints: vec![],
                    },
                ),
            );
        }

        let activation = self.wfm.get_next_activation()?;
        if !update_was_real && activation.jobs.is_empty() {
            return Err(RunUpdateErr {
                source: crate::worker::workflow::fatal!(
                    "Machines created for {} with no jobs",
                    self.wfm.machines.run_id
                ),
                complete_resp_chan: None,
            });
        }

        if activation.jobs.is_empty() {
            if self.wfm.machines.outstanding_local_activity_count() > 0 {
                // If the activation has no jobs but there are outstanding LAs, we need to restart
                // the WFT heartbeat.
                if let Some(ref mut lawait) = self.waiting_on_local_work.local_activities {
                    lawait.hb_timeout_handle.abort();
                    let wft_timeout = lawait.wft_timeout;
                    lawait.hb_timeout_handle = Self::start_la_heartbeat_timeout_with(
                        &self.run_timers,
                        &self.wfm.machines.run_id,
                        start_time,
                        wft_timeout,
                    );
                    // No activation needs to be sent to lang. We just need to wait for another
                    // heartbeat timeout or LAs to resolve
                    return Ok(None);
                } else {
                    panic!(
                        "Got a new WFT while there are outstanding local activities, but there \
                     was no waiting on LA info."
                    )
                }
            }
            if self.waiting_on_local_work.external_wait_set.retains_wft() {
                // The replacement task after a rollover. Lang has not been activated, so it
                // cannot re-request retention -- Core carries it across instead, along with every
                // subscription, cursor, and readiness generation the wait set already holds.
                // Autocompleting here would report the replacement task straight back and undo
                // the rollover it was created for.
                self.waiting_on_local_work
                    .external_wait_set
                    .set_wft_open(true);
                self.restart_external_stream_deadlines(start_time);
                return self._check_more_activations();
            }
            return Ok(Some(ActivationOrAuto::Autocomplete {
                run_id: self.wfm.machines.run_id.clone(),
            }));
        }

        Ok(Some(ActivationOrAuto::LangActivation(activation)))
    }

    /// Deletes the currently tracked WFT & records latency metrics. Should be called after it has
    /// been responded to (server has been told). Returns the WFT if there was one.
    pub(super) fn mark_wft_complete(
        &mut self,
        report_status: WFTReportStatus,
        task_storage_metrics: &TaskStorageMetrics,
    ) -> Option<OutstandingTask> {
        debug!("Marking WFT completed");
        // No task is open again until a replacement arrives. The wait set itself survives -- the
        // subscriptions are still registered and their cursors still hold -- but readiness can no
        // longer be delivered locally, which is what `NoOpenWorkflowTask` tells a watcher.
        self.waiting_on_local_work
            .external_wait_set
            .set_wft_open(false);
        let retme = self.wft.take();

        if let Some(ot) = &retme
            && let Some(ct) = report_status.completion_time()
        {
            let task_duration = ct.sub(ot.start_time);
            self.metrics.wf_task_latency(task_duration);
            log_workflow_task_duration(
                &self.wfm.machines.run_id,
                &self.wfm.machines.workflow_type,
                self.wfm.machines.last_processed_event + 1,
                ot.info.attempt,
                self.wfm.machines.history_size_bytes(),
                task_duration,
                task_storage_metrics,
            );
        }

        if let WFTReportStatus::Reported {
            reset_last_started_to,
            ..
        } = report_status
        {
            if let Some(id) = reset_last_started_to {
                self.wfm.machines.reset_last_started_id(id);
                // The server discarded the speculative task and its capacity marker, so its
                // replacement is a redelivery, not the fresh task that may release lang's waiter.
                self.waiting_on_local_work
                    .output_capacity_activation_pending = false;
            }
            // Tell the LA manager that we're done with the WFT
            if let Some(ref local_act_request_sink) = self.local_activity_request_sink {
                local_act_request_sink.sink_reqs(vec![
                    LocalActRequest::IndicateWorkflowTaskCompleted(
                        self.wfm.machines.run_id.clone(),
                    ),
                ]);
            }
        }

        retme
    }

    /// Checks if any further activations need to go out for this run and produces them if so.
    pub(super) fn check_more_activations(&mut self) -> RunUpdateAct {
        let res = self._check_more_activations();
        self.update_to_acts(res.map(Into::into))
    }

    fn _check_more_activations(&mut self) -> Result<Option<ActivationOrAuto>, RunUpdateErr> {
        // No point in checking for more activations if there's already an outstanding activation.
        if self.activation.is_some() {
            return Ok(None);
        }
        // In the event it's time to evict this run, cancel any outstanding LAs
        if self.trying_to_evict.is_some() {
            self.sink_la_requests(vec![LocalActRequest::CancelAllInRun(
                self.wfm.machines.run_id.clone(),
            )])?;
        }

        if self.wft.is_none() {
            // It doesn't make sense to do workflow work unless we have a WFT.
            //
            // This is also the whole of C15b's second transition, and it is a no-op by
            // construction: with no Workflow Task there is nothing accumulated, so no marker is
            // written and none is missing, and there is no task token to set `force_new_wft` on
            // either. The server-visible replacement is lang's wake sweep, which Core must not
            // duplicate (ADR-009).
            return Ok(None);
        }

        // An eviction tears this Run down, so it closes the stream boundary for exactly the same
        // reason Worker shutdown does. Recording the intent *here* rather than in
        // `request_eviction` is what orders finalization ahead of eviction: the eviction
        // activation is produced in this function's final branch, so a `FinalizeExternalStreams`
        // job queued now is always issued -- and answered -- before `RemoveFromCache` is.
        if self.trying_to_evict.is_some() {
            self.begin_external_stream_teardown();
        }

        // Ready waits become a job here rather than at the notification, so readiness that
        // arrived while an activation was outstanding is picked up the moment that activation
        // completes -- with no separate path to keep in step.
        self.maybe_issue_external_stream_resolve();

        // The Run is going away while it still holds a Workflow Task. Anything accumulated needs
        // lang's terminal before a marker may be written, which is what the finalization job asks
        // for; `false` means nothing was accumulated, and the task must then be completed anyway
        // rather than abandoned open, because completing is the only way to request the
        // replacement task that offers this Run back to the task queue.
        let teardown_needs_completion = self.waiting_on_local_work.shutdown_pending
            && !self.am_broken
            && !self.begin_external_stream_finalization(ParkReason::Shutdown);

        if self.wfm.machines.has_pending_jobs() && !self.am_broken {
            Ok(Some(ActivationOrAuto::LangActivation(
                self.wfm.get_next_activation()?,
            )))
        } else if self
            .waiting_on_local_work
            .finished(self.wfm.machines.outstanding_local_activity_count())
        {
            self.waiting_on_local_work
                .local_activities
                .take()
                .expect("local work was just checked to be present")
                .hb_timeout_handle
                .abort();
            Ok(Some(ActivationOrAuto::Autocomplete {
                run_id: self.run_id().to_string(),
            }))
        } else if teardown_needs_completion {
            Ok(Some(ActivationOrAuto::Autocomplete {
                run_id: self.run_id().to_string(),
            }))
        } else {
            if !self.am_broken {
                let has_pending_queries = self
                    .wft
                    .as_ref()
                    .map(|wft| !wft.pending_queries.is_empty())
                    .unwrap_or_default();
                if has_pending_queries {
                    return Ok(Some(ActivationOrAuto::ReadyForQueries(
                        self.wfm.machines.get_wf_activation(),
                    )));
                }
            }
            if self
                .waiting_on_local_work
                .local_activities
                .as_ref()
                .is_some_and(|waiting| waiting.heartbeat_timeout_pending)
            {
                Ok(Some(ActivationOrAuto::Autocomplete {
                    run_id: self.run_id().to_string(),
                }))
            } else if let Some(wte) = self.trying_to_evict.clone() {
                let act =
                    create_evict_activation(self.run_id().to_string(), wte.message, wte.reason);
                Ok(Some(ActivationOrAuto::LangActivation(act)))
            } else {
                Ok(None)
            }
        }
    }

    /// Called whenever lang successfully completes a workflow activation. Commands produced by the
    /// activation are passed in. `resp_chan` will be used to unblock the completion call when
    /// everything we need to do to fulfill it has happened.
    ///
    /// Can return an error in the event that another page of history needs to be fetched before
    /// the completion can proceed.
    pub(super) fn successful_completion(
        &mut self,
        mut commands: Vec<WFCommand>,
        used_flags: Vec<u32>,
        versioning_behavior: VersioningBehavior,
        resp_chan: Option<oneshot::Sender<ActivationCompleteResult>>,
        is_forced_failure: bool,
    ) -> Result<RunUpdateAct, Box<NextPageReq>> {
        let activation_was_only_eviction = self.activation_is_eviction();
        let (task_token, has_pending_query, start_time) = if let Some(entry) = self.wft.as_ref() {
            (
                entry.info.task_token.clone(),
                !entry.pending_queries.is_empty(),
                entry.start_time,
            )
        } else {
            if !activation_was_only_eviction {
                // Not an error if this was an eviction, since it's normal to issue eviction
                // activations without an associated workflow task in that case.
                dbg_panic!(
                    "Attempted to complete activation for run {} without associated workflow task",
                    self.run_id()
                );
            }
            let outcome = if let Some((tt, reason)) = self.trying_to_evict.as_mut().and_then(|te| {
                te.auto_reply_fail_tt
                    .take()
                    .map(|tt| (tt, te.message.clone()))
            }) {
                ActivationCompleteOutcome::ReportWFTFail(FailedActivationWFTReport::Report(
                    tt,
                    WorkflowTaskFailedCause::WorkflowWorkerUnhandledFailure,
                    Failure::application_failure(reason, true).into(),
                ))
            } else {
                ActivationCompleteOutcome::DoNothing
            };
            self.reply_to_complete(outcome, resp_chan);
            return Ok(None);
        };

        // If the only command from the activation is a legacy query response, that means we need
        // to respond differently than a typical activation.
        if matches!(&commands.as_slice(),
                    &[WFCommand {variant: WFCommandVariant::QueryResponse(qr), ..}]
                        if qr.query_id == LEGACY_QUERY_ID)
        {
            let qr = match commands.remove(0) {
                WFCommand {
                    variant: WFCommandVariant::QueryResponse(qr),
                    ..
                } => qr,
                _ => unreachable!("We just verified this is the only command"),
            };
            self.reply_to_complete(
                ActivationCompleteOutcome::ReportWFTSuccess(ServerCommandsWithWorkflowInfo {
                    task_token,
                    action: ActivationAction::RespondLegacyQuery {
                        result: Box::new(qr),
                    },
                    metrics: self.metrics.clone(),
                }),
                resp_chan,
            );
            Ok(None)
        } else {
            let (commands, query_responses) = self.preprocess_command_sequence(commands);

            if activation_was_only_eviction && !commands.is_empty() {
                dbg_panic!("Reply to an eviction included commands");
            }

            let rac = RunActivationCompletion {
                task_token,
                start_time,
                commands,
                activation_was_eviction: self.activation_is_eviction(),
                has_pending_query,
                query_responses,
                used_flags,
                resp_chan,
                is_forced_failure,
                versioning_behavior,
            };

            // Verify we can actually apply the next workflow task, which will happen as part of
            // applying the completion to machines. If we can't, return early indicating we need
            // to fetch a page.
            if !self.wfm.ready_to_apply_next_wft() {
                return if let Some(paginator) = self.paginator.take() {
                    debug!("Need to fetch a history page before next WFT can be applied");
                    self.completion_waiting_on_page_fetch = Some(rac);
                    Err(Box::new(NextPageReq {
                        paginator,
                        span: Span::current(),
                    }))
                } else {
                    Ok(self.update_to_acts(Err(RunUpdateErr {
                        source: WFMachinesError::Fatal(
                            "Run's paginator was absent when attempting to fetch next history \
                                page. This is a Core SDK bug."
                                .to_string(),
                        ),
                        complete_resp_chan: rac.resp_chan,
                    })))
                };
            }

            Ok(self.process_completion(rac))
        }
    }

    /// Core has received from lang a sequence containing all commands generated
    /// by all workflow coroutines. Return a command sequence containing all
    /// non-terminal (i.e. non-workflow-terminating) commands, followed by the
    /// first terminal command if there are any. Also strip out and return query
    /// results (these don't affect machines and are handled separately
    /// downstream)
    ///
    /// The reordering is done in order that all non-terminal commands generated
    /// by workflow coroutines are given a chance for the server to honor them.
    /// For example, in order to deliver an update result to a client as the
    /// workflow completes.
    ///
    /// Behavior here has changed backwards-incompatibly, so a flag is set if
    /// the outcome differs from what the outcome would have been previously.
    /// See also CoreInternalFlags::MoveTerminalCommands docstring and
    /// https://github.com/temporalio/features/issues/481.
    fn preprocess_command_sequence(
        &mut self,
        commands: Vec<WFCommand>,
    ) -> (Vec<WFCommand>, Vec<QueryResult>) {
        if self.wfm.machines.replaying
            && !self
                .wfm
                .machines
                .try_use_flag(CoreInternalFlags::MoveTerminalCommands, false)
        {
            preprocess_command_sequence_old_behavior(commands)
        } else {
            preprocess_command_sequence(commands)
        }
    }

    /// Called after the higher-up machinery has fetched more pages of event history needed to apply
    /// the next workflow task. The history update and paginator used to perform the fetch are
    /// passed in, with the update being used to apply the task, and the paginator stored to be
    /// attached with another fetch request if needed.
    pub(super) fn fetched_page_completion(
        &mut self,
        update: HistoryUpdate,
        paginator: HistoryPaginator,
    ) -> RunUpdateAct {
        let res = self._fetched_page_completion(update, paginator);
        self.update_to_acts(res.map(Into::into))
    }
    fn _fetched_page_completion(
        &mut self,
        update: HistoryUpdate,
        paginator: HistoryPaginator,
    ) -> Result<Option<FulfillableActivationComplete>, RunUpdateErr> {
        self.paginator = Some(paginator);
        if let Some(d) = self.completion_waiting_on_page_fetch.take() {
            self._process_completion(d, Some(update))
        } else {
            dbg_panic!(
                "Shouldn't be possible to be applying a next-page-fetch update when \
                        doing anything other than completing an activation."
            );
            Err(RunUpdateErr::from(WFMachinesError::Fatal(
                "Tried to apply next-page-fetch update to a run that wasn't handling a completion"
                    .to_string(),
            )))
        }
    }

    /// Called whenever either core lang cannot complete a workflow activation. EX: Nondeterminism
    /// or user code threw/panicked. The `cause` and `reason` fields are determined inside core
    /// always. The `failure` field may come from lang. `resp_chan` will be used to unblock the
    /// completion call when everything we need to do to fulfill it has happened.
    pub(super) fn failed_completion(
        &mut self,
        cause: WorkflowTaskFailedCause,
        reason: EvictionReason,
        failure: workflow_completion::Failure,
        is_auto_fail: bool,
        resp_chan: Option<oneshot::Sender<ActivationCompleteResult>>,
    ) -> RunUpdateAct {
        self.clear_external_output_buffered();
        self.waiting_on_local_work.output_latency_pending = false;
        self.waiting_on_local_work.park_rollback_resolve_pending = false;
        let tt = if let Some(tt) = self.wft.as_ref().map(|t| t.info.task_token.clone()) {
            tt
        } else {
            dbg_panic!(
                "No workflow task for run id {} found when trying to fail activation",
                self.run_id()
            );
            self.reply_to_complete(ActivationCompleteOutcome::DoNothing, resp_chan);
            return None;
        };

        let message = format!("Workflow activation completion failed: {:?}", &failure);
        // We don't want to fail queries that could otherwise be retried
        let is_no_report_query_fail = self.pending_work_is_legacy_query()
            && is_auto_fail
            && matches!(
                reason,
                EvictionReason::Unspecified | EvictionReason::PaginationOrHistoryFetch
            );

        let (should_report, rur) = if is_no_report_query_fail {
            (false, None)
        } else {
            // Blow up any cached data associated with the workflow
            let evict_req_outcome = self.request_eviction(RequestEvictMsg {
                run_id: self.run_id().to_string(),
                message,
                reason,
                auto_reply_fail_tt: None,
            });
            let should_report = match &evict_req_outcome {
                EvictionRequestResult::EvictionRequested(Some(attempt), _)
                | EvictionRequestResult::EvictionAlreadyRequested(Some(attempt)) => *attempt <= 1,
                _ => false,
            };
            let rur = evict_req_outcome.into_run_update_resp();
            (should_report, rur)
        };

        let outcome = if self.pending_work_is_legacy_query() {
            if is_no_report_query_fail {
                ActivationCompleteOutcome::WFTFailedDontReport
            } else {
                ActivationCompleteOutcome::ReportWFTFail(
                    FailedActivationWFTReport::ReportLegacyQueryFailure(tt, failure),
                )
            }
        } else if should_report {
            // Check if we should fail the workflow instead of the WFT because of user's preferences
            if matches!(cause, WorkflowTaskFailedCause::NonDeterministicError)
                && self.config.should_fail_workflow(
                    &self.wfm.machines.workflow_type,
                    &WorkflowErrorType::Nondeterminism,
                )
            {
                warn!(failure=?failure, "Failing workflow due to nondeterminism error");
                return self
                    .successful_completion(
                        vec![WFCommand::new(WFCommandVariant::FailWorkflow(
                            FailWorkflowExecution {
                                failure: failure.failure,
                            },
                        ))],
                        vec![],
                        VersioningBehavior::Unspecified, // Doesn't matter since we're failing wf
                        resp_chan,
                        true,
                    )
                    .unwrap_or_else(|e| {
                        dbg_panic!("Got next page request when auto-failing workflow: {e:?}");
                        None
                    });
            } else {
                ActivationCompleteOutcome::ReportWFTFail(FailedActivationWFTReport::Report(
                    tt, cause, failure,
                ))
            }
        } else {
            ActivationCompleteOutcome::WFTFailedDontReport
        };

        self.metrics
            .with_new_attrs([metrics::failure_reason(cause.into())])
            .wf_task_failed();
        self.reply_to_complete(outcome, resp_chan);
        rur
    }

    /// Must be called after the processing of the activation completion and WFT reporting.
    ///
    /// It will delete the currently tracked workflow activation (if there is one) and `pred`
    /// evaluates to true. In the event the activation was an eviction, the bool part of the return
    /// tuple is true. The [BufferedTasks] part will contain any buffered tasks that may still exist
    /// and need to be instantiated into a new instance of the run, if a `wft_from_complete` was
    /// provided, it will supersede any real WFTs in the buffer as by definition those are now
    /// out-of-date.
    pub(super) fn finish_activation(
        &mut self,
        pred: impl FnOnce(&OutstandingActivation) -> bool,
    ) -> (bool, BufferedTasks) {
        let evict = if self.activation().map(pred).unwrap_or_default() {
            let act = self.activation.take();
            act.map(|a| matches!(a, OutstandingActivation::Eviction))
                .unwrap_or_default()
        } else {
            false
        };
        if evict && let Some(sink) = self.local_activity_request_sink.as_deref() {
            let immediate_resolutions = sink.sink_reqs(vec![LocalActRequest::InvalidateRun(
                self.wfm.machines.run_id.clone(),
            )]);
            if !immediate_resolutions.is_empty() {
                dbg_panic!("Invalidating local activities should not produce resolutions");
            }
        }
        let buffered = if evict {
            mem::take(&mut self.task_buffer)
        } else {
            Default::default()
        };
        (evict, buffered)
    }

    /// Called when local activities resolve
    pub(super) fn local_resolution(&mut self, res: LocalResolution) -> RunUpdateAct {
        let res = self._local_resolution(res);
        self.update_to_acts(res.map(Into::into))
    }

    fn process_completion(&mut self, completion: RunActivationCompletion) -> RunUpdateAct {
        let res = self._process_completion(completion, None);
        self.update_to_acts(res.map(Into::into))
    }

    fn _process_completion(
        &mut self,
        completion: RunActivationCompletion,
        update_from_new_page: Option<HistoryUpdate>,
    ) -> Result<Option<FulfillableActivationComplete>, RunUpdateErr> {
        let completing_heartbeat_autocomplete =
            matches!(self.activation, Some(OutstandingActivation::Autocomplete))
                && self.waiting_on_local_work.local_activities.is_some();
        let completing_la_heartbeat = completing_heartbeat_autocomplete
            || self
                .waiting_on_local_work
                .local_activities
                .as_ref()
                .is_some_and(|waiting| waiting.heartbeat_timeout_pending);
        // A run-level rollover deadline forces a replacement task on its own, with no local
        // activity involved -- which is the whole point of the deadline being the run's rather
        // than the local-activity subsystem's.
        let completing_deadline_rollover =
            mem::take(&mut self.waiting_on_local_work.deadline_rollover_pending);
        let mut data = CompletionDataForWFT {
            task_token: completion.task_token,
            query_responses: completion.query_responses,
            has_pending_query: completion.has_pending_query,
            activation_was_eviction: completion.activation_was_eviction,
            is_forced_failure: completion.is_forced_failure,
            versioning_behavior: completion.versioning_behavior,
        };

        self.wfm.machines.add_lang_used_flags(completion.used_flags);

        // If this is just bookkeeping after a reply to an eviction activation, we can bypass
        // everything, since there is no reason to continue trying to update machines.
        if completion.activation_was_eviction {
            return Ok(Some(self.prepare_complete_resp(
                completion.resp_chan,
                data,
                false,
            )));
        }

        // A query answer held across a `FinalizeExternalStreams` round trip rides back onto the
        // completion that finally reports the task. The round trip keeps the Workflow Task open
        // and runs no user Workflow code, so lang has no way to resend the answer; without this it
        // would be answered to nobody and the server would wait its query out.
        if !self
            .waiting_on_local_work
            .deferred_query_responses
            .is_empty()
        {
            data.query_responses
                .append(&mut self.waiting_on_local_work.deferred_query_responses);
        }

        // External stream commands are consumed here rather than by the machines. Taking them
        // first is also what makes "no server-bound command accompanies the completion" checkable:
        // whatever is left after this *is* the server-bound set.
        let mut lang_commands = completion.commands;
        let mut stream_commands = match take_external_stream_commands(&mut lang_commands) {
            Ok(taken) => taken,
            Err(source) => {
                return Err(RunUpdateErr {
                    source,
                    complete_resp_chan: completion.resp_chan,
                });
            }
        };
        let output_was_buffered = self.waiting_on_local_work.output_buffered;
        if let Some(commit) = stream_commands.output_commit.take() {
            if self.waiting_on_local_work.pending_output_commit.is_some() {
                return Err(RunUpdateErr {
                    source: WFMachinesError::Fatal(
                        "Lang sent more than one WorkflowOutputStreamCommit for one Workflow Task"
                            .to_string(),
                    ),
                    complete_resp_chan: completion.resp_chan,
                });
            }
            let Some(manifest) = commit.manifest.as_ref() else {
                return Err(RunUpdateErr {
                    source: WFMachinesError::Fatal(
                        "WorkflowOutputStreamCommit carried no staged output manifest".to_string(),
                    ),
                    complete_resp_chan: completion.resp_chan,
                });
            };
            let Some(history_floor_event_id) = self
                .wfm
                .machines
                .current_wft_history_floor_event_id()
                .filter(|id| *id > 0)
            else {
                return Err(RunUpdateErr {
                    source: WFMachinesError::Fatal(
                        "Refusing an external output commit because Core could not identify the \
                         exact event preceding this Workflow Task's scheduled event"
                            .to_string(),
                    ),
                    complete_resp_chan: completion.resp_chan,
                });
            };
            if manifest.history_floor_event_id != history_floor_event_id {
                return Err(RunUpdateErr {
                    source: WFMachinesError::Fatal(format!(
                        "External output manifest history floor {} does not match this Workflow \
                         Task's exact floor {history_floor_event_id}",
                        manifest.history_floor_event_id
                    )),
                    complete_resp_chan: completion.resp_chan,
                });
            }
            if manifest.run_id != self.wfm.machines.run_id {
                return Err(RunUpdateErr {
                    source: WFMachinesError::Fatal(format!(
                        "External output manifest names Run {} but this Workflow Task belongs to {}",
                        manifest.run_id, self.wfm.machines.run_id
                    )),
                    complete_resp_chan: completion.resp_chan,
                });
            }
            if manifest.stage_token.is_empty() {
                return Err(RunUpdateErr {
                    source: WFMachinesError::Fatal(
                        "External output manifest carried an empty stage token".to_string(),
                    ),
                    complete_resp_chan: completion.resp_chan,
                });
            }
            if manifest.schema_version != 1
                || manifest.fingerprint_version != 1
                || manifest.provider_id.is_empty()
                || manifest.provider_format_version == 0
                || manifest.topics.is_empty()
                || manifest.segments.is_empty()
                || <_ as prost::Message>::encoded_len(manifest) > 64 * 1024
            {
                return Err(RunUpdateErr {
                    source: WFMachinesError::Fatal(
                        "External output manifest has an unsupported version, missing provider, \
                         empty schedule, or exceeds the 64 KiB marker budget"
                            .to_string(),
                    ),
                    complete_resp_chan: completion.resp_chan,
                });
            }
            let mut topic_names = HashSet::new();
            if manifest.topics.iter().any(|topic| {
                topic.topic.is_empty()
                    || !topic_names.insert(topic.topic.as_str())
                    || topic.record_count == 0
                    || topic.logical_byte_count == 0
                    || topic.logical_fingerprint.len() != 32
            }) || manifest
                .segments
                .iter()
                .any(|segment| segment.record_counts_by_topic.len() != manifest.topics.len())
                || manifest
                    .topics
                    .iter()
                    .enumerate()
                    .any(|(topic_index, topic)| {
                        manifest
                            .segments
                            .iter()
                            .map(|segment| u64::from(segment.record_counts_by_topic[topic_index]))
                            .sum::<u64>()
                            != u64::from(topic.record_count)
                    })
            {
                return Err(RunUpdateErr {
                    source: WFMachinesError::Fatal(
                        "External output manifest has duplicate or malformed topics, \
                         fingerprints, or activation segment counts"
                            .to_string(),
                    ),
                    complete_resp_chan: completion.resp_chan,
                });
            }
            self.waiting_on_local_work.pending_output_commit = Some(commit);
            self.clear_external_output_buffered();
        }
        if let Some(max_publish_latency) = stream_commands.output_buffered_latency.take()
            && !self.wfm.machines.replaying
        {
            self.note_external_output_buffered(max_publish_latency);
        }
        let output_commit_pending = self.waiting_on_local_work.pending_output_commit.is_some();
        let completing_output_capacity = self
            .waiting_on_local_work
            .pending_output_commit
            .as_ref()
            .is_some_and(|commit| commit.request_rollover);
        let has_server_bound_commands = !lang_commands.is_empty();

        // Lang answered `PrepareExternalStreamPark`. Paired against the *issued* job for the same
        // reason finalization is: a park job answered without a result would leave Core holding a
        // boundary it decided with no terminal to write, and an unprompted result would let lang
        // close an annotation Core never asked it to close.
        let output_latency_preempts_park = self.waiting_on_local_work.output_latency_pending
            && self.waiting_on_local_work.pending_park.is_some();
        let park_outcome = match (
            self.waiting_on_local_work.pending_park,
            stream_commands.park_result,
        ) {
            (Some(PendingPark::Queued(_)), None) if output_latency_preempts_park => {
                self.waiting_on_local_work.pending_park = None;
                self.wfm.machines.discard_pending_external_stream_park();
                self.abort_external_stream_park_for_boundary();
                Some(ParkApplication::Aborted)
            }
            (Some(PendingPark::Issued(_)), Some(_)) if output_latency_preempts_park => {
                self.waiting_on_local_work.pending_park = None;
                self.abort_external_stream_park_for_boundary();
                Some(ParkApplication::Aborted)
            }
            (Some(PendingPark::Issued(reason)), Some(result)) => {
                self.waiting_on_local_work.pending_park = None;
                Some(self.apply_park_result(reason, result))
            }
            (Some(PendingPark::Issued(reason)), None) => {
                return Err(RunUpdateErr {
                    source: WFMachinesError::Fatal(format!(
                        "Lang answered PrepareExternalStreamPark({reason:?}) without an \
                         ExternalStreamParkResult. Core never manufactures a terminal, so no \
                         marker is written and the Workflow Task is retried."
                    )),
                    complete_resp_chan: completion.resp_chan,
                });
            }
            (_, Some(_)) => {
                return Err(RunUpdateErr {
                    source: WFMachinesError::Fatal(
                        "Lang sent ExternalStreamParkResult with no park handshake outstanding"
                            .to_string(),
                    ),
                    complete_resp_chan: completion.resp_chan,
                });
            }
            (Some(PendingPark::Queued(_)) | None, None) => None,
        };
        // A handshake in flight retains the Workflow Task in every state but one. Queued or
        // issued, the set is in `Parking` and the job still has to be delivered and answered;
        // aborted or stale, the resolve activation Core owes lang needs a task to arrive on.
        // Only a *confirmed* park ends the task, and it ends it by writing the marker whose
        // terminal the confirmation just supplied.
        let park_retains = self.waiting_on_local_work.pending_park.is_some()
            || matches!(
                park_outcome,
                Some(ParkApplication::Aborted | ParkApplication::Stale)
            );
        let staged_output_from_losing_park = output_commit_pending
            && matches!(
                park_outcome,
                Some(ParkApplication::Aborted | ParkApplication::Stale)
            );
        if staged_output_from_losing_park {
            // Lang may already have staged output before Core discovers that readiness beat its
            // confirmation. The park terminal cannot be used, so finish rolling the park back and
            // obtain a valid terminal through finalization before recording the staged batch.
            self.abort_external_stream_park_for_boundary();
        }

        // A `FinalizeExternalStreams` job's only legal responses are `ExternalStreamFinalized` or
        // an activation failure. Anything else means Core asked for a terminal and did not get
        // one, and there is no best-effort path from there: writing a marker anyway would commit
        // a truncated annotation, which is durable and wrong, so the Workflow Task is failed and
        // retried instead. An abandoned task commits no cursor and loses no record.
        let finalized_boundary = match (
            self.waiting_on_local_work.pending_finalization.take(),
            &stream_commands.finalized,
        ) {
            (Some(reason), Some(finalized)) => {
                if (reason == ParkReason::OutputLatency || output_was_buffered)
                    && !output_commit_pending
                {
                    return Err(RunUpdateErr {
                        source: WFMachinesError::Fatal(format!(
                            "Lang answered FinalizeExternalStreams({reason:?}) while external \
                             output was buffered but supplied no WorkflowOutputStreamCommit"
                        )),
                        complete_resp_chan: completion.resp_chan,
                    });
                }
                self.waiting_on_local_work
                    .external_wait_set
                    .accumulate_annotation(&finalized.final_observation_delta);
                Some(reason)
            }
            (Some(reason), None) => {
                return Err(RunUpdateErr {
                    source: WFMachinesError::Fatal(format!(
                        "Lang answered FinalizeExternalStreams({reason:?}) without an \
                         ExternalStreamFinalized command. Core never manufactures a terminal, so \
                         no marker is written and the Workflow Task is retried."
                    )),
                    complete_resp_chan: completion.resp_chan,
                });
            }
            (None, Some(_)) => {
                return Err(RunUpdateErr {
                    source: WFMachinesError::Fatal(
                        "Lang sent ExternalStreamFinalized with no finalization job outstanding"
                            .to_string(),
                    ),
                    complete_resp_chan: completion.resp_chan,
                });
            }
            (None, None) => None,
        };

        // Accumulate on *every* completion path, retained or not: consuming a record and
        // committing that consumption are separate steps, and the second is not conditional on
        // why the Workflow Task ended. An empty delta accumulates like any other -- it is how a
        // subscription that observed nothing still records that it observed.
        for progress in &stream_commands.progress {
            self.waiting_on_local_work
                .external_wait_set
                .accumulate_annotation(&progress.observation_delta);
            if progress.request_rollover {
                // Lang decided this boundary, so the rollover needs no finalization round trip --
                // this very command already carried the terminal.
                self.waiting_on_local_work.budget_rollover_pending = true;
            }
        }
        let completing_budget_rollover =
            mem::take(&mut self.waiting_on_local_work.budget_rollover_pending);
        // This Run is being torn down -- Worker shutdown, or an eviction -- while it still holds
        // the Workflow Task (C15b). Like the rollover deadline it is a boundary *Core* decided, so
        // the terminal is owed by a finalization round trip; and like the rollover intent it has
        // to ride across that round trip, because the completion that *asked* consumed the flag.
        let shutdown_was_pending = mem::take(&mut self.waiting_on_local_work.shutdown_pending);
        let completing_shutdown =
            shutdown_was_pending || finalized_boundary == Some(ParkReason::Shutdown);
        // A finalization response carries the rollover intent across its round trip: the deadline
        // flag was consumed by the completion that *asked*, so without this the completion that
        // finally writes the marker would forget to request a replacement task.
        let completing_rollover = completing_deadline_rollover
            || completing_budget_rollover
            || finalized_boundary == Some(ParkReason::Rollover);
        let output_latency_was_pending =
            mem::take(&mut self.waiting_on_local_work.output_latency_pending);
        let completing_output_latency =
            output_latency_was_pending || finalized_boundary == Some(ParkReason::OutputLatency);
        let completing_park_rollback = finalized_boundary.is_some()
            && mem::take(&mut self.waiting_on_local_work.park_rollback_resolve_pending);
        if completing_park_rollback {
            // The mixed resolve/finalize activation removes the losing park's backend intents,
            // but finalization deliberately does not run Workflow code. Re-publishing readiness
            // makes the forced replacement task perform the drain the losing park interrupted.
            self.waiting_on_local_work
                .external_wait_set
                .mark_all_ready_after_aborted_park();
        }

        // Registering the wait set and retaining the Workflow Task are two separate questions, and
        // they are decided separately here. Both are decided before anything is pushed into the
        // machines so the marker can be ordered ahead of lang's own commands.
        //
        // These are the boundaries that answer *both* questions "no". Each of them either tears
        // the Run down or hands it to a finalization round trip that has already named the current
        // quiescence generation, so re-recording lang's snapshot would bump that generation
        // underneath a job already in flight for it.
        let boundary_closes_the_run = data.activation_was_eviction
            || completing_rollover
            || completing_shutdown
            || completing_output_latency
            || staged_output_from_losing_park
            || finalized_boundary.is_some();
        // A pending rollover overrides a retention request. Lang asked to be held open without
        // knowing the deadline had already expired, and honouring that would restart the deadline
        // and hold the task past the timeout it exists to stay inside -- rollover is
        // authoritative, so it wins.
        //
        // A teardown overrides it for a harder reason still: lang asked to be held open on a
        // Worker that is going away, and honouring that would leave a Workflow Task retained with
        // nothing left to release it and no replacement task ever coming.
        let retention_requested = (stream_commands.quiescence.is_some()
            || park_retains
            || self.waiting_on_local_work.output_buffered)
            && !boundary_closes_the_run;
        // Replay runs no timers. A replayed Run still has to *register* its wait set -- that set
        // is per-Worker runtime state, not History, so nothing else rebuilds it -- but retaining a
        // replayed task would arm the idle and rollover deadlines against wall-clock time that has
        // nothing to do with the recorded boundary, and a replay slower than the idle timeout
        // would queue a park handshake in between replay activations. The marker reproduces the
        // boundary instead; there is nothing here for a timer to decide.
        let replaying = self.wfm.machines.replaying;
        let answering_a_query = !data.query_responses.is_empty();
        let will_retain = retention_requested
            && !has_server_bound_commands
            && !answering_a_query
            && !replaying
            && !output_commit_pending;

        // Registering is not retaining. A completion that also produced a timer, activity, child
        // workflow or signal must be reported so the server can act on it -- but the subscriptions
        // that same completion described are still what the Workflow is blocked on, and Core is
        // the only place they are recorded. Dropping them along with the retention leaves the Run
        // permanently unresumable: nothing is registered, so a wake Signal marks nothing ready,
        // readiness has no wait to resolve against, and every Workflow Task the wake produces is
        // empty.
        let registers_without_retaining = !will_retain && !boundary_closes_the_run;
        // The one refusal lang's own commands do not account for. Lang asked to be held open, so
        // its `WorkflowStreamProgress` deliberately carried no terminal -- the terminal was going
        // to arrive on a park result or a finalization. Core is refusing because a query answer
        // has to be reported, and that makes this a boundary *Core* decided, owed a finalization
        // round trip exactly as the rollover deadline and a teardown are. Writing `TaskCompleted`
        // here instead would commit an annotation with no terminal frame, which ADR-008 forbids
        // without exception.
        //
        // "Still blocked" covers two shapes and has to cover both. Lang may have reported a fresh
        // snapshot on this very completion, or the snapshot it reported earlier may still be
        // registered and still retaining -- which is what a query answered on its own activation
        // looks like, since the completion carrying the answer produces no stream command at all.
        // Lang's terminal is no more available in the second shape than in the first, and the
        // annotation at risk was accumulated before either.
        let stream_waits_still_pending = stream_commands.quiescence.is_some()
            || park_retains
            || self.waiting_on_local_work.external_wait_set.retains_wft()
            || self.waiting_on_local_work.output_buffered;
        // A registered wait can belong to a previous task whose input has since resumed lang.
        // Only a current wait (or a query-only activation preserving that wait) justifies an
        // output replacement. Otherwise an empty forced task can buffer the Activity or timer
        // result that lang is actually waiting for until the task times out.
        //
        // Buffered output is not one of those stale waits. Staging the commit clears the flag, so
        // it can only be set here by a `WorkflowOutputStreamBuffered` on this same completion, and
        // the flush deadline it asks for fires into nothing once the task is gone.
        let output_waits_need_replacement = stream_commands.quiescence.is_some()
            || park_retains
            || self.waiting_on_local_work.output_buffered
            || (answering_a_query && self.waiting_on_local_work.external_wait_set.retains_wft());
        let query_refused_retention = stream_waits_still_pending
            && !boundary_closes_the_run
            && !has_server_bound_commands
            && !replaying
            && answering_a_query;

        // `WorkflowStreamProgress` precedes every command whose value could depend on the
        // consumed data, and so must the marker recording it. Emitting after lang's commands were
        // pushed would put the marker *after* the terminal command in History, and on replay the
        // command would then be matched before the record it came from was validated.
        let terminal = if will_retain {
            None
        } else if let Some(ParkApplication::Confirmed(reason)) = park_outcome {
            // Park owns its own marker path. The terminal arrived on the park result itself, so
            // there is no finalization round trip to wait for and nothing is owed -- which is why
            // idle park is C8's and not the finalization protocol's.
            Some(reason)
        } else if let Some(reason) = finalized_boundary {
            // Core decided this boundary and lang has now supplied its terminal.
            Some(reason)
        } else if lang_commands.iter().any(|c| c.variant.is_terminal()) {
            Some(ParkReason::WorkflowCompleted)
        } else if completing_output_capacity {
            Some(ParkReason::OutputCapacity)
        } else if completing_budget_rollover {
            Some(ParkReason::BudgetRollover)
        } else if has_server_bound_commands {
            Some(ParkReason::CommandsProduced)
        } else if completing_deadline_rollover
            || completing_shutdown
            || completing_output_latency
            || staged_output_from_losing_park
            || query_refused_retention
        {
            // Core decided this boundary and lang was never asked for a terminal. Nothing may be
            // written until the finalization round trip below supplies one.
            None
        } else {
            Some(ParkReason::TaskCompleted)
        };

        if terminal.is_some()
            && self.waiting_on_local_work.output_buffered
            && !output_commit_pending
        {
            return Err(RunUpdateErr {
                source: WFMachinesError::Fatal(
                    "A Workflow Task reached a durable boundary while external output was still \
                     buffered; lang must stage it and send WorkflowOutputStreamCommit first"
                        .to_string(),
                ),
                complete_resp_chan: completion.resp_chan,
            });
        }

        if let Some(terminal) = terminal
            && let Err(source) = self.emit_external_stream_marker(terminal)
        {
            return Err(RunUpdateErr {
                source,
                complete_resp_chan: completion.resp_chan,
            });
        }
        if terminal == Some(ParkReason::OutputCapacity) {
            // `force_new_wft` obtains the task, and this one-shot flag makes that otherwise-empty
            // task observable to lang. It is consumed only when the replacement is admitted.
            self.waiting_on_local_work
                .output_capacity_activation_pending = true;
        }

        // Recorded *after* the marker, so a marker still closes the snapshot that was in effect
        // while the records it carries were consumed, and *before* the finalization request below,
        // so a job asking lang to finalize names the snapshot lang has just reported rather than
        // the one it superseded.
        if registers_without_retaining && let Some(request) = stream_commands.quiescence.take() {
            self.register_external_stream_quiescence(request);
        }

        // The rollover deadline, a teardown, and a query answer are the three boundaries that
        // reach here still owing a terminal. `false` means there was no annotation to finalize, so
        // the task simply completes with no marker -- nothing is owed and nothing is missing.
        // Teardown outranks the deadline when both are pending: the replacement task the deadline
        // wanted is the same one the teardown asks for, and one boundary gets one marker.
        let awaiting_finalization = terminal.is_none()
            && !will_retain
            && (completing_deadline_rollover
                || completing_shutdown
                || completing_output_latency
                || staged_output_from_losing_park
                || query_refused_retention)
            && self.begin_external_stream_finalization(if completing_shutdown {
                ParkReason::Shutdown
            } else if completing_deadline_rollover {
                ParkReason::Rollover
            } else if completing_output_latency {
                ParkReason::OutputLatency
            } else {
                ParkReason::TaskCompleted
            });

        if awaiting_finalization {
            self.cancel_external_output_flush_timer();
        }

        let outcome = (|| {
            // Send commands from lang into the machines then check if the workflow run needs
            // another activation and mark it if so
            self.wfm.push_commands_and_iterate(lang_commands)?;
            if let Some(update) = update_from_new_page {
                self.wfm.feed_history_from_new_page(update)?;
            }
            // Don't bother applying the next task if we're evicting at the end of this activation
            // or are otherwise broken.
            if !completion.activation_was_eviction && !self.am_broken {
                self.wfm.apply_next_task_if_ready()?;
            }
            // A cold replay can reach a wake already contained in this History page, without
            // admitting another server task. Its reconstructed waits must receive that wake
            // before this task is reported empty and the zero-sized cache evicts them again.
            if !self.wfm.machines.replaying && self.apply_external_stream_wakes() {
                self.waiting_on_local_work
                    .external_wait_set
                    .set_wft_open(true);
                // Lang has finished this activation; only its completion bookkeeping remains.
                // Queue now so prepare_complete_resp sees pending work rather than reporting
                // this task before finish_activation makes the next activation deliverable.
                self.queue_external_stream_resolve(true);
            }
            let new_local_acts = self.wfm.drain_queued_local_activities();
            self.sink_la_requests(new_local_acts)?;

            if self.wfm.machines.outstanding_local_activity_count() == 0 {
                Ok(None)
            } else {
                let wft_timeout: Duration = self
                    .wfm
                    .machines
                    .get_started_info()
                    .and_then(|attrs| attrs.workflow_task_timeout)
                    .ok_or_else(|| {
                        WFMachinesError::Fatal(
                            "Workflow's start attribs were missing a well formed task timeout"
                                .to_string(),
                        )
                    })?;
                Ok(Some((completion.start_time, wft_timeout)))
            }
        })();

        match outcome {
            Ok(None) => {
                if let Some(waiting) = self.waiting_on_local_work.local_activities.take() {
                    waiting.hb_timeout_handle.abort();
                }

                // The finalization job is queued but not yet shipped. Reporting the task now would
                // complete it before its terminal exists, so the task stays open until lang
                // answers -- which is the whole point of Core never writing a marker for a
                // boundary it decided without one.
                if awaiting_finalization {
                    // A query answer that reached here must survive the round trip. The task is
                    // still open, so nothing has been reported to the server yet, and the
                    // finalization activation runs no user Workflow code -- lang cannot resend
                    // what it already answered.
                    self.waiting_on_local_work.deferred_query_responses =
                        mem::take(&mut data.query_responses);
                    return Ok(Some(FulfillableActivationComplete {
                        result: ActivationCompleteResult {
                            outcome: ActivationCompleteOutcome::DoNothing,
                            replaying: self.wfm.machines.replaying,
                        },
                        resp_chan: completion.resp_chan,
                    }));
                }

                // Retention applies only when nothing server-bound rides along. A completion that
                // also produced a timer, activity, child workflow, or signal must be reported so
                // the server can act on it; the subscriptions stay registered and are woken by
                // the wake Signal instead.
                if will_retain {
                    // An abandoned park retains without a new snapshot: the quiescent generation
                    // it failed to close is still the current one, so re-recording it would bump
                    // the generation and strand any readiness already accepted against the old.
                    if let Some(request) = stream_commands.quiescence {
                        self.begin_external_stream_quiescence(request);
                    }
                    return Ok(Some(FulfillableActivationComplete {
                        result: ActivationCompleteResult {
                            outcome: ActivationCompleteOutcome::DoNothing,
                            replaying: self.wfm.machines.replaying,
                        },
                        resp_chan: completion.resp_chan,
                    }));
                }

                // Not retaining: whatever quiescent snapshot was recorded no longer holds a task
                // open, so its timers must not outlive it.
                self.cancel_external_stream_idle_timer();
                self.cancel_wft_rollover_timer();

                Ok(Some(self.prepare_complete_resp(
                    completion.resp_chan,
                    data,
                    completing_heartbeat_autocomplete
                        || completing_rollover
                        || completing_shutdown
                        || completing_output_capacity
                        || completing_output_latency
                        || (output_commit_pending && output_waits_need_replacement),
                )))
            }
            Ok(Some((start_t, wft_timeout))) => {
                if let Some(wola) = self.waiting_on_local_work.local_activities.as_mut() {
                    wola.hb_timeout_handle.abort();
                }
                if completing_la_heartbeat || !data.query_responses.is_empty() {
                    // Reporting a query while an LA is still running must request another WFT;
                    // otherwise the LA could resolve without a task on which to deliver its job.
                    let hb_timeout_handle = Self::start_la_heartbeat_timeout_with(
                        &self.run_timers,
                        self.run_id(),
                        start_t,
                        wft_timeout,
                    );
                    hb_timeout_handle.abort();
                    self.waiting_on_local_work.local_activities =
                        Some(LocalActivityHeartbeatState {
                            wft_timeout,
                            hb_timeout_handle,
                            // Keep this set until the replacement WFT arrives. If pending workflow
                            // jobs prevent this completion from being reported, the heartbeat still
                            // needs to be honored after those jobs are processed.
                            heartbeat_timeout_pending: completing_la_heartbeat,
                        });
                    Ok(Some(self.prepare_complete_resp(
                        completion.resp_chan,
                        data,
                        true,
                    )))
                } else {
                    self.waiting_on_local_work.local_activities =
                        Some(LocalActivityHeartbeatState {
                            wft_timeout,
                            hb_timeout_handle: Self::start_la_heartbeat_timeout_with(
                                &self.run_timers,
                                self.run_id(),
                                start_t,
                                wft_timeout,
                            ),
                            heartbeat_timeout_pending: false,
                        });
                    // Retaining the task reports nothing to the server, so a rollover that was
                    // pending has not been acted on and must stay pending -- otherwise the
                    // deadline would be silently swallowed by the very completion that keeps the
                    // task open past it. Unless a finalization is already in flight for it, in
                    // which case that round trip carries the intent and re-arming here would ask
                    // for the same boundary twice.
                    self.waiting_on_local_work.deadline_rollover_pending |=
                        completing_deadline_rollover && !awaiting_finalization;
                    self.waiting_on_local_work.budget_rollover_pending |=
                        completing_budget_rollover;
                    // Same for the teardown intent, and for the same reason: the task is still
                    // open, so nothing has been handed back to the task queue yet. Only the raw
                    // flag is restored -- a boundary already carried by a finalization in flight
                    // would otherwise be asked for twice.
                    self.waiting_on_local_work.shutdown_pending |=
                        shutdown_was_pending && !awaiting_finalization;
                    Ok(Some(FulfillableActivationComplete {
                        result: ActivationCompleteResult {
                            outcome: ActivationCompleteOutcome::DoNothing,
                            replaying: self.wfm.machines.replaying,
                        },
                        resp_chan: completion.resp_chan,
                    }))
                }
            }
            Err(e) => Err(RunUpdateErr {
                source: e,
                complete_resp_chan: completion.resp_chan,
            }),
        }
    }

    fn _local_resolution(
        &mut self,
        res: LocalResolution,
    ) -> Result<Option<ActivationOrAuto>, RunUpdateErr> {
        debug!(resolution=?res, "Applying local resolution");
        self.wfm.notify_of_local_result(res)?;
        if self.activation.is_none() {
            self._check_more_activations()
        } else {
            Ok(None)
        }
    }

    // --- External stream retention (C6) ------------------------------------

    /// This run's workflow task timeout, which both run-level deadlines derive from.
    fn wft_timeout(&self) -> Option<Duration> {
        self.wfm
            .machines
            .get_started_info()
            .and_then(|attrs| attrs.workflow_task_timeout)
    }

    /// When the workflow task this run currently holds started.
    ///
    /// The rollover deadline is anchored here rather than at the moment it is (re)armed, because
    /// what it bounds is the *task*, not the wait it was armed for.
    fn current_wft_start_time(&self) -> Instant {
        self.wft
            .as_ref()
            .map(|wft| wft.start_time)
            .unwrap_or_else(Instant::now)
    }

    /// Writes the marker for a Workflow Task lang itself closed.
    ///
    /// These are the three paths where lang's own `WorkflowStreamProgress` carried the terminal,
    /// so no finalization round trip is needed -- Core already has everything it needs. The
    /// Core-decided boundaries (park, rollover deadline, shutdown) each integrate against this
    /// same primitive but obtain their terminal first.
    fn emit_external_stream_marker(
        &mut self,
        terminal_boundary: ParkReason,
    ) -> Result<(), WFMachinesError> {
        let output = self
            .waiting_on_local_work
            .pending_output_commit
            .take()
            .and_then(|commit| commit.manifest);
        let set = &mut self.waiting_on_local_work.external_wait_set;
        if set.replay_annotation().is_empty() && output.is_none() {
            // Nothing was observed, so there is nothing to record. Not an error: a Workflow Task
            // that touched no stream is the ordinary case.
            return Ok(());
        }
        let quiescence_generation = set.quiescence_generation();
        let waits = set
            .marker_waits()
            .into_iter()
            .map(|(wait_id, generation)| ExternalWaitMarker {
                wait_id,
                generation,
            })
            .collect();
        let replay_annotation = set
            .take_annotation()
            .map_err(|err| WFMachinesError::Fatal(err.to_string()))?;

        self.wfm
            .machines
            .emit_external_stream_marker(ExternalStreamMarkerData {
                schema_version: EXTERNAL_STREAM_MARKER_SCHEMA_VERSION,
                quiescence_generation,
                waits,
                replay_annotation,
                terminal_boundary: terminal_boundary as i32,
                output,
            })
    }

    /// Asks lang for the terminal of a boundary **Core** decided (C15a).
    ///
    /// The annotation ends with a blocked cursor snapshot and only lang can encode it, so Core
    /// cannot close a boundary it decided without asking. This is the protocol primitive, and it
    /// is deliberately independent of which boundary triggered it -- rollover and shutdown both
    /// come through here. Park does **not**: it obtains its terminal from
    /// `ExternalStreamParkResult`, which is a different round trip.
    ///
    /// Returns `true` when a job was issued. `false` means there is nothing to finalize, because
    /// nothing was accumulated -- so no marker is owed and none is missing.
    fn begin_external_stream_finalization(&mut self, reason: ParkReason) -> bool {
        if self.waiting_on_local_work.pending_finalization.is_some() {
            // Already asked. A second job for one boundary would put two runtime-internal
            // activations in flight, and there is never more than one outstanding per run.
            return true;
        }
        let set = &self.waiting_on_local_work.external_wait_set;
        if set.replay_annotation().is_empty()
            && !self.waiting_on_local_work.output_buffered
            && self.waiting_on_local_work.pending_output_commit.is_none()
        {
            return false;
        }
        let quiescence_generation = set.quiescence_generation();
        let waits = set
            .wait_snapshot()
            .into_iter()
            .map(
                |(wait_id, generation, immediately_parkable)| ExternalStreamWait {
                    wait_id,
                    generation,
                    immediately_parkable,
                },
            )
            .collect();

        self.waiting_on_local_work.pending_finalization = Some(reason);
        self.wfm.machines.send_core_generated_job(
            workflow_activation_job::Variant::FinalizeExternalStreams(FinalizeExternalStreams {
                quiescence_generation,
                waits,
                reason: reason as i32,
            }),
        );
        true
    }

    /// The Worker is shutting down while this Run may still be holding a Workflow Task (C15b).
    ///
    /// Nothing else will close that boundary: the pollers are stopped so no replacement task is
    /// coming, lang is not being activated, and `shutdown_done` counts an open Workflow Task as
    /// pending work -- so a Run retained by an external stream wait set would keep the whole
    /// Worker from finishing.
    ///
    /// Runs with no open Workflow Task are left untouched, deliberately. That is not an omission:
    /// nothing is accumulated there, so no marker is missing, and `force_new_wft` needs a task
    /// token the Run does not have. Lang's wake sweep is the server-visible replacement, and Core
    /// reimplementing it here would send a Signal for a Run that is about to be finalized anyway.
    pub(super) fn external_stream_shutdown(&mut self) -> RunUpdateAct {
        if !self.begin_external_stream_teardown() {
            return None;
        }
        let res = self._check_more_activations();
        self.update_to_acts(res.map(Into::into))
    }

    /// Records that this Run's open Workflow Task must be closed because the Run is going away.
    ///
    /// Returns whether this Run is in the state ADR-009's first row is about. The classification
    /// is the same one lang's shutdown sweep gets from `external_stream_run_status`, on purpose:
    /// the two halves must agree about which Run is in which state, or a Run would be swept by
    /// both mechanisms or by neither.
    fn begin_external_stream_teardown(&mut self) -> bool {
        // `wft` is Core's own record of holding the task; the probe additionally distinguishes a
        // parked set, which holds no task open even though the Run may still be cached.
        if self.wft.is_none()
            || (!self.waiting_on_local_work.output_buffered
                && !matches!(
                    self.external_stream_run_status(),
                    ExternalStreamRunStatus::WftOpen
                ))
        {
            return false;
        }
        self.cancel_external_output_flush_timer();
        self.waiting_on_local_work.shutdown_pending = true;
        true
    }

    /// Moves the complete set into `Parking` and asks lang to run the backend handshake (C8).
    ///
    /// Runtime-internal: no user Workflow code runs for this job. Lang installs one park intent
    /// per subscription, rechecks every stream, and answers `ExternalStreamParkResult` --
    /// `ParkSetConfirmed` carrying the terminal only Core cannot encode, or `StreamSetBecameReady`
    /// abandoning this parking generation.
    ///
    /// Returns `true` when a job was issued. `false` means the set refused to park: the generation
    /// named is no longer current, or readiness Core already accepted is sitting in it, and
    /// parking a set with a ready wait in it would strand that wait's record until a producer
    /// happened to signal.
    fn begin_external_stream_park(
        &mut self,
        quiescence_generation: u64,
        trigger: ParkTrigger,
    ) -> bool {
        if self.waiting_on_local_work.pending_park.is_some() {
            // One handshake at a time. A second job for one set would put two runtime-internal
            // activations in flight, and there is never more than one outstanding per run.
            return false;
        }
        let set = &mut self.waiting_on_local_work.external_wait_set;
        let reason = match set.start_parking(quiescence_generation, trigger) {
            ParkStartOutcome::Started(ParkTrigger::IdleTimeout) => ParkReason::Idle,
            ParkStartOutcome::Started(ParkTrigger::AllWriteFenced) => ParkReason::AllWriteFenced,
            ParkStartOutcome::StaleGeneration | ParkStartOutcome::AlreadyReady => return false,
        };
        let waits = set
            .wait_snapshot()
            .into_iter()
            .map(
                |(wait_id, generation, immediately_parkable)| ExternalStreamWait {
                    wait_id,
                    generation,
                    immediately_parkable,
                },
            )
            .collect();

        // The quiescent snapshot this park closes is over. Its idle timer must not survive the
        // handshake: firing again would start a second park for a set already in one.
        self.cancel_external_stream_idle_timer();
        self.waiting_on_local_work.pending_park = Some(PendingPark::Queued(reason));
        self.wfm.machines.send_core_generated_job(
            workflow_activation_job::Variant::PrepareExternalStreamPark(
                PrepareExternalStreamPark {
                    quiescence_generation,
                    waits,
                    reason: reason as i32,
                },
            ),
        );
        true
    }

    /// Applies lang's answer to `PrepareExternalStreamPark`.
    ///
    /// Both orderings of the readiness/park race resolve here, through the one pure function that
    /// decides them: readiness accepted while the handshake was in flight has already moved a wait
    /// out of `Parking`, and that is exactly what makes the confirmation which follows stale.
    fn apply_park_result(
        &mut self,
        reason: ParkReason,
        result: ExternalStreamParkResult,
    ) -> ParkApplication {
        let confirmed = matches!(
            result.outcome,
            Some(external_stream_park_result::Outcome::Confirmed(_))
        );
        let set = &mut self.waiting_on_local_work.external_wait_set;
        match set.resolve_park(result.quiescence_generation, confirmed) {
            ParkResolution::Confirmed => {
                // The terminal arrives *here*, on the park result -- park issues no finalization
                // job, so this is the only chance Core gets to obtain it before writing.
                set.accumulate_annotation(&result.final_observation_delta);
                ParkApplication::Confirmed(reason)
            }
            ParkResolution::Aborted => {
                // The recheck found records. No boundary was reached, so nothing is written; lang
                // is resumed by a *normal* resolve activation rather than by user code running
                // from inside the park path.
                set.mark_all_ready_after_aborted_park();
                ParkApplication::Aborted
            }
            // Readiness beat the confirmation, or a later snapshot replaced the one being parked.
            // Discarded with no effect and, above all, no marker: the boundary it claims to close
            // was never reached.
            ParkResolution::StaleGeneration => ParkApplication::Stale,
        }
    }

    fn abort_external_stream_park_for_boundary(&mut self) {
        self.waiting_on_local_work
            .external_wait_set
            .abort_park_for_boundary();
        self.waiting_on_local_work.park_rollback_resolve_pending = true;
    }

    /// Records a quiescent snapshot, and nothing else.
    ///
    /// This is the *registration* half of quiescence, and it exists apart from the retaining half
    /// because they answer different questions. What lang reports here is the set of subscriptions
    /// the Workflow is blocked on, and Core is the only place that set is recorded -- a completion
    /// that also produced a timer must be reported to the server, but the Workflow is no less
    /// blocked on those streams for it, and a Run whose waits were never registered can never be
    /// woken: readiness has no wait to resolve against and a wake Signal marks nothing ready.
    ///
    /// Nothing is armed here. No idle timer, no rollover deadline, and no all-fenced immediate
    /// park -- each of those exists to end a *retained* Workflow Task, and a task that is about to
    /// be reported, or that is being replayed, has no retention for them to end.
    ///
    /// Returns the new quiescence generation and the clamped idle timeout the snapshot was
    /// recorded with, so the retaining path can arm its deadlines against the same values.
    fn register_external_stream_quiescence(
        &mut self,
        request: QuiescenceRequest,
    ) -> (u64, Duration) {
        let idle_timeout = clamp_idle_below_rollover(request.idle_timeout, self.wft_timeout());
        let generation = self
            .waiting_on_local_work
            .external_wait_set
            .become_quiescent(request.waits, idle_timeout);
        (generation, idle_timeout)
    }

    /// Records a quiescent snapshot and starts the timers that bound it.
    ///
    /// **One** idle timer for the whole wait set, not one per subscription: the timeout measures
    /// *global* quiescence, so an idle stream cannot park a workflow task another stream is still
    /// driving.
    fn begin_external_stream_quiescence(&mut self, request: QuiescenceRequest) {
        // The rollover deadline is what stops a continuously fed stream -- one whose gaps never
        // reach the idle timeout -- from holding the task until it *fails*.
        //
        // It is anchored at the Workflow Task's start, not at this snapshot. Becoming quiescent
        // again is what a delivered record *does*, so re-anchoring here would push the deadline
        // out for as long as records keep arriving -- and that is exactly the workload rollover
        // exists for. The deadline would then never fire, the idle timer is clamped below it and
        // cannot fire either, and the retained task would run until the server timed it out.
        if let Some(wft_timeout) = self.wft_timeout() {
            let started_at = self.current_wft_start_time();
            self.start_wft_rollover_timer(started_at, wft_timeout);
        }

        let (generation, idle_timeout) = self.register_external_stream_quiescence(request);

        self.cancel_external_stream_idle_timer();

        // Every wait reached a write fence with no later record immediately available, so there is
        // nothing left for the idle delay to wait *for*: park now instead of holding the task open
        // for a timeout that can only expire. One fenced stream does not qualify -- parking is
        // all-or-nothing across the set -- and a set holding accepted readiness refuses, which is
        // what leaves the resolve below reachable.
        if self
            .waiting_on_local_work
            .external_wait_set
            .all_immediately_parkable()
            && self.begin_external_stream_park(generation, ParkTrigger::AllWriteFenced)
        {
            return;
        }

        self.waiting_on_local_work.idle_timer = Some(self.run_timers.start(
            Instant::now().add(idle_timeout),
            LocalInputs::ExternalStreamIdleTimeout(ExternalStreamIdleTimeoutMsg {
                run_id: self.wfm.machines.run_id.clone(),
                quiescence_generation: generation,
            }),
        ));

        // Readiness that arrived while lang was computing this snapshot survives it when the
        // generations match, and must still reach lang.
        self.maybe_issue_external_stream_resolve();
    }

    /// Re-arms the idle and rollover deadlines for a wait set carried onto a replacement task.
    ///
    /// The quiescent snapshot is *not* renewed: the same generation continues, so a readiness
    /// notification in flight across the rollover still matches and is not lost.
    fn restart_external_stream_deadlines(&mut self, start_time: Instant) {
        let wft_timeout = self.wft_timeout();
        if let Some(wft_timeout) = wft_timeout {
            self.start_wft_rollover_timer(start_time, wft_timeout);
        }
        let idle_timeout = clamp_idle_below_rollover(
            self.waiting_on_local_work
                .external_wait_set
                .idle_timeout()
                .unwrap_or(Duration::from_secs(1)),
            wft_timeout,
        );
        let generation = self
            .waiting_on_local_work
            .external_wait_set
            .quiescence_generation();
        self.cancel_external_stream_idle_timer();
        self.waiting_on_local_work.idle_timer = Some(self.run_timers.start(
            start_time.add(idle_timeout),
            LocalInputs::ExternalStreamIdleTimeout(ExternalStreamIdleTimeoutMsg {
                run_id: self.wfm.machines.run_id.clone(),
                quiescence_generation: generation,
            }),
        ));
    }

    fn cancel_external_stream_idle_timer(&mut self) {
        if let Some(handle) = self.waiting_on_local_work.idle_timer.take() {
            handle.abort();
        }
    }

    fn note_external_output_buffered(&mut self, max_publish_latency: Duration) {
        self.waiting_on_local_work.output_buffered = true;

        if let Some(wft_timeout) = self.wft_timeout() {
            self.start_wft_rollover_timer(self.current_wft_start_time(), wft_timeout);
        }

        let deadline = Instant::now().add(max_publish_latency);
        if self
            .waiting_on_local_work
            .output_flush_timer
            .as_ref()
            .is_some_and(|timer| timer.deadline <= deadline)
        {
            return;
        }
        self.cancel_external_output_flush_timer();
        self.waiting_on_local_work.output_flush_timer = Some(OutputFlushTimer {
            deadline,
            handle: self.run_timers.start(
                deadline,
                LocalInputs::ExternalOutputFlushDeadline(self.wfm.machines.run_id.clone()),
            ),
        });
    }

    fn cancel_external_output_flush_timer(&mut self) {
        if let Some(timer) = self.waiting_on_local_work.output_flush_timer.take() {
            timer.handle.abort();
        }
    }

    fn clear_external_output_buffered(&mut self) {
        self.cancel_external_output_flush_timer();
        self.waiting_on_local_work.output_buffered = false;
    }

    // --- Run-level timers --------------------------------------------------

    /// The local-activity heartbeat deadline, now scheduled through the run's own timer facility.
    ///
    /// A static so it can be called while `self.waiting_on_local_work` is mutably borrowed. The
    /// deadline itself is unchanged: 80% of the workflow task timeout.
    fn start_la_heartbeat_timeout_with(
        timers: &RunTimerSink,
        run_id: &str,
        wft_start_time: Instant,
        wft_timeout: Duration,
    ) -> AbortHandle {
        let deadline = wft_start_time.add(wft_timeout.mul_f32(WFT_HEARTBEAT_TIMEOUT_FRACTION));
        timers.start(deadline, LocalInputs::HeartbeatTimeout(run_id.to_string()))
    }

    /// Starts (or restarts) this run's workflow task rollover deadline.
    ///
    /// A retained workflow task is bounded by the server's workflow task timeout, so a
    /// continuously fed stream whose gaps stay below the idle timeout would otherwise hold the
    /// task until it *fails* rather than merely being held too long.
    #[allow(dead_code, reason = "started by the retention path in C6")]
    pub(super) fn start_wft_rollover_timer(
        &mut self,
        wft_start_time: Instant,
        wft_timeout: Duration,
    ) {
        self.cancel_wft_rollover_timer();
        let deadline = wft_start_time.add(wft_timeout.mul_f32(WFT_HEARTBEAT_TIMEOUT_FRACTION));
        self.waiting_on_local_work.wft_rollover_timer = Some(self.run_timers.start(
            deadline,
            LocalInputs::WftRolloverDeadline(self.wfm.machines.run_id.clone()),
        ));
    }

    #[allow(dead_code, reason = "cancelled by the retention path in C6")]
    pub(super) fn cancel_wft_rollover_timer(&mut self) {
        if let Some(handle) = self.waiting_on_local_work.wft_rollover_timer.take() {
            handle.abort();
        }
    }

    /// The rollover deadline expired.
    ///
    /// Records that the next completion must request a replacement task, and autocompletes if
    /// there is no activation outstanding to carry it. Preserving every subscription, cursor, and
    /// readiness generation across that replacement is C12a's.
    pub(super) fn wft_rollover_deadline(&mut self) -> RunUpdateAct {
        self.waiting_on_local_work.wft_rollover_timer = None;
        self.waiting_on_local_work.deadline_rollover_pending = true;
        let maybe_act = if self.activation.is_none() && self.wft.is_some() {
            Some(ActivationOrAuto::Autocomplete {
                run_id: self.wfm.machines.run_id.clone(),
            })
        } else {
            None
        };
        self.update_to_acts(Ok(maybe_act.into()))
    }

    /// The earliest max-publish-latency deadline for buffered output expired.
    pub(super) fn external_output_flush_deadline(&mut self) -> RunUpdateAct {
        self.waiting_on_local_work.output_flush_timer = None;
        if !self.waiting_on_local_work.output_buffered || self.wft.is_none() {
            return None;
        }

        if self.activation.is_some() {
            self.waiting_on_local_work.output_latency_pending = true;
            return None;
        }

        if !self.begin_external_stream_finalization(ParkReason::OutputLatency) {
            return None;
        }
        let res = self._check_more_activations();
        self.update_to_acts(res.map(Into::into))
    }

    // --- External Workflow Streams -----------------------------------------

    /// A watcher reports a record is buffered for one wait.
    ///
    /// Returns the acknowledgement alongside any activation, because the watcher's next action
    /// depends on which of the five results this was and only the wait set can say.
    pub(super) fn external_stream_ready(
        &mut self,
        wait_id: u32,
        wait_generation: u64,
    ) -> (ExternalStreamReadyResult, RunUpdateAct) {
        let outcome = self
            .waiting_on_local_work
            .external_wait_set
            .notify_ready(wait_id, wait_generation);

        if outcome != ReadinessOutcome::Accepted {
            return (outcome.into(), None);
        }

        // Readiness ends this quiescent snapshot, so the timer measuring it must not survive it.
        // The rollover deadline is *not* cancelled: it bounds the workflow task itself, which is
        // still open and still being held.
        self.cancel_external_stream_idle_timer();

        // `_check_more_activations` is what turns pending readiness into an activation, and it
        // does so whether readiness arrived now or while an earlier activation was outstanding --
        // so both orderings coalesce through one path.
        let act = match self._check_more_activations() {
            Ok(act) => self.update_to_acts(Ok(act.into())),
            Err(err) => self.update_to_acts(Err(err)),
        };
        (outcome.into(), act)
    }

    /// Classifies the wake Signals the machines decoded out of this task's history.
    ///
    /// Returns `true` if any of them should wake the Run. Every one is suppressed from user
    /// handlers regardless -- that already happened in the machines -- so what is decided here is
    /// only whether the Run resumes.
    fn apply_external_stream_wakes(&mut self) -> bool {
        let wakes = self.wfm.machines.take_external_stream_wakes();
        if wakes.is_empty() {
            return false;
        }
        let chain = self
            .wfm
            .machines
            .get_started_info()
            .map(|info| info.first_execution_run_id.clone())
            .unwrap_or_default();

        let mut resume = false;
        for wake in wakes {
            // Chain identity, not Run identity. The Signal is addressed to the Workflow ID
            // without a Run ID, so it always lands on the current Run of the chain -- and a
            // Signal from a *different* chain is a mis-addressed message, not a stale one.
            //
            // Compared strictly, including when this run's chain id is unknown: a Signal naming
            // a chain we cannot confirm is ours is exactly the case that must not be honoured.
            if wake.first_execution_run_id != chain {
                debug!(
                    signalled_chain = %wake.first_execution_run_id,
                    "Rejecting an external stream wake Signal for a different chain"
                );
                continue;
            }
            if !self
                .waiting_on_local_work
                .external_wait_set
                .accepts_wake_generation(wake.park_generation)
            {
                // A *non-zero* generation this Run does not recognise is a claim that turned out
                // to be wrong. Generation 0 is never rejected here: it is the unparked wake, and
                // an unnecessary one costs at most one empty Workflow Task.
                debug!(
                    park_generation = wake.park_generation,
                    "Ignoring a stale external stream wake Signal"
                );
                continue;
            }
            resume = true;
        }

        if resume {
            // The Signal names one stream, but it is only a hint: every active wait is marked so
            // lang rechecks all of them on wakeup. A wake for a stream that turns out to have
            // nothing costs one empty drain, and missing one costs a stalled Workflow.
            self.waiting_on_local_work
                .external_wait_set
                .mark_all_ready_for_wake();
        }
        resume
    }

    /// Queues one `ResolveExternalStreamWaits` if readiness is pending and a task can carry it.
    ///
    /// Coalescing lives here rather than at the notification: every wait known ready ships in one
    /// activation, and notifications arriving while an activation is outstanding accumulate for
    /// the next one. There is never more than one outstanding activation per run.
    fn maybe_issue_external_stream_resolve(&mut self) {
        if self.activation.is_some() {
            return;
        }
        self.queue_external_stream_resolve(false);
    }

    /// Queues the job without asking whether an activation is outstanding.
    ///
    /// Two callers are legal: the readiness path once no activation is outstanding, and the
    /// completion path of the outstanding activation, after `apply_next_task_if_ready` and before
    /// `prepare_complete_resp` picks the pending jobs up. Lang has finished that activation, so
    /// the job lands on the next one. From anywhere else the job would ride an activation lang is
    /// still working on, which breaks the one-outstanding-activation rule this run relies on.
    /// `completing_outstanding_activation` is the caller saying which of the two it is.
    fn queue_external_stream_resolve(&mut self, completing_outstanding_activation: bool) {
        // Violating this reorders activations rather than crashing, so a release build has to say
        // so as well; `debug_assert!` alone would leave it silent everywhere it matters.
        if completing_outstanding_activation != self.activation.is_some() {
            dbg_panic!("external stream resolve queued outside the readiness and completion paths");
        }
        if self.wft.is_none() || self.am_broken {
            return;
        }
        let set = &mut self.waiting_on_local_work.external_wait_set;
        if !set.has_pending_readiness() {
            return;
        }
        let quiescence_generation = set.quiescence_generation();
        let ready_hints = set
            .take_ready_wait_ids()
            .into_iter()
            .filter_map(|wait_id| {
                set.wait(wait_id).map(|w| ExternalStreamWait {
                    wait_id: w.wait_id,
                    generation: w.wait_generation,
                    immediately_parkable: w.immediately_parkable,
                })
            })
            .collect();
        self.wfm.machines.send_core_generated_job(
            workflow_activation_job::Variant::ResolveExternalStreamWaits(
                ResolveExternalStreamWaits {
                    quiescence_generation,
                    ready_hints,
                },
            ),
        );
    }

    /// Test scaffolding -- see [`ExternalStreamSeedWaitsMsg`].
    #[cfg(test)]
    pub(super) fn seed_external_wait_set(&mut self, msg: &super::ExternalStreamSeedWaitsMsg) {
        let set = &mut self.waiting_on_local_work.external_wait_set;
        set.become_quiescent(
            msg.wait_ids
                .iter()
                .map(|id| super::external_streams::ExternalWaitState::new(*id, 0, false)),
            msg.idle_timeout,
        );
        if let Some(generation) = msg.parked_at {
            set.force_parked(generation);
        } else {
            set.set_wft_open(msg.wft_open);
        }
    }

    /// Test scaffolding -- see [`super::EmitTerminalLessMarkerMsg`].
    #[cfg(test)]
    pub(super) fn emit_terminal_less_marker(&mut self) -> bool {
        self.wfm
            .machines
            .emit_external_stream_marker(ExternalStreamMarkerData {
                schema_version: EXTERNAL_STREAM_MARKER_SCHEMA_VERSION,
                quiescence_generation: 1,
                waits: vec![],
                replay_annotation: b"no terminal here".to_vec(),
                terminal_boundary: ParkReason::Unspecified as i32,
                output: None,
            })
            .is_err()
    }

    /// The accumulated, unwritten replay annotation. Core never parses it.
    #[cfg(test)]
    pub(super) fn external_stream_annotation(&self) -> &[u8] {
        self.waiting_on_local_work
            .external_wait_set
            .replay_annotation()
    }

    /// The read-only status probe. Must leave the run exactly as it was.
    pub(super) fn external_stream_run_status(&self) -> ExternalStreamRunStatus {
        if self.waiting_on_local_work.external_wait_set.is_empty() {
            // No waits registered at all. The run is cached but holds nothing this probe is
            // about, which for the sweep is the same instruction as "no open Workflow Task".
            return ExternalStreamRunStatus::NoOpenWorkflowTask;
        }
        self.waiting_on_local_work
            .external_wait_set
            .run_status()
            .into()
    }

    /// The global quiescence timer for `quiescence_generation` expired.
    ///
    /// Nothing was delivered on any active wait for the whole timeout, so the complete set is
    /// asked to park. A timer for a snapshot the Workflow has already run past changes nothing.
    pub(super) fn external_stream_idle_timeout(
        &mut self,
        quiescence_generation: u64,
    ) -> RunUpdateAct {
        // The handle is spent: this *is* its expiry.
        self.waiting_on_local_work.idle_timer = None;
        if !self.begin_external_stream_park(quiescence_generation, ParkTrigger::IdleTimeout) {
            return None;
        }
        let res = self._check_more_activations();
        self.update_to_acts(res.map(Into::into))
    }

    pub(super) fn heartbeat_timeout(&mut self) -> RunUpdateAct {
        let maybe_act = if self._heartbeat_timeout() {
            Some(ActivationOrAuto::Autocomplete {
                run_id: self.wfm.machines.run_id.clone(),
            })
        } else {
            None
        };
        self.update_to_acts(Ok(maybe_act.into()))
    }
    /// Returns `true` if autocompletion should be issued to report the heartbeat WFT completion.
    fn _heartbeat_timeout(&mut self) -> bool {
        if let Some(ref mut wait_dat) = self.waiting_on_local_work.local_activities {
            wait_dat.hb_timeout_handle.abort();
            wait_dat.heartbeat_timeout_pending = true;
            return self.activation.is_none();
        }
        false
    }

    /// Returns true if the managed run has any form of pending work
    /// If `ignore_evicts` is true, pending evictions do not count as pending work.
    /// If `ignore_buffered` is true, buffered workflow tasks do not count as pending work.
    pub(super) fn has_any_pending_work(&self, ignore_evicts: bool, ignore_buffered: bool) -> bool {
        let evict_work = if ignore_evicts {
            false
        } else {
            self.trying_to_evict.is_some()
        };
        let act_work = if ignore_evicts {
            self.activation
                .map(|a| !matches!(a, OutstandingActivation::Eviction))
                .unwrap_or_default()
        } else {
            self.activation.is_some()
        };
        let buffered = if ignore_buffered {
            false
        } else {
            self.task_buffer.has_tasks()
        };
        trace!(wft=self.wft.is_some(), buffered=?buffered, more_work=?self.more_pending_work(),
               act_work, evict_work, "Does run have pending work?");
        self.wft.is_some() || buffered || self.more_pending_work() || act_work || evict_work
    }

    /// Stores some work if there is any outstanding WFT or activation for the run. If there was
    /// not, returns the work back out inside the option.
    pub(super) fn buffer_wft_if_outstanding_work(
        &mut self,
        work: PermittedWFT,
    ) -> Option<PermittedWFT> {
        let about_to_issue_evict = self.trying_to_evict.is_some();
        let has_activation = self.activation().is_some();
        if must_buffer_wft(
            self.wft.is_some(),
            has_activation,
            about_to_issue_evict,
            self.more_pending_work(),
        ) {
            debug!(run_id = %self.run_id(),
                   "Got new WFT for a run with outstanding work, buffering it act: {:?} wft: {:?} about to evict: {:?}", &self.activation(), &self.wft, about_to_issue_evict);
            self.task_buffer.buffer(work);
            None
        } else {
            Some(work)
        }
    }

    /// Returns true if there is a buffered workflow task for this run.
    pub(super) fn has_buffered_wft(&self) -> bool {
        self.task_buffer.has_tasks()
    }

    pub(super) fn request_eviction(&mut self, info: RequestEvictMsg) -> EvictionRequestResult {
        let attempts = self.wft.as_ref().map(|wt| wt.info.attempt);

        // If we were waiting on a page fetch and we're getting evicted because fetching failed,
        // then make sure we allow the completion to proceed, otherwise we're stuck waiting forever.
        if self.completion_waiting_on_page_fetch.is_some()
            && matches!(info.reason, EvictionReason::PaginationOrHistoryFetch)
        {
            // We just checked it is some, unwrap OK.
            let c = self.completion_waiting_on_page_fetch.take().unwrap();
            let run_upd = self.failed_completion(
                WorkflowTaskFailedCause::WorkflowWorkerUnhandledFailure,
                info.reason,
                Failure::application_failure(info.message, false).into(),
                true,
                c.resp_chan,
            );
            return EvictionRequestResult::EvictionRequested(attempts, run_upd);
        }

        if !self.activation_is_eviction() && self.trying_to_evict.is_none() {
            let outstanding_las = self.wfm.machines.outstanding_local_activity_count();
            if outstanding_las > 0 && self.config.max_cached_workflows == 0 {
                warn!(
                    run_id=%info.run_id,
                    reason=?info.reason,
                    outstanding_local_activities=outstanding_las,
                    "Eviction requested while local activities are still in flight; local activities when using max_cached_workflows=0 are likely to be dropped or retried"
                );
            }
            debug!(run_id=%info.run_id, reason=%info.message, "Eviction requested");
            // If we've requested an eviction because of failure related reasons then we want to
            // delete any pending queries, since handling them no longer makes sense. Evictions
            // because the cache is full should get a chance to finish processing properly.
            if !matches!(info.reason, EvictionReason::CacheFull | EvictionReason::WorkflowExecutionEnding)
                // If the wft was just a legacy query, still reply, otherwise we might try to
                // reply to the task as if it were a task rather than a query.
                && !self.pending_work_is_legacy_query()
                && let Some(wft) = self.wft.as_mut()
            {
                wft.pending_queries.clear();
            }

            self.trying_to_evict = Some(info);
            EvictionRequestResult::EvictionRequested(attempts, self.check_more_activations())
        } else {
            // Always store the most recent eviction reason
            self.trying_to_evict = Some(info);
            EvictionRequestResult::EvictionAlreadyRequested(attempts)
        }
    }

    pub(super) fn record_span_fields(&mut self, span: &Span) {
        if let Some(spid) = span.id() {
            if self.recorded_span_ids.contains(&spid) {
                return;
            }
            self.recorded_span_ids.insert(spid);

            span.record("run_id", self.run_id());
            if let Some(wid) = self.wft().map(|wft| &wft.info.wf_id) {
                span.record("workflow_id", wid.as_str());
            }
        }
    }

    /// Take the result of some update to ourselves and turn it into a return value of zero or more
    /// actions
    fn update_to_acts(&mut self, outcome: Result<ActOrFulfill, RunUpdateErr>) -> RunUpdateAct {
        match outcome {
            Ok(act_or_fulfill) => {
                let (mut maybe_act, maybe_fulfill) = match act_or_fulfill {
                    ActOrFulfill::OutgoingAct(a) => (a, None),
                    ActOrFulfill::FulfillableComplete(c) => (None, c),
                };
                // If there's no activation but is pending work, check and possibly generate one
                if self.more_pending_work() && maybe_act.is_none() {
                    match self._check_more_activations() {
                        Ok(oa) => maybe_act = oa,
                        Err(e) => {
                            return self.update_to_acts(Err(e));
                        }
                    }
                }
                let r = match maybe_act {
                    Some(ActivationOrAuto::LangActivation(activation)) => {
                        if activation.jobs.is_empty() {
                            dbg_panic!("Should not send lang activation with no jobs");
                        }
                        Some(ActivationOrAuto::LangActivation(activation))
                    }
                    Some(ActivationOrAuto::ReadyForQueries(mut act)) => {
                        if let Some(wft) = self.wft.as_mut() {
                            put_queries_in_act(&mut act, wft);
                            Some(ActivationOrAuto::LangActivation(act))
                        } else {
                            dbg_panic!("Ready for queries but no WFT!");
                            None
                        }
                    }
                    a @ Some(
                        ActivationOrAuto::Autocomplete { .. } | ActivationOrAuto::AutoFail { .. },
                    ) => a,
                    None => {
                        if let Some(reason) = self.trying_to_evict.as_ref() {
                            // If we had nothing to do, but we're trying to evict, just do that now
                            // as long as there's no other outstanding work.
                            if self.activation.is_none() && !self.more_pending_work() {
                                let mut evict_act = create_evict_activation(
                                    self.run_id().to_string(),
                                    reason.message.clone(),
                                    reason.reason,
                                );
                                evict_act.history_length =
                                    self.most_recently_processed_event_number() as u32;
                                Some(ActivationOrAuto::LangActivation(evict_act))
                            } else {
                                None
                            }
                        } else {
                            None
                        }
                    }
                };
                if let Some(f) = maybe_fulfill {
                    f.fulfill();
                }

                match r {
                    // After each run update, check if it's ready to handle any buffered task
                    None | Some(ActivationOrAuto::Autocomplete { .. })
                        if !self.has_any_pending_work(false, true) =>
                    {
                        if let Some(bufft) = self.task_buffer.get_next_wft() {
                            self.incoming_wft(bufft)
                        } else {
                            None
                        }
                    }
                    Some(r) => {
                        self.insert_outstanding_activation(&r);
                        Some(r)
                    }
                    None => None,
                }
            }
            Err(fail) => {
                self.am_broken = true;

                if let Some(resp_chan) = fail.complete_resp_chan {
                    // Automatically fail the workflow task in the event we couldn't update machines
                    let fail_cause = if matches!(&fail.source, WFMachinesError::Nondeterminism(_)) {
                        WorkflowTaskFailedCause::NonDeterministicError
                    } else {
                        WorkflowTaskFailedCause::WorkflowWorkerUnhandledFailure
                    };
                    self.failed_completion(
                        fail_cause,
                        fail.source.evict_reason(),
                        fail.source.as_failure(),
                        true,
                        Some(resp_chan),
                    )
                } else {
                    warn!(error=?fail.source, "Error while updating workflow");
                    Some(ActivationOrAuto::AutoFail {
                        run_id: self.run_id().to_owned(),
                        machines_err: fail.source,
                    })
                }
            }
        }
    }

    fn insert_outstanding_activation(&mut self, act: &ActivationOrAuto) {
        let act_type = match &act {
            ActivationOrAuto::LangActivation(act) | ActivationOrAuto::ReadyForQueries(act) => {
                if act.is_only_eviction() {
                    OutstandingActivation::Eviction
                } else if act.is_legacy_query() {
                    OutstandingActivation::LegacyQuery
                } else {
                    OutstandingActivation::Normal
                }
            }
            ActivationOrAuto::Autocomplete { .. } | ActivationOrAuto::AutoFail { .. } => {
                OutstandingActivation::Autocomplete
            }
        };
        if let Some(old_act) = self.activation {
            // This is a panic because we have screwed up core logic if this is violated. It must be
            // upheld.
            panic!(
                "Attempted to insert a new outstanding activation {act:?}, but there already was \
                 one outstanding: {old_act:?}"
            );
        }
        // A normal activation drains every queued job, so a park handshake that was waiting for
        // one is now in lang's hands -- and the completion answering *this* activation is the one
        // that owes Core the park result. An eviction or autocomplete drains nothing, so a job
        // queued behind it is still only queued.
        if matches!(act_type, OutstandingActivation::Normal)
            && let Some(PendingPark::Queued(reason)) = self.waiting_on_local_work.pending_park
        {
            self.waiting_on_local_work.pending_park = Some(PendingPark::Issued(reason));
        }
        self.activation = Some(act_type);
    }

    fn prepare_complete_resp(
        &mut self,
        resp_chan: Option<oneshot::Sender<ActivationCompleteResult>>,
        data: CompletionDataForWFT,
        due_to_heartbeat_timeout: bool,
    ) -> FulfillableActivationComplete {
        let mut machines_wft_response = self.wfm.prepare_for_wft_response();
        if data.activation_was_eviction
            && (machines_wft_response.commands().peek().is_some()
                || machines_wft_response.has_messages())
            && !self.am_broken
        {
            dbg_panic!(
                "There should not be any outgoing commands or messages when preparing a completion \
                 response if the activation was only an eviction. This is an SDK bug."
            );
        }

        let query_responses = data.query_responses;
        let has_query_responses = !query_responses.is_empty();
        let is_query_playback = data.has_pending_query && !has_query_responses;
        let mut force_new_wft = due_to_heartbeat_timeout;

        // We only actually want to send commands back to the server if there are no more pending
        // activations and we are caught up on replay. We don't want to complete a wft if we already
        // saw the final event in the workflow, or if we are playing back for the express purpose of
        // fulfilling a query. If the activation we sent was *only* an eviction, don't send that
        // either.
        let should_respond = !(machines_wft_response.has_pending_jobs
            || (machines_wft_response.replaying && !data.is_forced_failure)
            || is_query_playback
            || data.activation_was_eviction
            || machines_wft_response.have_seen_terminal_event);
        // If there are pending LA resolutions, and we're responding to a query here,
        // we want to make sure to force a new task, as otherwise once we tell lang about
        // the LA resolution there wouldn't be any task to reply to with the result of iterating
        // the workflow.
        if has_query_responses && machines_wft_response.have_pending_la_resolutions {
            force_new_wft = true;
        }

        // Reporting the task ends local delivery, and it ends it *here* rather than at
        // `mark_wft_complete`, which runs only once the server has answered. `Accepted` promises
        // the watcher that Core will activate, and a task on its way to the server can no longer
        // carry that activation: from this instant readiness is answered `NoOpenWorkflowTask`
        // instead, which is the one answer that tells the watcher to send the wake Signal itself.
        //
        // Readiness already accepted against this task has no such fallback -- its watcher was
        // told to do nothing and will not report again -- and the completion paths that reach
        // here never turn it into a job: a snapshot that registers without retaining (a replaying
        // completion, one carrying server-bound commands, or one answering a query) records the
        // wait set and reports the task, and `_check_more_activations` is not on that path. So the
        // replacement task is asked for explicitly, and `apply_new_wft` re-opens the wait set on
        // it and issues the resolve job the pending readiness was promised. Without it the record
        // stays buffered behind a Run that Core believes it has already told.
        if should_respond || has_query_responses {
            let waits = &mut self.waiting_on_local_work.external_wait_set;
            waits.set_wft_open(false);
            if waits.has_pending_readiness() {
                force_new_wft = true;
            }
        }

        let outcome = if should_respond || has_query_responses {
            // If we broke there could be commands or messages in the pipe that we didn't
            // get a chance to handle properly during replay. Don't send them.
            let (commands, messages) = if self.am_broken && data.activation_was_eviction {
                (vec![], vec![])
            } else {
                (
                    machines_wft_response.commands().collect(),
                    machines_wft_response.messages(),
                )
            };

            let attempt = self.wft.as_ref().map(|t| t.info.attempt).unwrap_or(1);
            ActivationCompleteOutcome::ReportWFTSuccess(ServerCommandsWithWorkflowInfo {
                task_token: data.task_token,
                action: ActivationAction::WftComplete {
                    force_new_wft,
                    commands,
                    messages,
                    query_responses,
                    sdk_metadata: machines_wft_response.metadata_for_complete(),
                    versioning_behavior: data.versioning_behavior,
                    attempt,
                },
                metrics: self.metrics.clone(),
            })
        } else {
            ActivationCompleteOutcome::DoNothing
        };
        FulfillableActivationComplete {
            result: ActivationCompleteResult {
                outcome,
                replaying: machines_wft_response.replaying,
            },
            resp_chan,
        }
    }

    /// Pump some local activity requests into the sink, applying any immediate results to the
    /// workflow machines.
    fn sink_la_requests(
        &mut self,
        new_local_acts: Vec<LocalActRequest>,
    ) -> Result<(), WFMachinesError> {
        let immediate_resolutions =
            if let Some(ref local_act_request_sink) = self.local_activity_request_sink {
                local_act_request_sink.sink_reqs(new_local_acts)
            } else {
                Vec::new()
            };
        for resolution in immediate_resolutions {
            self.wfm
                .notify_of_local_result(LocalResolution::LocalActivity(resolution))?;
        }
        Ok(())
    }

    fn reply_to_complete(
        &mut self,
        outcome: ActivationCompleteOutcome,
        chan: Option<oneshot::Sender<ActivationCompleteResult>>,
    ) {
        if let Some(chan) = chan
            && chan
                .send(ActivationCompleteResult {
                    outcome,
                    replaying: self.wfm.machines.replaying,
                })
                .is_err()
        {
            let warnstr = "The workflow task completer went missing! This likely indicates an \
                               SDK bug, please report."
                .to_string();
            warn!(run_id=%self.run_id(), "{}", warnstr);
            self.request_eviction(RequestEvictMsg {
                run_id: self.run_id().to_string(),
                message: warnstr,
                reason: EvictionReason::Fatal,
                auto_reply_fail_tt: None,
            });
        }
    }

    /// Returns true if the handle is currently processing a WFT which contains a legacy query.
    fn pending_work_is_legacy_query(&self) -> bool {
        // Either we know because there is a pending legacy query, or it's already been drained and
        // sent as an activation.
        matches!(self.activation, Some(OutstandingActivation::LegacyQuery))
            || self
                .wft
                .as_ref()
                .map(|t| t.has_pending_legacy_query())
                .unwrap_or_default()
    }

    fn most_recently_processed_event_number(&self) -> i64 {
        self.wfm.machines.last_processed_event
    }

    fn activation_is_eviction(&mut self) -> bool {
        self.activation
            .map(|a| matches!(a, OutstandingActivation::Eviction))
            .unwrap_or_default()
    }

    fn run_id(&self) -> &str {
        &self.wfm.machines.run_id
    }
}

// Construct a new command sequence with query responses removed, and any
// terminal responses removed, except for the first terminal response, which is
// placed at the end. Return new command sequence and query commands. Note that
// multiple coroutines may have generated a terminal command, leading to
// multiple terminal commands in the input to this function.
fn preprocess_command_sequence(commands: Vec<WFCommand>) -> (Vec<WFCommand>, Vec<QueryResult>) {
    let mut query_results = vec![];
    let mut terminals = vec![];

    let mut commands: Vec<_> = commands
        .into_iter()
        .filter_map(|c| {
            if let WFCommandVariant::QueryResponse(qr) = c.variant {
                query_results.push(qr);
                None
            } else if c.variant.is_terminal() {
                terminals.push(c);
                None
            } else {
                Some(c)
            }
        })
        .collect();
    if let Some(first_terminal) = terminals.into_iter().next() {
        commands.push(first_terminal);
    }
    (commands, query_results)
}

fn preprocess_command_sequence_old_behavior(
    commands: Vec<WFCommand>,
) -> (Vec<WFCommand>, Vec<QueryResult>) {
    let mut query_results = vec![];
    let mut seen_terminal = false;

    let commands: Vec<_> = commands
        .into_iter()
        .filter_map(|c| {
            if let WFCommandVariant::QueryResponse(qr) = c.variant {
                query_results.push(qr);
                None
            } else if seen_terminal {
                None
            } else {
                if c.variant.is_terminal() {
                    seen_terminal = true;
                }
                Some(c)
            }
        })
        .collect();
    (commands, query_results)
}

/// Drains pending queries from the workflow task and appends them to the activation's jobs
fn put_queries_in_act(act: &mut WorkflowActivation, wft: &mut OutstandingTask) {
    // Nothing to do if there are no pending queries
    if wft.pending_queries.is_empty() {
        return;
    }

    let has_legacy = wft.has_pending_legacy_query();
    // Cannot dispatch legacy query if there are any other jobs - which can happen if, ex, a local
    // activity resolves while we've gotten a legacy query after heartbeating.
    if has_legacy && !act.jobs.is_empty() {
        return;
    }

    debug!(queries=?wft.pending_queries, "Dispatching queries");
    let query_jobs = wft
        .pending_queries
        .drain(..)
        .map(|q| workflow_activation_job::Variant::QueryWorkflow(q).into());
    act.jobs.extend(query_jobs);
}

/// The annotation format version this Core writes.
///
/// Leads the marker envelope so a marker written by an older SDK stays readable.
const EXTERNAL_STREAM_MARKER_SCHEMA_VERSION: u32 = 1;

/// Where a park handshake is between Core deciding to park and lang answering.
///
/// The two states are not decoration: the completion that must carry an `ExternalStreamParkResult`
/// is the one that answers the activation *carrying* the job, and an activation can already be
/// outstanding when the set begins parking -- the all-fenced trigger fires from inside a
/// completion, and a timer can expire while lang is still working. Enforcing the pairing against
/// `Queued` would fail a completion for a job lang has not been handed yet.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PendingPark {
    /// The job is queued; lang has not been activated with it.
    Queued(ParkReason),
    /// Lang holds the job. The completion that answers it must carry the result.
    Issued(ParkReason),
}

/// What lang's `ExternalStreamParkResult` did to the wait set.
///
/// Three outcomes, not two, because a confirmation that lost the race with readiness is neither a
/// park nor an abort: nothing was parked and nothing was rechecked, so the completion must leave
/// the task exactly as it found it -- above all writing no marker for a boundary never reached.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ParkApplication {
    /// The set is parked. Carries the reason, which becomes the marker's terminal boundary.
    Confirmed(ParkReason),
    /// The final recheck found records; every wait is ready again.
    Aborted,
    /// The result named a generation that is no longer parking. Discarded.
    Stale,
}

/// Lang's `WorkflowStreamQuiescent`, validated.
struct QuiescenceRequest {
    waits: Vec<ExternalWaitState>,
    idle_timeout: Duration,
}

/// The external stream commands pulled out of a completion.
///
/// They are consumed above the state machines: progress accumulates into the wait set, and the
/// other three answer runtime-internal activations. Letting them reach the machines would drop a
/// replay-visible observation delta on the floor.
#[derive(Default)]
struct ExternalStreamCommands {
    quiescence: Option<QuiescenceRequest>,
    progress: Vec<WorkflowStreamProgress>,
    park_result: Option<ExternalStreamParkResult>,
    finalized: Option<ExternalStreamFinalized>,
    output_commit: Option<WorkflowOutputStreamCommit>,
    output_buffered_latency: Option<Duration>,
}

/// Splits lang's commands, leaving everything else in `commands`.
fn take_external_stream_commands(
    commands: &mut Vec<WFCommand>,
) -> Result<ExternalStreamCommands, WFMachinesError> {
    let mut taken = ExternalStreamCommands::default();
    let mut remaining = Vec::with_capacity(commands.len());
    let mut seen_other_command = false;
    for command in commands.drain(..) {
        match command.variant {
            WFCommandVariant::ExternalStreamQuiescent(q) => {
                let idle_timeout = q
                    .idle_timeout
                    .map(|d| d.try_into().unwrap_or(Duration::ZERO))
                    .unwrap_or(Duration::ZERO);
                if idle_timeout.is_zero() {
                    // Rejected rather than coerced: a zero or absent timeout would either park
                    // instantly or hold the task until it timed out, and neither is something a
                    // caller can have meant.
                    return Err(WFMachinesError::Fatal(
                        "WorkflowStreamQuiescent carried a non-positive idle timeout".to_string(),
                    ));
                }
                taken.quiescence = Some(QuiescenceRequest {
                    waits: q
                        .waits
                        .into_iter()
                        .map(|w| {
                            ExternalWaitState::new(w.wait_id, w.generation, w.immediately_parkable)
                        })
                        .collect(),
                    idle_timeout,
                });
            }
            WFCommandVariant::ExternalStreamProgress(p) => {
                // Ordering is normative, not stylistic. On replay this is what guarantees a
                // record's integrity is validated *before* the command derived from it is
                // matched; the other way round, a damaged stream would be discovered only after
                // its consequences had already been accepted as durable.
                if seen_other_command {
                    return Err(WFMachinesError::Fatal(
                        "WorkflowStreamProgress must precede every command whose value could \
                         depend on the consumed data, but one followed another command"
                            .to_string(),
                    ));
                }
                taken.progress.push(p);
            }
            WFCommandVariant::ExternalStreamParkResult(p) => taken.park_result = Some(p),
            WFCommandVariant::ExternalStreamFinalized(f) => taken.finalized = Some(f),
            WFCommandVariant::ExternalOutputStreamCommit(commit) => {
                if taken.output_commit.replace(commit).is_some() {
                    return Err(WFMachinesError::Fatal(
                        "Lang sent more than one WorkflowOutputStreamCommit in one completion"
                            .to_string(),
                    ));
                }
            }
            WFCommandVariant::ExternalOutputStreamBuffered(buffered) => {
                let max_publish_latency = buffered
                    .max_publish_latency
                    .map(|duration| duration.try_into().unwrap_or(Duration::ZERO))
                    .unwrap_or(Duration::ZERO);
                if max_publish_latency.is_zero() {
                    return Err(WFMachinesError::Fatal(
                        "WorkflowOutputStreamBuffered carried a non-positive max publish latency"
                            .to_string(),
                    ));
                }
                taken.output_buffered_latency = Some(
                    taken
                        .output_buffered_latency
                        .map_or(max_publish_latency, |current| {
                            current.min(max_publish_latency)
                        }),
                );
            }
            _ => {
                seen_other_command = true;
                remaining.push(command);
            }
        }
    }
    *commands = remaining;
    Ok(taken)
}

/// Keeps the idle deadline strictly inside the rollover deadline.
///
/// Rollover is authoritative: a retained task is bounded by the server's workflow task timeout
/// whatever the idle timeout says, so an idle timer allowed to outlast it would simply never fire.
fn clamp_idle_below_rollover(idle: Duration, wft_timeout: Option<Duration>) -> Duration {
    match wft_timeout {
        // The same fraction the rollover deadline uses, pulled in far enough that the idle timer
        // still gets a chance to fire first when it was configured to.
        Some(wft) => idle.min(wft.mul_f32(WFT_HEARTBEAT_TIMEOUT_FRACTION * 0.9)),
        None => idle,
    }
}

/// Tracks the heartbeat while a workflow task has outstanding local activities.
struct LocalActivityHeartbeatState {
    wft_timeout: Duration,
    /// Can be used to abort heartbeat timeouts
    hb_timeout_handle: AbortHandle,
    /// Defers the heartbeat when lang must finish an outstanding activation before Core can safely
    /// complete the workflow task.
    heartbeat_timeout_pending: bool,
}

struct OutputFlushTimer {
    deadline: Instant,
    handle: AbortHandle,
}

/// Local work that may retain the open workflow task.
///
/// Retention used to be a local-activity concept, expressed as `Option<WaitingOnLAs>` and keyed
/// off `outstanding_local_activity_count()`. The broader per-run concept is *local work that may
/// retain the workflow task*, of which outstanding local activities are one kind and an external
/// stream wait set is another, so the two can be asked the same question without either knowing
/// about the other.
#[derive(Default)]
struct WaitingOnLocalWork {
    /// Present while local activities are outstanding, or while a heartbeat is deferred waiting
    /// for lang to finish an activation.
    local_activities: Option<LocalActivityHeartbeatState>,
    /// This run's external stream waits. Empty until lang reports quiescence.
    external_wait_set: ExternalWaitSet,
    /// Cancels the run-level workflow task rollover deadline, when one is running.
    ///
    /// Separate from the local-activity heartbeat handle: a retained task needs a rollover
    /// deadline whether or not any local activity is outstanding.
    wft_rollover_timer: Option<AbortHandle>,
    /// Set when the *deadline* expired. Forces a replacement task -- but writes no marker, because
    /// Core decided this boundary and has no terminal for it until finalization supplies one.
    deadline_rollover_pending: bool,
    /// Set when *lang* asked for a rollover at the annotation byte budget. Also forces a
    /// replacement task, and unlike the deadline it may write its marker immediately: the very
    /// command that asked for it already carried the terminal.
    budget_rollover_pending: bool,
    /// Cancels the *global* quiescence timer for the external wait set.
    ///
    /// One timer for the whole set. Readiness for any member cancels it, which is what makes an
    /// idle stream unable to park a workflow task another stream is still driving.
    idle_timer: Option<AbortHandle>,
    /// Earliest deadline reported for output buffered in lang but not yet staged.
    output_flush_timer: Option<OutputFlushTimer>,
    /// Whether lang still owns unstaged external output for this Workflow Task.
    output_buffered: bool,
    /// The flush timer expired while a lang activation was outstanding. The completion of that
    /// activation queues the finalization job rather than expecting an answer to an unseen job.
    output_latency_pending: bool,
    /// A losing park's resolve job removed backend intents but did not run Workflow code. Once
    /// finalization is durable, its forced replacement must issue the resolve again.
    park_rollback_resolve_pending: bool,
    /// The boundary a `FinalizeExternalStreams` job is outstanding for.
    ///
    /// Core is annotation-blind, so it cannot manufacture a terminal. Between issuing the job and
    /// receiving `ExternalStreamFinalized` the accumulated annotation is held and **no marker may
    /// be written** -- a truncated annotation is durable and wrong, while an abandoned Workflow
    /// Task commits no cursor and loses no record.
    pending_finalization: Option<ParkReason>,
    /// The trigger a `PrepareExternalStreamPark` job is outstanding for, as the terminal boundary
    /// it will become.
    ///
    /// Held rather than recomputed, because the reason belongs to the moment parking *started*: an
    /// idle expiry and an all-fenced snapshot are indistinguishable by the time the answer comes
    /// back, and the marker must say which one it actually was. Its presence is also what stops a
    /// second handshake being started for a set already in one.
    pending_park: Option<PendingPark>,
    /// The compact manifest for output already staged by lang but not yet recorded in History.
    pending_output_commit: Option<WorkflowOutputStreamCommit>,
    /// The accepted output-capacity boundary requested an otherwise-empty replacement Workflow
    /// Task. That task must enter lang once so it can release the publisher's capacity waiter.
    output_capacity_activation_pending: bool,
    /// Set when the Run is being torn down -- Worker shutdown or an eviction -- while it still
    /// holds a Workflow Task.
    ///
    /// Like the rollover deadline this forces a replacement task, and for a related reason: the
    /// completion is an *offer* of this Run back to the task queue, so that any eligible Worker
    /// can pick it up and reconstruct the subscriptions from the marker. Unlike the deadline, the
    /// Run does not expect to serve that replacement itself.
    shutdown_pending: bool,
    /// A query answer held while a `FinalizeExternalStreams` round trip runs.
    ///
    /// A query response is what refuses retention on an otherwise ordinary quiescent completion,
    /// and that refusal makes the boundary Core's rather than lang's -- so lang's report carries
    /// no terminal and one has to be asked for. The round trip keeps the Workflow Task open, so
    /// nothing has been reported yet, and it runs no user Workflow code, so lang cannot resend the
    /// answer. Held here, it rides onto the completion that finally reports the task.
    deferred_query_responses: Vec<QueryResult>,
}

impl Drop for WaitingOnLocalWork {
    /// A run's external stream deadlines do not outlive the run.
    ///
    /// Each timer task holds a clone of the local-input sender, so one left running keeps the
    /// workflow stream alive for its whole duration -- a Worker shutting down with a retained wait
    /// set would sit out the full idle deadline before it could finish, and would then deliver a
    /// park handshake to a run that no longer exists. The local-activity heartbeat handle is
    /// deliberately *not* aborted here: its callers abort it explicitly and have always relied on
    /// dropping the handle being a no-op.
    fn drop(&mut self) {
        if let Some(handle) = self.idle_timer.take() {
            handle.abort();
        }
        if let Some(handle) = self.wft_rollover_timer.take() {
            handle.abort();
        }
        if let Some(timer) = self.output_flush_timer.take() {
            timer.handle.abort();
        }
    }
}

impl WaitingOnLocalWork {
    /// Whether local work was retaining the task and has now finished.
    ///
    /// The outstanding local activity count lives on the machines rather than here, so it is
    /// passed in. Asking through this method is what keeps the retention decision in one place as
    /// more kinds of local work are added, instead of each caller keying off the count directly.
    ///
    /// Note this is not the negation of "retains": a run that never had local work at all has not
    /// "finished", and autocompleting it would report a workflow task nothing was waiting on.
    fn finished(&self, outstanding_local_activities: usize) -> bool {
        self.local_activities.is_some() && outstanding_local_activities == 0
    }
}
#[derive(Debug)]
struct CompletionDataForWFT {
    task_token: TaskToken,
    query_responses: Vec<QueryResult>,
    has_pending_query: bool,
    activation_was_eviction: bool,
    is_forced_failure: bool,
    versioning_behavior: VersioningBehavior,
}

/// Manages an instance of a [WorkflowMachines], which is not thread-safe, as well as other data
/// associated with that specific workflow run.
struct WorkflowManager {
    machines: WorkflowMachines,
    /// Is always `Some` in normal operation. Optional to allow for unit testing with the test
    /// workflow driver, which does not need to complete activations the normal way.
    command_sink: Option<Sender<Vec<WFCommand>>>,
}

impl WorkflowManager {
    /// Create a new workflow manager given workflow history and execution info as would be found
    /// in [PollWorkflowTaskQueueResponse]
    fn new(basics: RunBasics) -> Self {
        let (wfb, cmd_sink) = DrivenWorkflow::new();
        let state_machines = WorkflowMachines::new(basics, wfb);
        Self {
            machines: state_machines,
            command_sink: Some(cmd_sink),
        }
    }

    /// Update the machines with some events from fetching another page of history. Does *not*
    /// attempt to pull the next activation, unlike [Self::get_next_activation].
    fn feed_history_from_new_page(&mut self, update: HistoryUpdate) -> Result<()> {
        self.machines.new_history_from_server(update)
    }

    /// Let this workflow know that something we've been waiting locally on has resolved, like a
    /// local activity or side effect
    ///
    /// Returns true if the resolution did anything. EX: If the activity is already canceled and
    /// used the TryCancel or Abandon modes, the resolution is uninteresting.
    fn notify_of_local_result(&mut self, resolved: LocalResolution) -> Result<bool> {
        self.machines.local_resolution(resolved)
    }

    /// Fetch the next workflow activation for this workflow if one is required. Doing so will apply
    /// the next unapplied workflow task if such a sequence exists in history we already know about.
    ///
    /// Callers may also need to call [get_server_commands] after this to issue any pending commands
    /// to the server.
    fn get_next_activation(&mut self) -> Result<WorkflowActivation> {
        // First check if there are already some pending jobs, which can be a result of replay.
        let activation = self.machines.get_wf_activation();
        if !activation.jobs.is_empty() {
            return Ok(activation);
        }

        self.machines.apply_next_wft_from_history()?;
        Ok(self.machines.get_wf_activation())
    }

    /// Returns true if machines are ready to apply the next WFT sequence, false if events will need
    /// to be fetched in order to create a complete update with the entire next WFT sequence.
    pub(crate) fn ready_to_apply_next_wft(&self) -> bool {
        self.machines.ready_to_apply_next_wft()
    }

    /// If there are no pending jobs for the workflow apply the next workflow task and check again
    /// if there are any jobs. Importantly, does not *drain* jobs.
    fn apply_next_task_if_ready(&mut self) -> Result<()> {
        if self.machines.has_pending_jobs() {
            return Ok(());
        }
        loop {
            let consumed_events = self.machines.apply_next_wft_from_history()?;

            if consumed_events == 0 || !self.machines.replaying || self.machines.has_pending_jobs()
            {
                // Keep applying tasks while there are events, we are still replaying, and there are
                // no jobs
                break;
            }
        }
        Ok(())
    }

    /// Must be called when we're ready to respond to a WFT after handling catching up on replay
    /// and handling all activation completions from lang.
    fn prepare_for_wft_response(&mut self) -> MachinesWFTResponseContent<'_> {
        self.machines.prepare_for_wft_response()
    }

    /// Remove and return all queued local activities. Once this is called, they need to be
    /// dispatched for execution.
    fn drain_queued_local_activities(&mut self) -> Vec<LocalActRequest> {
        self.machines.drain_queued_local_activities()
    }

    /// Feed the workflow machines new commands issued by the executing workflow code, and iterate
    /// the machines.
    fn push_commands_and_iterate(&mut self, cmds: Vec<WFCommand>) -> Result<()> {
        if let Some(cs) = self.command_sink.as_mut() {
            cs.send(cmds).map_err(|_| {
                WFMachinesError::Fatal("Internal error buffering workflow commands".to_string())
            })?;
        }
        self.machines.iterate_machines()?;
        Ok(())
    }
}

#[derive(Debug)]
struct FulfillableActivationComplete {
    result: ActivationCompleteResult,
    resp_chan: Option<oneshot::Sender<ActivationCompleteResult>>,
}
impl FulfillableActivationComplete {
    fn fulfill(self) {
        if let Some(resp_chan) = self.resp_chan {
            let _ = resp_chan.send(self.result);
        }
    }
}

#[derive(Debug)]
struct RunActivationCompletion {
    task_token: TaskToken,
    start_time: Instant,
    commands: Vec<WFCommand>,
    activation_was_eviction: bool,
    has_pending_query: bool,
    query_responses: Vec<QueryResult>,
    used_flags: Vec<u32>,
    is_forced_failure: bool,
    /// Used to notify the worker when the completion is done processing and the completion can
    /// unblock. Must always be `Some` when initialized.
    resp_chan: Option<oneshot::Sender<ActivationCompleteResult>>,
    versioning_behavior: VersioningBehavior,
}
#[derive(Debug, derive_more::From)]
enum ActOrFulfill {
    OutgoingAct(Option<ActivationOrAuto>),
    FulfillableComplete(Option<FulfillableActivationComplete>),
}

#[derive(derive_more::Debug)]
#[debug("RunUpdateErr({source:?})")]
struct RunUpdateErr {
    source: WFMachinesError,
    complete_resp_chan: Option<oneshot::Sender<ActivationCompleteResult>>,
}

impl From<WFMachinesError> for RunUpdateErr {
    fn from(e: WFMachinesError) -> Self {
        RunUpdateErr {
            source: e,
            complete_resp_chan: None,
        }
    }
}

#[allow(clippy::too_many_arguments)]
fn log_workflow_task_duration(
    run_id: &str,
    workflow_type: &str,
    event_id: i64,
    attempt: u32,
    history_size_bytes: u64,
    duration: Duration,
    storage: &TaskStorageMetrics,
) {
    let threshold = wft_duration_warn_threshold();
    if duration <= threshold {
        return;
    }
    let dl = storage.download.as_ref();
    let ul = storage.upload.as_ref();
    let duration_millis = |d: Duration| d.as_millis() as u64;
    let storage_millis = |m: Option<&ExternalStorageMetrics>| -> u64 {
        m.and_then(|m| m.total_duration)
            .and_then(|d| Duration::try_from(d).ok())
            .map(duration_millis)
            .unwrap_or_default()
    };
    warn!(
        workflow_type = %workflow_type,
        event_id = event_id,
        attempt = attempt,
        workflow_task_duration = duration_millis(duration),
        workflow_history_size = history_size_bytes,
        payload_download_count = dl.map(|m| m.payload_count).unwrap_or_default(),
        payload_download_size = dl.map(|m| m.total_size_bytes).unwrap_or_default(),
        payload_download_duration = storage_millis(dl),
        payload_download_drivers = ?dl.map(|m| sorted(&m.driver_names)).unwrap_or_default(),
        payload_upload_count = ul.map(|m| m.payload_count).unwrap_or_default(),
        payload_upload_size = ul.map(|m| m.total_size_bytes).unwrap_or_default(),
        payload_upload_duration = storage_millis(ul),
        payload_upload_drivers = ?ul.map(|m| sorted(&m.driver_names)).unwrap_or_default(),
        "[TMPRL1104] {run_id}:{event_id}:{attempt} Workflow task duration exceeded {} seconds.",
        threshold.as_secs()
    );
}

fn wft_duration_warn_threshold() -> Duration {
    static THRESHOLD: std::sync::OnceLock<Duration> = std::sync::OnceLock::new();
    *THRESHOLD.get_or_init(|| {
        parse_wft_duration_warn_threshold(
            std::env::var("TEMPORAL_WORKFLOW_TASK_DURATION_WARN_SECONDS").ok(),
        )
    })
}

// Separated from the env read so the parse + default fallback can be unit-tested without mutating
// the process environment.
fn parse_wft_duration_warn_threshold(value: Option<String>) -> Duration {
    value
        .and_then(|s| s.parse::<u64>().ok())
        .map(Duration::from_secs)
        .unwrap_or(Duration::from_secs(5))
}

fn sorted(names: &[String]) -> Vec<String> {
    let mut v = names.to_vec();
    v.sort();
    v
}

/// Whether a newly polled WFT for an existing run must be buffered rather than applied.
///
/// Pulled out of [`ManagedRun::buffer_wft_if_outstanding_work`] because the condition *is* the
/// thing that has to be right: `_incoming_wft` treats a second WFT for one run as a bug and
/// `dbg_panic!`s on it, so every state that means "this run already has a WFT" has to be kept out
/// here.
///
/// `has_wft` is listed first because it was the one missing. `more_pending_work` is not a stand-in
/// for it -- that is `wft.is_some() && machines.has_pending_jobs()`, which answers `false` for an
/// outstanding WFT whose machines have nothing queued, and that is exactly the state a freshly
/// polled task used to sail through. Buffering instead is safe in a way admitting is not: the
/// buffer is drained on the next run update that finds no pending work, and `has_any_pending_work`
/// counts an outstanding WFT, so the task waits precisely until the one in flight is cleared.
fn must_buffer_wft(
    has_wft: bool,
    has_activation: bool,
    about_to_issue_evict: bool,
    more_pending_work: bool,
) -> bool {
    has_wft || has_activation || about_to_issue_evict || more_pending_work
}

#[cfg(test)]
mod tests {
    use super::{
        ManagedRun, TaskStorageMetrics, log_workflow_task_duration, must_buffer_wft,
        parse_wft_duration_warn_threshold,
    };
    use crate::{
        MetricsContext,
        abstractions::tests::fixed_size_permit_dealer,
        protosext::ValidPollWFTQResponse,
        replay::TestHistoryBuilder,
        test_help::{ResponseType, hist_to_poll_resp, mock_worker_client, test_worker_cfg},
        worker::{
            WorkflowSlotKind,
            client::mocks::DEFAULT_TEST_CAPABILITIES,
            workflow::{
                ActivationOrAuto, HistoryPaginator, HistoryUpdate, PermittedWFT, RunBasics,
                RunTimerSink, WFCommand, WFCommandVariant, WFTReportStatus,
            },
        },
    };
    use std::{
        fmt::Write,
        mem::{Discriminant, discriminant},
        sync::{Arc, Mutex},
        time::{Duration, Instant},
    };
    use temporalio_common::protos::{
        coresdk::{WorkflowSlotInfo, common::ExternalStorageMetrics},
        temporal::api::enums::v1::EventType,
    };
    use tokio::sync::mpsc::unbounded_channel;
    use tracing::{
        Event, Level, Metadata, Subscriber,
        field::{Field, Visit},
        span,
    };

    use command_utils::*;

    #[derive(Default)]
    struct CapturedEvent {
        level: Option<Level>,
        fields: String,
    }
    #[derive(Default, Clone)]
    struct CapturingSub {
        events: Arc<Mutex<Vec<CapturedEvent>>>,
    }
    struct FieldVisitor(String);
    impl Visit for FieldVisitor {
        fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
            let _ = write!(self.0, "{}={:?};", field.name(), value);
        }
        fn record_u64(&mut self, field: &Field, value: u64) {
            let _ = write!(self.0, "{}={};", field.name(), value);
        }
        fn record_i64(&mut self, field: &Field, value: i64) {
            let _ = write!(self.0, "{}={};", field.name(), value);
        }
        fn record_str(&mut self, field: &Field, value: &str) {
            let _ = write!(self.0, "{}={};", field.name(), value);
        }
    }
    impl Subscriber for CapturingSub {
        fn enabled(&self, _: &Metadata<'_>) -> bool {
            true
        }
        fn new_span(&self, _: &span::Attributes<'_>) -> span::Id {
            span::Id::from_u64(1)
        }
        fn record(&self, _: &span::Id, _: &span::Record<'_>) {}
        fn record_follows_from(&self, _: &span::Id, _: &span::Id) {}
        fn event(&self, event: &Event<'_>) {
            let mut v = FieldVisitor(String::new());
            event.record(&mut v);
            self.events.lock().unwrap().push(CapturedEvent {
                level: Some(*event.metadata().level()),
                fields: v.0,
            });
        }
        fn enter(&self, _: &span::Id) {}
        fn exit(&self, _: &span::Id) {}
    }

    fn capture(duration: Duration, storage: &TaskStorageMetrics) -> Option<CapturedEvent> {
        let sub = CapturingSub::default();
        tracing::subscriber::with_default(sub.clone(), || {
            log_workflow_task_duration("run-1", "MyWorkflow", 12, 3, 4096, duration, storage);
        });
        sub.events.lock().unwrap().drain(..).next()
    }

    async fn permitted_wft(
        history: &TestHistoryBuilder,
        response_type: ResponseType,
    ) -> PermittedWFT {
        let valid = ValidPollWFTQResponse::try_from(
            hist_to_poll_resp(history, "workflow", response_type).resp,
        )
        .unwrap();
        let (paginator, work) = HistoryPaginator::from_poll(valid, Arc::new(mock_worker_client()))
            .await
            .unwrap();
        let permit = fixed_size_permit_dealer::<WorkflowSlotKind>(1)
            .try_acquire_owned()
            .unwrap()
            .into_used(WorkflowSlotInfo {
                workflow_type: work.workflow_type.clone(),
                is_sticky: work.is_incremental(),
            });
        PermittedWFT {
            work,
            permit,
            paginator,
        }
    }

    async fn run_with_outstanding_wft_and_replacement() -> (ManagedRun, PermittedWFT) {
        let mut history = TestHistoryBuilder::default();
        history.add_by_type(EventType::WorkflowExecutionStarted);
        history.add_workflow_task_scheduled_and_started();
        let first = permitted_wft(&history, ResponseType::AllHistory).await;

        history.add_workflow_task_completed();
        history.add_workflow_task_scheduled_and_started();
        let replacement = permitted_wft(&history, ResponseType::OneTask(2)).await;

        let (timer_tx, _timer_rx) = unbounded_channel();
        let (mut run, first_activation) = ManagedRun::new(
            RunBasics {
                worker_config: Arc::new(test_worker_cfg().build().unwrap()),
                workflow_id: "workflow".to_string(),
                workflow_type: first.work.workflow_type.clone(),
                run_id: history.get_orig_run_id().to_string(),
                history: HistoryUpdate::dummy(),
                metrics: MetricsContext::no_op(),
                capabilities: DEFAULT_TEST_CAPABILITIES,
                sdk_name: "test-core",
                sdk_version: "0.0.0",
            },
            first,
            None,
            RunTimerSink { tx: timer_tx },
        );
        assert!(matches!(
            first_activation,
            Some(ActivationOrAuto::LangActivation(_))
        ));

        run.finish_activation(|_| true);
        assert!(run.activation().is_none());
        assert!(run.wft().is_some());
        assert!(!run.more_pending_work());
        (run, replacement)
    }

    #[test]
    fn an_outstanding_wft_alone_buffers_a_newly_polled_task() {
        // The regression. No activation, nothing about to evict, and machines with nothing
        // queued -- so `more_pending_work` is false -- but a WFT is still outstanding. Admitting
        // here reaches `_incoming_wft`, which treats two WFTs for one run as a bug and
        // `dbg_panic!`s: the workflow-processing thread dies on a debug build, and a release build
        // logs and carries on into code that was never written to hold two.
        assert!(
            must_buffer_wft(true, false, false, false),
            "a run with an outstanding WFT admitted another one"
        );
    }

    #[test]
    fn a_run_with_nothing_outstanding_admits_the_task() {
        // The other half: buffering unconditionally would stall every run, since the buffer is
        // only drained by a later run update.
        assert!(!must_buffer_wft(false, false, false, false));
    }

    #[test]
    fn every_other_kind_of_outstanding_work_still_buffers() {
        // These three were already handled and must stay handled -- the fix adds a reason to
        // buffer, it does not replace the existing ones.
        assert!(must_buffer_wft(false, true, false, false), "activation");
        assert!(must_buffer_wft(false, false, true, false), "pending evict");
        assert!(must_buffer_wft(false, false, false, true), "pending jobs");
    }

    #[tokio::test]
    async fn an_outstanding_managed_run_wft_buffers_then_drains_a_polled_task() {
        let (mut run, replacement) = run_with_outstanding_wft_and_replacement().await;

        assert!(run.buffer_wft_if_outstanding_work(replacement).is_none());
        assert!(run.has_buffered_wft());
        assert!(run.wft().is_some());

        assert!(
            run.mark_wft_complete(
                WFTReportStatus::Reported {
                    reset_last_started_to: None,
                    completion_time: Instant::now(),
                },
                &TaskStorageMetrics::default(),
            )
            .is_some()
        );
        assert!(run.wft().is_none());

        assert!(matches!(
            run.check_more_activations(),
            Some(ActivationOrAuto::Autocomplete { .. })
        ));
        assert!(run.wft().is_some());
        assert!(!run.has_buffered_wft());
    }

    #[cfg(debug_assertions)]
    #[tokio::test]
    #[should_panic(expected = "Trying to send a new WFT for a run which already has one!")]
    async fn bypassing_wft_admission_guard_trips_the_debug_invariant() {
        let (mut run, replacement) = run_with_outstanding_wft_and_replacement().await;

        run.incoming_wft(replacement);
    }

    #[cfg(not(debug_assertions))]
    #[tokio::test]
    async fn bypassing_wft_admission_guard_buffers_in_release() {
        let (mut run, replacement) = run_with_outstanding_wft_and_replacement().await;
        let original_task_token = run.wft().unwrap().info.task_token.clone();

        assert!(run.incoming_wft(replacement).is_none());
        assert!(run.has_buffered_wft());
        assert_eq!(run.wft().unwrap().info.task_token, original_task_token);

        assert!(
            run.mark_wft_complete(
                WFTReportStatus::Reported {
                    reset_last_started_to: None,
                    completion_time: Instant::now(),
                },
                &TaskStorageMetrics::default(),
            )
            .is_some()
        );
        assert!(matches!(
            run.check_more_activations(),
            Some(ActivationOrAuto::Autocomplete { .. })
        ));
        assert!(run.wft().is_some());
        assert!(!run.has_buffered_wft());
    }

    #[test]
    fn tmprl1104_warns_only_over_threshold() {
        let none = TaskStorageMetrics::default();
        assert!(capture(Duration::from_secs(2), &none).is_none());
        let warn_ev = capture(Duration::from_secs(7), &none).expect("warn emitted");
        assert_eq!(warn_ev.level, Some(Level::WARN));
        assert!(
            warn_ev.fields.contains("[TMPRL1104]"),
            "fields: {}",
            warn_ev.fields
        );
    }

    #[test]
    fn tmprl1104_threshold_parsing() {
        assert_eq!(
            parse_wft_duration_warn_threshold(None),
            Duration::from_secs(5)
        );
        assert_eq!(
            parse_wft_duration_warn_threshold(Some("10".to_string())),
            Duration::from_secs(10)
        );
        assert_eq!(
            parse_wft_duration_warn_threshold(Some("0".to_string())),
            Duration::from_secs(0)
        );
        // Unparseable / empty / negative values fall back to the default (parsed as u64, so a
        // negative can never yield a threshold).
        assert_eq!(
            parse_wft_duration_warn_threshold(Some("nope".to_string())),
            Duration::from_secs(5)
        );
        assert_eq!(
            parse_wft_duration_warn_threshold(Some(String::new())),
            Duration::from_secs(5)
        );
        assert_eq!(
            parse_wft_duration_warn_threshold(Some("-5".to_string())),
            Duration::from_secs(5)
        );
    }

    #[test]
    fn tmprl1104_fields_present() {
        let storage = TaskStorageMetrics {
            download: Some(ExternalStorageMetrics {
                payload_count: 2,
                total_size_bytes: 1024,
                total_duration: Some(prost_types::Duration {
                    seconds: 0,
                    nanos: 5_000_000,
                }),
                driver_names: vec!["s3".to_string()],
            }),
            upload: None,
        };
        let ev = capture(Duration::from_secs(6), &storage).expect("warn emitted");
        assert!(ev.fields.contains("attempt=3"), "fields: {}", ev.fields);
        assert!(
            ev.fields.contains("[TMPRL1104] run-1:12:3"),
            "fields: {}",
            ev.fields
        );
        // The message names the (default) threshold it exceeded.
        assert!(
            ev.fields.contains("exceeded 5 seconds"),
            "fields: {}",
            ev.fields
        );
        assert!(
            ev.fields.contains("workflow_history_size=4096"),
            "fields: {}",
            ev.fields
        );
        assert!(
            ev.fields.contains("payload_download_count=2"),
            "fields: {}",
            ev.fields
        );
        // No upload occurred; that group must still be present as zero.
        assert!(
            ev.fields.contains("payload_upload_count=0"),
            "fields: {}",
            ev.fields
        );
    }

    #[rstest::rstest]
    #[case::empty(
        vec![],
        vec![])]
    #[case::non_terminal_is_retained(
        vec![update_response()],
        vec![update_response()])]
    #[case::terminal_is_retained(
        vec![complete()],
        vec![complete()])]
    #[case::post_terminal_is_retained(
        vec![complete(), update_response()],
        vec![update_response(), complete()])]
    #[case::second_terminal_is_discarded(
        vec![cancel(), complete()],
        vec![cancel()])]
    #[case::move_terminals_to_end_and_retain_first(
        vec![update_response(), complete(), update_response(), cancel(), update_response()],
        vec![update_response(), update_response(), update_response(), complete()])]
    #[test]
    fn preprocess_command_sequence(
        #[case] commands_in: Vec<WFCommand>,
        #[case] expected_commands: Vec<WFCommand>,
    ) {
        let (commands, _) = super::preprocess_command_sequence(commands_in);
        assert_eq!(command_types(&commands), command_types(&expected_commands));
    }

    #[rstest::rstest]
    #[case::query_responses_extracted(
        vec![query_response(), update_response(), query_response(), complete(), query_response()],
        3,
    )]
    #[test]
    fn preprocess_command_sequence_extracts_queries(
        #[case] commands_in: Vec<WFCommand>,
        #[case] expected_queries_out: usize,
    ) {
        let (_, query_responses_out) = super::preprocess_command_sequence(commands_in);
        assert_eq!(query_responses_out.len(), expected_queries_out);
    }

    #[rstest::rstest]
    #[case::empty(
        vec![],
        vec![])]
    #[case::non_terminal_is_retained(
        vec![update_response()],
        vec![update_response()])]
    #[case::terminal_is_retained(
        vec![complete()],
        vec![complete()])]
    #[case::post_terminal_is_discarded(
        vec![complete(), update_response()],
        vec![complete()])]
    #[case::second_terminal_is_discarded(
        vec![cancel(), complete()],
        vec![cancel()])]
    #[case::truncate_at_first_complete(
        vec![update_response(), complete(), update_response(), cancel()],
        vec![update_response(), complete()])]
    #[test]
    fn preprocess_command_sequence_old_behavior(
        #[case] commands_in: Vec<WFCommand>,
        #[case] expected_out: Vec<WFCommand>,
    ) {
        let (commands_out, _) = super::preprocess_command_sequence_old_behavior(commands_in);
        assert_eq!(command_types(&commands_out), command_types(&expected_out));
    }

    #[rstest::rstest]
    #[case::query_responses_extracted(
        vec![query_response(), update_response(), query_response(), complete(), query_response()],
        3,
    )]
    #[test]
    fn preprocess_command_sequence_old_behavior_extracts_queries(
        #[case] commands_in: Vec<WFCommand>,
        #[case] expected_queries_out: usize,
    ) {
        let (_, query_responses_out) = super::preprocess_command_sequence_old_behavior(commands_in);
        assert_eq!(query_responses_out.len(), expected_queries_out);
    }

    mod command_utils {
        use temporalio_common::protos::coresdk::workflow_commands::{
            CancelWorkflowExecution, CompleteWorkflowExecution, QueryResult, UpdateResponse,
        };

        use super::*;

        pub(crate) fn complete() -> WFCommand {
            WFCommand::new(WFCommandVariant::CompleteWorkflow(
                CompleteWorkflowExecution { result: None },
            ))
        }

        pub(crate) fn cancel() -> WFCommand {
            WFCommand::new(WFCommandVariant::CancelWorkflow(
                CancelWorkflowExecution::default(),
            ))
        }

        pub(crate) fn query_response() -> WFCommand {
            WFCommand::new(WFCommandVariant::QueryResponse(QueryResult {
                query_id: "".into(),
                variant: None,
            }))
        }

        pub(crate) fn update_response() -> WFCommand {
            WFCommand::new(WFCommandVariant::UpdateResponse(UpdateResponse {
                protocol_instance_id: "".into(),
                response: None,
            }))
        }

        pub(crate) fn command_types(commands: &[WFCommand]) -> Vec<Discriminant<WFCommand>> {
            commands.iter().map(discriminant).collect()
        }
    }
}
