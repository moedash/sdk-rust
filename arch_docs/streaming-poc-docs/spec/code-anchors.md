# Code anchors

**Every file-and-line reference in these documents lives here and nowhere else.** Specs name
symbols and files; this table carries the line numbers, so a Core rebase updates one table
instead of every document that cites a location.

Line-accurate against:

| Tree | Path | Commit |
|------|------|--------|
| Core (Rust) | `sdk-python/temporalio/bridge/sdk-core` (submodule) | `6e90e6d5` |
| Python SDK | `sdk-python` | `ec200384` |

Core **code** work happens inside the `sdk-python` submodule checkout. The standalone `sdk-rust/`
worktree stays at the same upstream commit and carries no Core source changes — it hosts these
documents under `arch_docs/streaming-poc-docs/` and nothing else.

## Core — workflow orchestration

| What | Where |
|------|-------|
| WFT-retention state | `managed_run.rs:1404` (`struct WaitingOnLAs`), field at `:76` |
| Retention decision logic | `managed_run.rs:745`–`:870`, keyed off `outstanding_local_activity_count()` |
| Other `waiting_on_la` readers | `managed_run.rs:144`, `:230`, `:269`, `:370`–`:395`, `:898` |
| Local input extension point | `workflow_stream.rs:663` (`enum LocalInputs`), `run_id()` match at `:677` |
| Readiness entry-point templates | `worker/mod.rs:1393` (`pub fn record_activity_heartbeat`), `:1735` (`fn notify_local_result`) → `workflow/mod.rs:644` (`notify_of_local_result`) |
| Command validation | `workflow/mod.rs:1311` (`validate_completion`), `:1448` (`TryFrom<WorkflowCommand> for WFCommand`) |
| Marker lookahead pattern | `machines/local_activity_state_machine.rs:51`, `:68`–`:69`, `:87` |
| Read-only query on the run lane | `GetStateInfoMsg` handled at `workflow_stream.rs:145`, sent via `send_get_state_info_msg` (`workflow/mod.rs:672`) |

## Core — rollover timer

| What | Where |
|------|-------|
| Rollover deadline scheduling | `managed_run.rs:1381` (`sink_heartbeat_timeout_start`) — no-ops without the LA sink |
| LA request sink field | `managed_run.rs:74` (`Option`al) |
| Sink created only under this flag | `worker/mod.rs:807` (`config.task_types.enable_local_activities`) |
| Python sets that flag | `_worker.py:653` (`enable_local_activities = self._activity_worker is not None`) |
| `force_new_wft` computed | `managed_run.rs:1176`, in `prepare_complete_resp`, from `due_to_heartbeat_timeout` |
| `force_new_wft` reaches the server | `workflow/mod.rs:402` (`ActivationAction::WftComplete`), on an outcome built from `data.task_token` |

## Core — shutdown sequencing

| What | Where |
|------|-------|
| No eviction while an activation is outstanding | `managed_run.rs:349` (`_check_more_activations`) |
| An eviction completion may carry no commands | `managed_run.rs:486`; `should_respond` false when `activation_was_eviction` |
| Shutdown waits for all pending work | `workflow_stream.rs:589` (`shutdown_done`) |
| `ignore_evicts_on_shutdown = false` default | `worker/mod.rs:227` — not overridden by Python |

## Core — protos

All under `crates/protos/protos/local/temporal/sdk/core/`:

| File | Anchor |
|------|--------|
| `workflow_commands/workflow_commands.proto` | `WorkflowCommand.variant` oneof at line 28; tags 1–22 in use |
| `workflow_activation/workflow_activation.proto` | `WorkflowActivationJob.variant` oneof at line 108; tags 1–16 assigned except 3 (retired, not reused); 50 is `RemoveFromCache` |
| `external_data/external_data.proto` | alongside `LocalActivityMarkerData`, `PatchedMarkerData` |
| `workflow_completion/workflow_completion.proto` | `oneof status { Success \| Failure }`; `Failure` carries only `failure` + `force_cause` |
| `api_upstream/.../failed_cause.proto:88` | `WORKFLOW_TASK_FAILED_CAUSE_EXTERNAL_STORAGE_FAILURE = 38` |

## Python SDK

| What | Where |
|------|-------|
| `activate()` is synchronous | `_workflow_instance.py:444` |
| Deadlock timeout (2 seconds) | `_workflow.py:180`; executor + `wait_for` at `:410`–`:418` |
| Async pre-activation hook | `_workflow.py:310` (`async def _handle_activation`); awaits `decode_activation` at `:383`, hands to executor at `:407` |
| Activation job dispatch chain | `_workflow_instance.py:602` (`job.HasField(...)`) |
| Quiescence hook | `_workflow_instance.py:2627` (`_run_once`) — already drains `self._ready` to empty |
| Single-batch activation drain | `_workflow_instance.py:508` (`_single_batch_activation`) |
| Cache-eviction teardown | `_workflow.py:538` (`_handle_cache_eviction`); `_running_workflows` entry removed at `:635` |
| Bridge | `bridge/src/worker.rs:659` (sync `record_activity_heartbeat`), `bridge/worker.py:256` |
| Sandbox passthrough registration | `worker/workflow_sandbox/_restrictions.py` |
| Failure-type option names | `workflow_failure_exception_types` (Worker, `plugin.py:53`), `failure_exception_types` (`workflow/_definition.py:42`) |
| Existing contrib feature to avoid colliding with | `temporalio/contrib/workflow_streams/`; reserved names at `_stream.py:53`–`:55` |
| Proto generation | `scripts/gen_protos.py`; re-run `scripts/gen_payload_visitor.py` for any new message carrying a `Payload` |

## Environment facts verified in the container

- `libprotobuf-dev` is **required** and is not in the base image — without it `prost-wkt-types`
  fails on missing `google/protobuf/{duration,timestamp}.proto`.
- `Logger::Console` gained a `format` field in Core `6e90e6d5`; the bridge needs `format: None`
  at `temporalio/bridge/src/runtime.rs:122`. Applied in `ec200384`.
- **Redis 7.0.15** via apt; **Temporal CLI 1.8.2** (Server 1.31.2) at `/usr/local/bin/temporal`.
- `redis>=5,<9` is in the `dev` dependency group of `sdk-python/pyproject.toml` (line 88).
- Redis stream IDs are `<ms>-<seq>` and stay totally ordered within a single millisecond
  (verified: `...012-0`, `...012-1`).
- **`XREAD` is exclusive; `XRANGE` is inclusive.** Verified against the running Redis 7.0.15:
  with entries *A* then *B*, `XREAD ... STREAMS s <A>` returns only *B*, while `XRANGE s <A> +`
  returns both. The replay read path must use `XRANGE`; `XREAD BLOCK` is for live watching only.
- The container has no init system. Run `./start-env.sh` from the task directory after every
  container start (`start` | `status` | `stop`).
