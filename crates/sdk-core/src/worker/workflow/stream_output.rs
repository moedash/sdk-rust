//! The stream output a Workflow Task publishes: the records lang commits with a completion, and
//! the manifest Core builds from them and records in a `core_external_stream` marker.

use super::WFMachinesError;
use std::collections::HashMap;
use temporalio_common::{
    protos::{
        coresdk::{
            external_data::{
                ExternalOutputSegmentManifest, ExternalOutputStreamManifest,
                ExternalOutputTopicManifest,
            },
            workflow_commands::OutputRecord,
        },
        temporal::sdk::streams::v1::StreamRecordKind,
    },
    streams::{FINGERPRINT_VERSION, FingerprintRecord, fingerprint},
};

/// The marker version Core writes. Readers must accept every version up to this one.
pub(super) const MARKER_SCHEMA_VERSION: u32 = 1;
const MANIFEST_SCHEMA_VERSION: u32 = 1;
/// The layout a store keeps records in. A store that changes it must refuse older stages.
const STORE_FORMAT_VERSION: u32 = 1;
/// Keeps one output proof well inside the server's per-event payload limits.
const MAX_MANIFEST_BYTES: usize = 64 * 1024;
const CONTENT_HASH_BYTES: usize = 32;

type Result<T, E = WFMachinesError> = std::result::Result<T, E>;

/// One publishing completion's records, held until its Workflow Task's completion is reported.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct OutputBatch {
    /// The stage token the completion's marker names.
    pub(crate) token: String,
    pub(crate) history_floor_event_id: i64,
    pub(crate) workflow_id: String,
    pub(crate) run_id: String,
    pub(crate) first_run_id: String,
    pub(crate) records: Vec<OutputRecord>,
}

/// The stream output a completion carries to the report path.
#[derive(Debug, Default)]
pub(crate) struct ReportedOutput {
    /// To stage before the completion is sent, in commit order.
    pub(crate) output: Vec<OutputBatch>,
    /// What replay proved committed, to promote, in commit order.
    pub(crate) proven: Vec<ProvenStage>,
}

impl ReportedOutput {
    /// What to report with the next completion, leaving both lists empty, or `None` when there is
    /// nothing.
    pub(crate) fn take(
        output: &mut Vec<OutputBatch>,
        proven: &mut Vec<ProvenStage>,
    ) -> Option<Box<Self>> {
        if output.is_empty() && proven.is_empty() {
            return None;
        }
        Some(Box::new(Self {
            output: std::mem::take(output),
            proven: std::mem::take(proven),
        }))
    }
}

/// A stage History proved committed, found while replaying.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct ProvenStage {
    pub(crate) token: String,
    pub(crate) workflow_id: String,
    pub(crate) first_run_id: String,
    /// In order of first publish, as the recorded manifest lists them.
    pub(crate) topics: Vec<String>,
}

/// What the server answered to a completion that carried stream output.
#[cfg(feature = "streams")]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum CompletionOutcome {
    /// The commands were applied, so each output marker is in History.
    Accepted,
    /// The commands were not applied, so no output marker is in History.
    Refused,
    /// The completion may or may not have been applied.
    Unknown,
}

/// The Worker's side of a Workflow's stream output: stages each completion's records, then
/// promotes or aborts them by what the server answered.
#[cfg(feature = "streams")]
pub(crate) struct OutputStore {
    store: std::sync::Arc<dyn temporalio_streams::StreamStore>,
    namespace: String,
    /// Stages whose completion's outcome is unknown, by run. The run is evicted when that
    /// happens, so the replay before its next completion proves each committed one. Bounded,
    /// since a run that moves to another Worker never comes back here.
    undecided: parking_lot::Mutex<lru::LruCache<String, Vec<OutputBatch>>>,
    /// Tokens this Worker promoted or aborted, so a run replayed again and again doesn't ask the
    /// store about each one every time. Forgetting one only costs a repeat request.
    settled: parking_lot::Mutex<lru::LruCache<String, ()>>,
}

#[cfg(feature = "streams")]
const MAX_UNDECIDED_RUNS: usize = 1000;
#[cfg(feature = "streams")]
const MAX_SETTLED_TOKENS: usize = 10_000;

#[cfg(feature = "streams")]
impl OutputStore {
    pub(crate) fn new(
        store: std::sync::Arc<dyn temporalio_streams::StreamStore>,
        namespace: String,
    ) -> Self {
        Self {
            store,
            namespace,
            undecided: parking_lot::Mutex::new(lru::LruCache::new(
                std::num::NonZeroUsize::new(MAX_UNDECIDED_RUNS).expect("not zero"),
            )),
            settled: parking_lot::Mutex::new(lru::LruCache::new(
                std::num::NonZeroUsize::new(MAX_SETTLED_TOKENS).expect("not zero"),
            )),
        }
    }

    /// Stages `batches` in commit order, each whole. Nothing is visible until promoted, so a
    /// completion the server refuses publishes nothing.
    pub(crate) async fn stage(
        &self,
        batches: &[OutputBatch],
    ) -> std::result::Result<(), temporalio_streams::StreamError> {
        use prost::Message;
        use temporalio_streams::proto::{StagedBatch, StagedRecord};
        for batch in batches {
            let records = batch
                .records
                .iter()
                .map(|record| {
                    temporalio_streams::stored_output_record(
                        &record.topic,
                        record.kind,
                        record.body.clone(),
                        &record.content_hash,
                        &batch.run_id,
                    )
                    .map(|stored| StagedRecord {
                        topic: record.topic.clone(),
                        record: stored.encode_to_vec(),
                    })
                })
                .collect::<std::result::Result<Vec<_>, _>>()?;
            self.store
                .stage(StagedBatch {
                    chain: Some(self.chain(&batch.workflow_id, &batch.first_run_id)),
                    run_id: batch.run_id.clone(),
                    token: batch.token.clone(),
                    history_floor_event_id: batch.history_floor_event_id,
                    records,
                })
                .await?;
        }
        Ok(())
    }

    /// Promotes the stages replay proved committed, in commit order, each once.
    pub(crate) async fn promote_proven(&self, run_id: &str, proven: &[ProvenStage]) {
        for stage in proven {
            if let Some(undecided) = self.undecided.lock().get_mut(run_id) {
                undecided.retain(|batch| batch.token != stage.token);
            }
            if self.settled.lock().contains(&stage.token) {
                continue;
            }
            let stage_ref = self.stage_ref(
                &stage.workflow_id,
                &stage.first_run_id,
                &stage.token,
                stage.topics.clone(),
            );
            self.promote(stage_ref).await;
        }
    }

    /// Promotes or aborts `batches`, staged for one completion, by the server's answer to it.
    ///
    /// An accepted completion also settles the run's stages whose outcome was unknown. Replay
    /// before this completion proved and promoted every one of them that History holds, so the
    /// rest never committed.
    pub(crate) async fn settle(
        &self,
        run_id: &str,
        batches: Vec<OutputBatch>,
        outcome: CompletionOutcome,
    ) {
        match outcome {
            CompletionOutcome::Accepted => {
                for batch in &batches {
                    self.promote(self.batch_ref(batch)).await;
                }
                let undecided = self.undecided.lock().pop(run_id).unwrap_or_default();
                for batch in &undecided {
                    self.abort(self.batch_ref(batch)).await;
                }
            }
            CompletionOutcome::Refused => {
                for batch in &batches {
                    self.abort(self.batch_ref(batch)).await;
                }
            }
            CompletionOutcome::Unknown => {
                if !batches.is_empty() {
                    let mut undecided = self.undecided.lock();
                    match undecided.get_mut(run_id) {
                        Some(held) => held.extend(batches),
                        None => {
                            undecided.put(run_id.to_string(), batches);
                        }
                    }
                }
            }
        }
    }

    async fn promote(&self, stage: temporalio_streams::proto::StageRef) {
        use temporalio_streams::proto::PromoteOutcome;
        match self.store.promote(&stage).await {
            Ok(result) => {
                if result.outcome() == PromoteOutcome::Lost {
                    warn!(
                        token = %stage.token,
                        "Stream output a Workflow committed was dropped by retention before it \
                         was promoted"
                    );
                }
                self.settled.lock().put(stage.token, ());
            }
            Err(error) => {
                // The stage stays pending in the store, and a reader settles it from History.
                warn!(token = %stage.token, %error, "Could not promote committed stream output");
            }
        }
    }

    async fn abort(&self, stage: temporalio_streams::proto::StageRef) {
        match self.store.abort(&stage).await {
            Ok(()) => {
                self.settled.lock().put(stage.token, ());
            }
            Err(error) => {
                // The stage stays pending in the store, and a reader settles it from History.
                warn!(token = %stage.token, %error, "Could not abort uncommitted stream output");
            }
        }
    }

    fn chain(&self, workflow_id: &str, first_run_id: &str) -> temporalio_streams::proto::ChainId {
        temporalio_streams::proto::ChainId {
            namespace: self.namespace.clone(),
            workflow_id: workflow_id.to_string(),
            first_run_id: first_run_id.to_string(),
        }
    }

    fn batch_ref(&self, batch: &OutputBatch) -> temporalio_streams::proto::StageRef {
        let mut topics: Vec<String> = vec![];
        for record in &batch.records {
            if !topics.contains(&record.topic) {
                topics.push(record.topic.clone());
            }
        }
        self.stage_ref(
            &batch.workflow_id,
            &batch.first_run_id,
            &batch.token,
            topics,
        )
    }

    fn stage_ref(
        &self,
        workflow_id: &str,
        first_run_id: &str,
        token: &str,
        topics: Vec<String>,
    ) -> temporalio_streams::proto::StageRef {
        temporalio_streams::proto::StageRef {
            chain: Some(self.chain(workflow_id, first_run_id)),
            token: token.to_string(),
            topics,
        }
    }
}

/// Builds the manifest for the records one completion committed.
///
/// `replaying` allows records without bodies, since lang sends no body when nothing is stored
/// again. The run id and stage token are recorded but replay never compares them. Fails the
/// Workflow Task when a record is one no writer stores, when Core can't name the task's history
/// floor, or when the manifest is over its budget. Each failure names what to fix, because the
/// task fails the same way on every retry.
pub(super) fn build_output_manifest(
    records: &[OutputRecord],
    history_floor_event_id: Option<i64>,
    run_id: &str,
    stage_token: String,
    replaying: bool,
) -> Result<ExternalOutputStreamManifest> {
    let Some(history_floor_event_id) = history_floor_event_id.filter(|id| *id > 0) else {
        return Err(refused(
            "Core could not identify the exact event preceding this Workflow Task's scheduled \
             event"
                .to_string(),
        ));
    };
    if records.is_empty() {
        return Err(refused("the commit carried no records".to_string()));
    }
    for (index, record) in records.iter().enumerate() {
        check_record(record, replaying)
            .map_err(|reason| refused(format!("record {index} {reason}")))?;
    }
    let mut topics: Vec<&str> = vec![];
    let mut by_topic: HashMap<&str, Vec<&OutputRecord>> = HashMap::new();
    for record in records {
        let topic = record.topic.as_str();
        by_topic
            .entry(topic)
            .or_insert_with(|| {
                topics.push(topic);
                vec![]
            })
            .push(record);
    }
    let topic_manifests: Vec<ExternalOutputTopicManifest> = topics
        .iter()
        .map(|topic| {
            let topic_records = &by_topic[topic];
            ExternalOutputTopicManifest {
                topic: topic.to_string(),
                record_count: topic_records.len() as u32,
                logical_byte_count: topic_records.iter().map(|r| r.logical_size).sum(),
                logical_fingerprint: fingerprint(topic_records.iter().map(|r| FingerprintRecord {
                    topic,
                    kind: r.kind,
                    content_hash: &r.content_hash,
                }))
                .to_vec(),
                finished: topic_records
                    .iter()
                    .any(|r| r.kind == StreamRecordKind::Finish as i32),
            }
        })
        .collect();
    let manifest = ExternalOutputStreamManifest {
        schema_version: MANIFEST_SCHEMA_VERSION,
        fingerprint_version: FINGERPRINT_VERSION,
        stage_token,
        history_floor_event_id,
        run_id: run_id.to_string(),
        segments: vec![ExternalOutputSegmentManifest {
            record_counts_by_topic: topic_manifests.iter().map(|t| t.record_count).collect(),
        }],
        topics: topic_manifests,
        provider_id: String::new(),
        provider_format_version: STORE_FORMAT_VERSION,
    };
    let encoded_len = prost::Message::encoded_len(&manifest);
    if encoded_len > MAX_MANIFEST_BYTES {
        return Err(refused(format!(
            "the manifest of {} topic(s) encodes to {encoded_len} bytes, over the {} KiB marker \
             budget; spread the topics across Workflow Tasks",
            manifest.topics.len(),
            MAX_MANIFEST_BYTES / 1024
        )));
    }
    Ok(manifest)
}

/// Compares the manifest Core built from the records lang committed while replaying with the one
/// History recorded.
///
/// The stage token is left out because nothing is staged on replay, so there is no token. The
/// run id is left out because a reset forks the base run's History, so the new run replays
/// markers that name the base run. Everything else derives from the Workflow's own output.
pub(super) fn check_replayed_output_manifest(
    mut replayed: ExternalOutputStreamManifest,
    recorded: Option<ExternalOutputStreamManifest>,
) -> Result<()> {
    let Some(mut recorded) = recorded else {
        return Err(WFMachinesError::Nondeterminism(
            "Lang committed external output while replaying a Workflow Task that recorded none"
                .to_string(),
        ));
    };
    // Only the logical output must replay the same. A reset run replays markers that name its
    // base run, a stage token is new on every attempt, and a provider can be renamed or swapped
    // for a Replayer without changing what the Workflow published.
    for manifest in [&mut replayed, &mut recorded] {
        manifest.stage_token.clear();
        manifest.run_id.clear();
        manifest.provider_id.clear();
    }
    if replayed != recorded {
        return Err(WFMachinesError::Nondeterminism(format!(
            "External output committed while replaying differs from the manifest recorded in \
             History: committed {replayed:?}, recorded {recorded:?}"
        )));
    }
    Ok(())
}

fn check_record(record: &OutputRecord, replaying: bool) -> std::result::Result<(), String> {
    if record.topic.is_empty() {
        return Err("has no topic".to_string());
    }
    match StreamRecordKind::try_from(record.kind) {
        Ok(StreamRecordKind::Data) => {
            if record.body.is_none() && !replaying {
                return Err("is DATA without a body".to_string());
            }
            if record.content_hash.len() != CONTENT_HASH_BYTES {
                return Err(format!(
                    "is DATA with a {}-byte content hash, not the {CONTENT_HASH_BYTES}-byte \
                     SHA-256 of its plaintext body",
                    record.content_hash.len()
                ));
            }
        }
        Ok(StreamRecordKind::Finish) => {
            if record.body.is_some() || !record.content_hash.is_empty() || record.logical_size != 0
            {
                return Err("is FINISH with a body".to_string());
            }
        }
        _ => return Err(format!("has kind {}, not DATA or FINISH", record.kind)),
    }
    Ok(())
}

fn refused(reason: String) -> WFMachinesError {
    WFMachinesError::Fatal(format!("Refusing a stream output commit, since {reason}"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use temporalio_common::protos::temporal::api::common::v1::Payload;

    const RUN_ID: &str = "run-id";
    const FLOOR: i64 = 4;

    fn data(topic: &str, hash: u8, size: u64) -> OutputRecord {
        OutputRecord {
            topic: topic.to_string(),
            kind: StreamRecordKind::Data as i32,
            body: Some(Payload {
                data: b"ciphertext".to_vec(),
                ..Default::default()
            }),
            content_hash: vec![hash; CONTENT_HASH_BYTES],
            logical_size: size,
        }
    }

    fn finish(topic: &str) -> OutputRecord {
        OutputRecord {
            topic: topic.to_string(),
            kind: StreamRecordKind::Finish as i32,
            ..Default::default()
        }
    }

    fn build(records: &[OutputRecord]) -> Result<ExternalOutputStreamManifest> {
        build_output_manifest(records, Some(FLOOR), RUN_ID, "token".to_string(), false)
    }

    fn rejection(records: &[OutputRecord]) -> String {
        build(records)
            .expect_err("commit must be refused")
            .to_string()
    }

    #[test]
    fn the_manifest_groups_topics_in_order_of_first_publish() {
        let records = [
            data("b", 1, 3),
            data("a", 2, 4),
            data("b", 3, 5),
            finish("b"),
        ];
        let manifest = build(&records).unwrap();
        assert_eq!(manifest.schema_version, 1);
        assert_eq!(manifest.fingerprint_version, 2);
        assert_eq!(manifest.stage_token, "token");
        assert_eq!(manifest.history_floor_event_id, FLOOR);
        assert_eq!(manifest.run_id, RUN_ID);
        assert_eq!(manifest.provider_format_version, 1);
        let topics: Vec<_> = manifest
            .topics
            .iter()
            .map(|t| {
                (
                    t.topic.as_str(),
                    t.record_count,
                    t.logical_byte_count,
                    t.finished,
                )
            })
            .collect();
        assert_eq!(topics, [("b", 3, 8, true), ("a", 1, 4, false)]);
        assert_eq!(manifest.segments.len(), 1);
        assert_eq!(manifest.segments[0].record_counts_by_topic, [3, 1]);
        let b = [&records[0], &records[2], &records[3]];
        assert_eq!(
            manifest.topics[0].logical_fingerprint,
            fingerprint(b.iter().map(|r| FingerprintRecord {
                topic: "b",
                kind: r.kind,
                content_hash: &r.content_hash,
            }))
        );
    }

    #[test]
    fn the_manifest_never_depends_on_the_encoded_body() {
        let mut reencrypted = data("t", 1, 3);
        reencrypted.body.as_mut().unwrap().data = b"other ciphertext".to_vec();
        let mut replayed = data("t", 1, 3);
        replayed.body = None;
        let live = build(&[data("t", 1, 3)]).unwrap();
        assert_eq!(build(&[reencrypted]).unwrap(), live);
        let replay =
            build_output_manifest(&[replayed], Some(FLOOR), RUN_ID, String::new(), true).unwrap();
        assert_eq!(replay.topics, live.topics);
    }

    #[test]
    fn an_unknown_floor_is_refused_rather_than_guessed() {
        for floor in [None, Some(0)] {
            let error = build_output_manifest(
                &[data("t", 1, 1)],
                floor,
                RUN_ID,
                "token".to_string(),
                false,
            )
            .unwrap_err()
            .to_string();
            assert!(error.contains("exact event"), "{error}");
        }
    }

    #[test]
    fn each_record_no_writer_stores_is_refused_with_its_own_message() {
        let mut no_body = data("t", 1, 1);
        no_body.body = None;
        let mut short_hash = data("t", 1, 1);
        short_hash.content_hash.truncate(3);
        let mut finish_with_body = finish("t");
        finish_with_body.body = Some(Payload::default());
        let unspecified = OutputRecord {
            kind: StreamRecordKind::Unspecified as i32,
            ..data("t", 1, 1)
        };
        for (record, expected) in [
            (data("", 1, 1), "record 1 has no topic"),
            (no_body, "record 1 is DATA without a body"),
            (short_hash, "record 1 is DATA with a 3-byte content hash"),
            (finish_with_body, "record 1 is FINISH with a body"),
            (unspecified, "record 1 has kind 0"),
        ] {
            let message = rejection(&[data("t", 9, 1), record]);
            assert!(message.contains(expected), "{expected}: {message}");
        }
        assert!(rejection(&[]).contains("no records"));
    }

    fn manifest() -> ExternalOutputStreamManifest {
        build(&[data("a", 1, 3), finish("b")]).unwrap()
    }

    #[test]
    fn a_replayed_commit_matches_the_recorded_manifest_whatever_its_token() {
        let mut replayed = manifest();
        replayed.stage_token.clear();
        check_replayed_output_manifest(replayed, Some(manifest())).unwrap();
    }

    #[test]
    fn a_replayed_commit_matches_a_manifest_recorded_by_another_run() {
        let mut recorded = manifest();
        recorded.run_id = "reset-base-run".to_string();
        check_replayed_output_manifest(manifest(), Some(recorded)).unwrap();
    }

    #[test]
    fn a_replayed_commit_matches_a_manifest_recorded_under_another_store_name() {
        let mut recorded = manifest();
        recorded.provider_id = "renamed-store".to_string();
        check_replayed_output_manifest(manifest(), Some(recorded)).unwrap();
    }

    #[test]
    fn a_replayed_commit_in_another_store_format_is_nondeterministic() {
        let mut recorded = manifest();
        recorded.provider_format_version = 2;
        assert!(matches!(
            check_replayed_output_manifest(manifest(), Some(recorded)),
            Err(WFMachinesError::Nondeterminism(message)) if message.contains("differs")
        ));
    }

    #[test]
    fn a_replayed_commit_that_differs_from_history_is_nondeterministic() {
        let replayed = build(&[data("a", 2, 3), finish("b")]).unwrap();
        assert!(matches!(
            check_replayed_output_manifest(replayed, Some(manifest())),
            Err(WFMachinesError::Nondeterminism(message)) if message.contains("differs")
        ));
    }

    #[test]
    fn a_replayed_commit_with_nothing_recorded_is_nondeterministic() {
        assert!(matches!(
            check_replayed_output_manifest(manifest(), None),
            Err(WFMachinesError::Nondeterminism(message)) if message.contains("recorded none")
        ));
    }

    #[test]
    fn a_manifest_over_its_budget_is_refused() {
        let records: Vec<_> = (0..1200)
            .map(|i| data(&format!("topic-{i:04}-{}", "x".repeat(16)), 1, 1))
            .collect();
        let message = rejection(&records);
        assert!(
            message.contains("over the 64 KiB marker budget"),
            "{message}"
        );
    }

    #[cfg(feature = "streams")]
    mod store {
        use super::{super::*, data};
        use parking_lot::Mutex;
        use std::sync::Arc;
        use temporalio_streams::{
            StreamError, StreamResult, StreamStore,
            proto::{
                ChainId, DeleteOwnerRequest, DeleteOwnerResponse, PendingStage, PromoteOutcome,
                PromoteResult, StageRef, StagedBatch, StoreAppendRequest, StoreAppendResponse,
                StoreLatestRequest, StoreLatestResponse, StoreReadRequest, StoreReadResponse,
            },
        };

        /// Logs promotions and aborts, and refuses everything else.
        #[derive(Default)]
        struct Log(Mutex<Vec<String>>);

        #[async_trait::async_trait]
        impl StreamStore for Log {
            fn name(&self) -> &str {
                "log"
            }
            async fn append(&self, _: StoreAppendRequest) -> StreamResult<StoreAppendResponse> {
                Err(StreamError::unsupported("append"))
            }
            async fn read(&self, _: StoreReadRequest) -> StreamResult<StoreReadResponse> {
                Err(StreamError::unsupported("read"))
            }
            async fn latest(&self, _: StoreLatestRequest) -> StreamResult<StoreLatestResponse> {
                Err(StreamError::unsupported("latest"))
            }
            async fn record_at(
                &self,
                _: &ChainId,
                _: &str,
                _: &str,
            ) -> StreamResult<Option<Vec<u8>>> {
                Err(StreamError::unsupported("record_at"))
            }
            async fn stage(&self, _: StagedBatch) -> StreamResult<()> {
                Err(StreamError::unsupported("stage"))
            }
            async fn promote(&self, stage: &StageRef) -> StreamResult<PromoteResult> {
                self.0.lock().push(format!("promote {}", stage.token));
                Ok(PromoteResult {
                    outcome: PromoteOutcome::Promoted as i32,
                    records: 1,
                })
            }
            async fn abort(&self, stage: &StageRef) -> StreamResult<()> {
                self.0.lock().push(format!("abort {}", stage.token));
                Ok(())
            }
            async fn close_chain(&self, _: &ChainId) -> StreamResult<()> {
                Err(StreamError::unsupported("close_chain"))
            }
            async fn close_topic(&self, _: &ChainId, _: &str) -> StreamResult<()> {
                Err(StreamError::unsupported("close_topic"))
            }
            async fn pending_stages(&self, _: &ChainId) -> StreamResult<Vec<PendingStage>> {
                Err(StreamError::unsupported("pending_stages"))
            }
            async fn delete_owner(
                &self,
                _: DeleteOwnerRequest,
            ) -> StreamResult<DeleteOwnerResponse> {
                Err(StreamError::unsupported("delete_owner"))
            }
        }

        fn proven(token: &str) -> ProvenStage {
            ProvenStage {
                token: token.to_string(),
                workflow_id: "wf".to_string(),
                first_run_id: "run".to_string(),
                topics: vec!["t".to_string()],
            }
        }

        fn batch(token: &str) -> OutputBatch {
            OutputBatch {
                token: token.to_string(),
                history_floor_event_id: 3,
                workflow_id: "wf".to_string(),
                run_id: "run".to_string(),
                first_run_id: "run".to_string(),
                records: vec![data("t", 1, 1)],
            }
        }

        #[tokio::test]
        async fn a_proven_stage_is_promoted_once_however_often_the_run_replays() {
            let log = Arc::new(Log::default());
            let store = OutputStore::new(log.clone(), "ns".to_string());
            for _ in 0..3 {
                store
                    .promote_proven("run", &[proven("a"), proven("b")])
                    .await;
            }
            assert_eq!(*log.0.lock(), ["promote a", "promote b"]);
        }

        #[tokio::test]
        async fn an_undecided_stage_that_replay_proves_is_not_aborted() {
            let log = Arc::new(Log::default());
            let store = OutputStore::new(log.clone(), "ns".to_string());
            store
                .settle(
                    "run",
                    vec![batch("kept"), batch("lost")],
                    CompletionOutcome::Unknown,
                )
                .await;
            store.promote_proven("run", &[proven("kept")]).await;
            store
                .settle("run", vec![], CompletionOutcome::Accepted)
                .await;
            assert_eq!(*log.0.lock(), ["promote kept", "abort lost"]);
        }

        #[tokio::test]
        async fn undecided_stages_belong_to_their_run() {
            let log = Arc::new(Log::default());
            let store = OutputStore::new(log.clone(), "ns".to_string());
            store
                .settle("other", vec![batch("theirs")], CompletionOutcome::Unknown)
                .await;
            store
                .settle("run", vec![batch("mine")], CompletionOutcome::Accepted)
                .await;
            assert_eq!(*log.0.lock(), ["promote mine"]);
        }
    }
}
