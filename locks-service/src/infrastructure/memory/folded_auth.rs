use std::sync::Arc;

use async_trait::async_trait;
use tokio::sync::RwLock;

use crate::application::errors::ApplicationError;
use crate::application::models::{
    EncryptedFoldedAuthFlowSecret, FOLDED_AUTH_TOMBSTONE_TTL, FoldedAuthAttempt,
    FoldedAuthSecretContext, FoldedAuthStartDisposition, FoldedAuthStartResult,
    FoldedAuthStateHash, FoldedAuthStatus, GraphTaskRecord, RestoredFoldedAuthAttempt,
    StartFoldedAuthAttempt,
};
use crate::application::ports::{FoldedAuthRepository, GraphTaskRepository};
use crate::infrastructure::folded_auth::FoldedAuthSecretCipher;

use super::graph_tasks::InMemoryGraphTaskRepository;

#[derive(Debug, Clone)]
struct StoredAttempt {
    attempt: FoldedAuthAttempt,
    encrypted_flow_secret: Option<EncryptedFoldedAuthFlowSecret>,
}

#[derive(Debug, Default)]
struct FoldedAuthMemoryState {
    records: Vec<StoredAttempt>,
}

#[derive(Debug, Clone)]
pub struct InMemoryFoldedAuthRepository {
    state: Arc<RwLock<FoldedAuthMemoryState>>,
    graph: Arc<InMemoryGraphTaskRepository>,
    cipher: FoldedAuthSecretCipher,
}

impl InMemoryFoldedAuthRepository {
    pub fn new(graph: Arc<InMemoryGraphTaskRepository>, cipher: FoldedAuthSecretCipher) -> Self {
        Self {
            state: Arc::new(RwLock::new(FoldedAuthMemoryState::default())),
            graph,
            cipher,
        }
    }

    pub fn reconstruct(&self, cipher: FoldedAuthSecretCipher) -> Self {
        Self {
            state: self.state.clone(),
            graph: self.graph.clone(),
            cipher,
        }
    }
}

#[async_trait]
impl FoldedAuthRepository for InMemoryFoldedAuthRepository {
    async fn get_latest_by_handle(
        &self,
        creator: &locks_core::ids::CreatorPubky,
        bundle_id: &locks_core::ids::BundleId,
    ) -> Result<Option<FoldedAuthAttempt>, ApplicationError> {
        Ok(self
            .state
            .read()
            .await
            .records
            .iter()
            .filter(|stored| {
                &stored.attempt.creator == creator && &stored.attempt.bundle_id == bundle_id
            })
            .max_by_key(|stored| stored.attempt.generation)
            .map(|stored| stored.attempt.clone()))
    }

    async fn accepted_graph_task(
        &self,
        state_hash: &FoldedAuthStateHash,
    ) -> Result<Option<GraphTaskRecord>, ApplicationError> {
        let Some(attempt) = self.get_by_state_hash(state_hash).await? else {
            return Ok(None);
        };
        if attempt.status != FoldedAuthStatus::Accepted {
            return Ok(None);
        }
        self.graph
            .get_graph_task_by_handle(&attempt.creator, &attempt.bundle_id)
            .await
    }

    async fn start_or_rotate(
        &self,
        request: StartFoldedAuthAttempt,
    ) -> Result<FoldedAuthStartResult, ApplicationError> {
        request.validate().map_err(invalid_state)?;
        let _admission = self
            .graph
            .acquire_admission(request.creator(), request.pubky_lock_resource.lock_id())
            .await;
        let _handle_admission = self
            .graph
            .acquire_handle_admission(request.creator(), request.bundle_id())
            .await;
        let mut state = self.state.write().await;
        if self
            .graph
            .has_graph_task_by_handle_under_admission(request.creator(), request.bundle_id())
            .await
        {
            return Err(ApplicationError::VerificationTaskConflict);
        }
        let database_now = self.graph.authoritative_time();
        let expires_at = database_now
            .checked_add(crate::application::models::FOLDED_AUTH_TTL)
            .ok_or_else(|| storage("folded auth expiry overflow"))?;
        if state
            .records
            .iter()
            .any(|stored| stored.attempt.state_hash == request.state_hash)
        {
            return Err(ApplicationError::VerificationTaskConflict);
        }
        let latest_index = state
            .records
            .iter()
            .enumerate()
            .filter(|(_, stored)| {
                stored.attempt.creator == *request.creator()
                    && stored.attempt.bundle_id == *request.bundle_id()
            })
            .max_by_key(|(_, stored)| stored.attempt.generation)
            .map(|(index, _)| index);
        let mut disposition = FoldedAuthStartDisposition::Started;
        let (generation, previous_terminal) = if let Some(index) = latest_index {
            let latest = &state.records[index].attempt;
            if !latest.exact_request_matches(&request)
                || latest.status == FoldedAuthStatus::Accepted
            {
                return Err(ApplicationError::VerificationTaskConflict);
            }
            let generation = latest
                .generation
                .checked_add(1)
                .ok_or_else(|| storage("folded auth generation overflow"))?;
            let previous_terminal = if latest.status == FoldedAuthStatus::Pending {
                let status = if latest.expires_at <= database_now {
                    FoldedAuthStatus::Expired
                } else {
                    disposition = FoldedAuthStartDisposition::Rotated;
                    FoldedAuthStatus::RestartRequired
                };
                Some((status, checked_purge_after(database_now)?))
            } else {
                None
            };
            (generation, previous_terminal)
        } else {
            (1, None)
        };
        let context = FoldedAuthSecretContext {
            creator: request.creator().clone(),
            bundle_id: request.bundle_id().clone(),
            generation,
            pubky_lock_resource: request.pubky_lock_resource.clone(),
            lock_hash: request.lock_hash,
        };
        let encrypted = self.cipher.encrypt(&context, &request.flow_secret)?;
        self.graph
            .reserve_folded_auth_under_admission(request.creator(), request.bundle_id())
            .await;
        let attempt = FoldedAuthAttempt {
            creator: request.creator().clone(),
            bundle_id: request.bundle_id().clone(),
            generation,
            state_hash: request.state_hash,
            status: FoldedAuthStatus::Pending,
            submitted_proof_bundle: request.submitted_proof_bundle,
            pubky_lock_resource: request.pubky_lock_resource,
            lock_hash: request.lock_hash,
            verification_window_seconds: request.verification_window_seconds,
            graph: request.graph,
            created_at: database_now,
            expires_at,
            poll_dispatched: false,
            terminal_at: None,
            purge_after: None,
            task_id: None,
        };
        if let (Some(index), Some((status, purge_after))) = (latest_index, previous_terminal) {
            terminalize(&mut state.records[index], status, database_now, purge_after);
        }
        state.records.push(StoredAttempt {
            attempt: attempt.clone(),
            encrypted_flow_secret: Some(encrypted),
        });
        Ok(FoldedAuthStartResult {
            attempt,
            disposition,
        })
    }

    async fn get_by_state_hash(
        &self,
        state_hash: &FoldedAuthStateHash,
    ) -> Result<Option<FoldedAuthAttempt>, ApplicationError> {
        Ok(self
            .state
            .read()
            .await
            .records
            .iter()
            .find(|stored| &stored.attempt.state_hash == state_hash)
            .map(|stored| stored.attempt.clone()))
    }

    async fn restore_pending(
        &self,
        state_hash: &FoldedAuthStateHash,
    ) -> Result<Option<RestoredFoldedAuthAttempt>, ApplicationError> {
        let stored = self
            .state
            .read()
            .await
            .records
            .iter()
            .find(|stored| &stored.attempt.state_hash == state_hash)
            .cloned();
        let Some(stored) = stored else {
            return Ok(None);
        };
        if stored.attempt.status != FoldedAuthStatus::Pending || stored.attempt.poll_dispatched {
            return Ok(None);
        }
        let encrypted = stored
            .encrypted_flow_secret
            .as_ref()
            .ok_or_else(|| invalid_state("pending folded auth attempt lacks flow secret"))?;
        let flow_secret = self.cipher.decrypt(&stored.attempt.context(), encrypted)?;
        Ok(Some(RestoredFoldedAuthAttempt {
            attempt: stored.attempt,
            flow_secret,
        }))
    }

    async fn mark_poll_dispatched(
        &self,
        state_hash: &FoldedAuthStateHash,
        expected_generation: u64,
        _now: time::OffsetDateTime,
    ) -> Result<Option<FoldedAuthAttempt>, ApplicationError> {
        let mut state = self.state.write().await;
        let Some(stored) = exact_pending_mut(&mut state, state_hash, expected_generation) else {
            return Ok(None);
        };
        if stored.attempt.poll_dispatched {
            return Ok(None);
        }
        stored.attempt.poll_dispatched = true;
        Ok(Some(stored.attempt.clone()))
    }

    async fn confirm_not_attempted(
        &self,
        state_hash: &FoldedAuthStateHash,
        expected_generation: u64,
        _now: time::OffsetDateTime,
    ) -> Result<Option<FoldedAuthAttempt>, ApplicationError> {
        let mut state = self.state.write().await;
        let Some(stored) = exact_pending_mut(&mut state, state_hash, expected_generation) else {
            return Ok(None);
        };
        if !stored.attempt.poll_dispatched {
            return Ok(None);
        }
        stored.attempt.poll_dispatched = false;
        Ok(Some(stored.attempt.clone()))
    }

    async fn confirm_pending_generation(
        &self,
        state_hash: &FoldedAuthStateHash,
        expected_generation: u64,
        _now: time::OffsetDateTime,
    ) -> Result<Option<FoldedAuthAttempt>, ApplicationError> {
        let mut state = self.state.write().await;
        let now = self.graph.authoritative_time();
        let Some(stored) = exact_pending_mut(&mut state, state_hash, expected_generation) else {
            return Ok(None);
        };
        if stored.attempt.expires_at <= now {
            let purge_after = checked_purge_after(now)?;
            terminalize(stored, FoldedAuthStatus::Expired, now, purge_after);
            self.graph
                .release_folded_auth_reservation(&stored.attempt.creator, &stored.attempt.bundle_id)
                .await;
        } else {
            stored.attempt.poll_dispatched = false;
        }
        Ok(Some(stored.attempt.clone()))
    }

    async fn require_restart(
        &self,
        state_hash: &FoldedAuthStateHash,
        expected_generation: u64,
        _now: time::OffsetDateTime,
    ) -> Result<Option<FoldedAuthAttempt>, ApplicationError> {
        let mut state = self.state.write().await;
        let now = self.graph.authoritative_time();
        let Some(stored) = exact_pending_mut(&mut state, state_hash, expected_generation) else {
            return Ok(None);
        };
        let purge_after = checked_purge_after(now)?;
        let status = if stored.attempt.expires_at <= now {
            FoldedAuthStatus::Expired
        } else {
            FoldedAuthStatus::RestartRequired
        };
        terminalize(stored, status, now, purge_after);
        self.graph
            .release_folded_auth_reservation(&stored.attempt.creator, &stored.attempt.bundle_id)
            .await;
        Ok(Some(stored.attempt.clone()))
    }

    async fn expire(
        &self,
        state_hash: &FoldedAuthStateHash,
        expected_generation: u64,
        _now: time::OffsetDateTime,
    ) -> Result<Option<FoldedAuthAttempt>, ApplicationError> {
        let mut state = self.state.write().await;
        let now = self.graph.authoritative_time();
        let Some(stored) = exact_pending_mut(&mut state, state_hash, expected_generation) else {
            return Ok(None);
        };
        if stored.attempt.expires_at > now {
            return Ok(Some(stored.attempt.clone()));
        }
        let purge_after = checked_purge_after(now)?;
        terminalize(stored, FoldedAuthStatus::Expired, now, purge_after);
        self.graph
            .release_folded_auth_reservation(&stored.attempt.creator, &stored.attempt.bundle_id)
            .await;
        Ok(Some(stored.attempt.clone()))
    }

    async fn accept_and_insert_graph_task(
        &self,
        state_hash: &FoldedAuthStateHash,
        expected_generation: u64,
        task_id: locks_core::ids::TaskId,
        reader: locks_core::ids::CreatorPubky,
        _now: time::OffsetDateTime,
    ) -> Result<Option<GraphTaskRecord>, ApplicationError> {
        let attempt = {
            let state = self.state.read().await;
            let Some(stored) = state.records.iter().find(|stored| {
                &stored.attempt.state_hash == state_hash
                    && stored.attempt.generation == expected_generation
                    && stored.attempt.status == FoldedAuthStatus::Pending
            }) else {
                return Ok(None);
            };
            stored.attempt.clone()
        };
        let _admission = self
            .graph
            .acquire_admission(&attempt.creator, attempt.pubky_lock_resource.lock_id())
            .await;
        let mut state = self.state.write().await;
        let now = self.graph.authoritative_time();
        let Some(stored) = exact_pending_mut(&mut state, state_hash, expected_generation) else {
            return Ok(None);
        };
        if stored.attempt.expires_at <= now {
            let purge_after = checked_purge_after(now)?;
            terminalize(stored, FoldedAuthStatus::Expired, now, purge_after);
            self.graph
                .release_folded_auth_reservation(&stored.attempt.creator, &stored.attempt.bundle_id)
                .await;
            return Ok(None);
        }
        let task = stored
            .attempt
            .accepted_graph_task(task_id, reader, now)
            .map_err(invalid_state)?;
        let purge_after = checked_purge_after(now)?;
        let inserted = self
            .graph
            .insert_graph_task_before_publish_under_admission(
                task,
                |inserted| {
                    stored.attempt.task_id = Some(inserted.task_id);
                    terminalize(stored, FoldedAuthStatus::Accepted, now, purge_after);
                    Ok(())
                },
                true,
            )
            .await?;
        Ok(Some(inserted))
    }

    async fn purge_terminal_before(
        &self,
        _now: time::OffsetDateTime,
    ) -> Result<u64, ApplicationError> {
        let mut state = self.state.write().await;
        let now = self.graph.authoritative_time();
        let purge_after = checked_purge_after(now)?;
        for stored in &mut state.records {
            if stored.attempt.status == FoldedAuthStatus::Pending {
                if stored.attempt.expires_at <= now {
                    terminalize(stored, FoldedAuthStatus::Expired, now, purge_after);
                    self.graph
                        .release_folded_auth_reservation(
                            &stored.attempt.creator,
                            &stored.attempt.bundle_id,
                        )
                        .await;
                } else if stored.attempt.poll_dispatched {
                    terminalize(stored, FoldedAuthStatus::RestartRequired, now, purge_after);
                    self.graph
                        .release_folded_auth_reservation(
                            &stored.attempt.creator,
                            &stored.attempt.bundle_id,
                        )
                        .await;
                }
            }
        }
        let before = state.records.len();
        state.records.retain(|stored| {
            stored.attempt.status == FoldedAuthStatus::Pending
                || stored
                    .attempt
                    .purge_after
                    .is_none_or(|purge_after| purge_after > now)
        });
        u64::try_from(before - state.records.len())
            .map_err(|_| storage("folded auth purge count overflow"))
    }
}

fn exact_pending_mut<'a>(
    state: &'a mut FoldedAuthMemoryState,
    state_hash: &FoldedAuthStateHash,
    generation: u64,
) -> Option<&'a mut StoredAttempt> {
    state.records.iter_mut().find(|stored| {
        &stored.attempt.state_hash == state_hash
            && stored.attempt.generation == generation
            && stored.attempt.status == FoldedAuthStatus::Pending
    })
}

fn terminalize(
    stored: &mut StoredAttempt,
    status: FoldedAuthStatus,
    now: time::OffsetDateTime,
    purge_after: time::OffsetDateTime,
) {
    stored.attempt.status = status;
    stored.attempt.poll_dispatched = false;
    stored.attempt.terminal_at = Some(now);
    stored.attempt.purge_after = Some(purge_after);
    stored.encrypted_flow_secret = None;
}

fn checked_purge_after(
    now: time::OffsetDateTime,
) -> Result<time::OffsetDateTime, ApplicationError> {
    now.checked_add(FOLDED_AUTH_TOMBSTONE_TTL)
        .ok_or_else(|| storage("folded auth tombstone expiry overflow"))
}

fn storage(message: &'static str) -> ApplicationError {
    ApplicationError::Storage {
        message: message.to_owned(),
    }
}

fn invalid_state(message: impl Into<String>) -> ApplicationError {
    ApplicationError::InvalidFoldedAuthState {
        message: message.into(),
    }
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicI64, Ordering};

    use async_trait::async_trait;
    use locks_core::ids::{
        BundleId, ContentLockPath, CreatorPubky, LockHash, LockId, PubkyLockResource, TaskId,
    };
    use locks_core::lock_policy::{GraphParticipantV2, GraphPolicyV2, GraphPredicateV2};
    use locks_core::verification::SubmittedProofBundleV2;
    use time::macros::datetime;
    use tokio::sync::Notify;

    use super::InMemoryFoldedAuthRepository;
    use crate::application::errors::ApplicationError;
    use crate::application::models::{
        FoldedAuthFlowSecret, FoldedAuthStateHash, GraphTaskRecord, GraphTaskSnapshot,
        GraphTaskSnapshotV1, StartFoldedAuthAttempt, VerificationTaskStatus,
    };
    use crate::application::ports::{
        Clock, FoldedAuthApprovalPoll, FoldedAuthApprovalPoller, FoldedAuthPollCall,
        FoldedAuthRepository, GraphTaskRepository,
    };
    use crate::application::use_cases::poll_folded_auth::{
        PollFoldedAuthAttempt, PollFoldedAuthOutcome,
    };
    use crate::infrastructure::folded_auth::FoldedAuthSecretCipher;
    use crate::infrastructure::memory::graph_tasks::InMemoryGraphTaskRepository;
    use crate::infrastructure::memory::verification_task_deletion_fence::InMemoryVerificationTaskDeletionFence;
    use crate::infrastructure::runtime_master_key::RuntimeMasterKey;

    #[tokio::test]
    async fn folded_start_and_ordinary_graph_admission_cannot_both_win() {
        let graph = Arc::new(InMemoryGraphTaskRepository::new());
        let repository = Arc::new(InMemoryFoldedAuthRepository::new(
            Arc::clone(&graph),
            cipher(),
        ));
        let creator = creator();
        let bundle_id = BundleId::from_str("000G40R40M30E209185GR38E1W").unwrap();
        let resource =
            PubkyLockResource::new(creator.clone(), ContentLockPath::from_lock_id(lock_id()));
        let admission = graph.acquire_admission(&creator, resource.lock_id()).await;

        let starting = {
            let repository = Arc::clone(&repository);
            let request = start_request(bundle_id.clone(), resource.clone());
            tokio::spawn(async move { repository.start_or_rotate(request).await })
        };
        tokio::task::yield_now().await;
        assert!(
            !starting.is_finished(),
            "folded start must wait for shared graph admission"
        );
        let inserting = {
            let graph = Arc::clone(&graph);
            let task = graph_task(creator.clone(), bundle_id.clone(), resource);
            tokio::spawn(async move { graph.insert_graph_task(task).await })
        };
        drop(admission);

        let start_result = starting.await.unwrap();
        let insert_result = inserting.await.unwrap();
        assert_ne!(start_result.is_ok(), insert_result.is_ok());
        let losing_error = start_result
            .err()
            .or_else(|| insert_result.err())
            .expect("one admission must lose");
        assert!(matches!(
            losing_error,
            ApplicationError::VerificationTaskConflict
        ));
        let has_attempt = repository
            .get_by_state_hash(&FoldedAuthStateHash::derive(b"admission-race"))
            .await
            .unwrap()
            .is_some();
        let has_task = graph
            .get_graph_task_by_handle(&creator, &bundle_id)
            .await
            .unwrap()
            .is_some();
        assert_ne!(has_attempt, has_task);
    }

    #[tokio::test]
    async fn terminal_folded_attempt_releases_handle_reservation_for_ordinary_admission() {
        let graph = Arc::new(InMemoryGraphTaskRepository::new());
        let repository = InMemoryFoldedAuthRepository::new(Arc::clone(&graph), cipher());
        let creator = creator();
        let bundle_id = BundleId::from_str("000G40R40M30E209185GR38E1W").unwrap();
        let resource =
            PubkyLockResource::new(creator.clone(), ContentLockPath::from_lock_id(lock_id()));
        let started = repository
            .start_or_rotate(start_request(bundle_id.clone(), resource.clone()))
            .await
            .unwrap();

        repository
            .require_restart(
                &started.attempt.state_hash,
                started.attempt.generation,
                datetime!(2026-10-30 12:01:00 UTC),
            )
            .await
            .unwrap()
            .unwrap();

        graph
            .insert_graph_task(graph_task(creator, bundle_id, resource))
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn acceptance_samples_authoritative_time_after_waiting_for_admission() {
        let now = datetime!(2026-10-30 12:00:00 UTC);
        let clock = Arc::new(MutableClock::new(now));
        let fence = Arc::new(InMemoryVerificationTaskDeletionFence::with_clock(
            clock.clone(),
        ));
        let graph = Arc::new(InMemoryGraphTaskRepository::with_deletion_fence(fence));
        let repository = Arc::new(InMemoryFoldedAuthRepository::new(
            Arc::clone(&graph),
            cipher(),
        ));
        let creator = creator();
        let bundle_id = BundleId::from_str("000G40R40M30E209185GR38E1W").unwrap();
        let resource =
            PubkyLockResource::new(creator.clone(), ContentLockPath::from_lock_id(lock_id()));
        let started = repository
            .start_or_rotate(start_request(bundle_id.clone(), resource.clone()))
            .await
            .unwrap();
        let admission = graph.acquire_admission(&creator, resource.lock_id()).await;
        let entered = Arc::new(Notify::new());
        let use_case = Arc::new(PollFoldedAuthAttempt::new(
            Arc::clone(&repository),
            Arc::new(ApprovedPoller {
                entered: Arc::clone(&entered),
                reader: creator.clone(),
            }),
            clock.clone(),
        ));
        let state_hash = started.attempt.state_hash.clone();
        let polling = tokio::spawn(async move { use_case.execute(&state_hash).await.unwrap() });
        entered.notified().await;
        tokio::task::yield_now().await;
        clock.set(started.attempt.expires_at);
        drop(admission);

        assert!(matches!(
            polling.await.unwrap(),
            PollFoldedAuthOutcome::Expired(_)
        ));
        assert!(
            graph
                .get_graph_task_by_handle(&creator, &bundle_id)
                .await
                .unwrap()
                .is_none()
        );
    }

    struct ApprovedPoller {
        entered: Arc<Notify>,
        reader: CreatorPubky,
    }

    #[async_trait]
    impl FoldedAuthApprovalPoller for ApprovedPoller {
        async fn poll_approval(&self, _flow_secret: &FoldedAuthFlowSecret) -> FoldedAuthPollCall {
            self.entered.notify_one();
            FoldedAuthPollCall::Attempted(Ok(FoldedAuthApprovalPoll::Approved {
                reader: self.reader.clone(),
            }))
        }
    }

    struct MutableClock(AtomicI64);

    impl MutableClock {
        fn new(now: time::OffsetDateTime) -> Self {
            Self(AtomicI64::new(now.unix_timestamp()))
        }

        fn set(&self, now: time::OffsetDateTime) {
            self.0.store(now.unix_timestamp(), Ordering::SeqCst);
        }
    }

    impl Clock for MutableClock {
        fn now(&self) -> time::OffsetDateTime {
            time::OffsetDateTime::from_unix_timestamp(self.0.load(Ordering::SeqCst)).unwrap()
        }
    }

    fn start_request(bundle_id: BundleId, resource: PubkyLockResource) -> StartFoldedAuthAttempt {
        StartFoldedAuthAttempt {
            submitted_proof_bundle: SubmittedProofBundleV2 {
                version: 2,
                bundle_id,
                pubky_lock_resource: resource.clone(),
                reader_pubky: None,
            },
            pubky_lock_resource: resource,
            lock_hash: [9; 32],
            verification_window_seconds: 300,
            graph: graph_policy(),
            state_hash: FoldedAuthStateHash::derive(b"admission-race"),
            flow_secret: FoldedAuthFlowSecret::new(b"admission-race-secret".to_vec()),
            created_at: datetime!(2026-10-30 12:00:00 UTC),
        }
    }

    fn graph_task(
        creator: CreatorPubky,
        bundle_id: BundleId,
        resource: PubkyLockResource,
    ) -> GraphTaskRecord {
        GraphTaskRecord {
            task_id: TaskId::from_str("018fc6ec-2f3d-4f7e-8b7d-6f5c4b3a2d10").unwrap(),
            snapshot: GraphTaskSnapshot::V1(
                GraphTaskSnapshotV1::new(
                    creator.clone(),
                    bundle_id,
                    resource,
                    [9; 32],
                    creator,
                    datetime!(2026-10-30 12:00:00 UTC),
                    300,
                    graph_policy(),
                )
                .unwrap(),
            ),
            evaluator_revision: 0,
            status: VerificationTaskStatus::Pending,
            entitlement_to_publish: None,
            entitlement_publication_token: None,
            completed_at: None,
        }
    }

    fn graph_policy() -> GraphPolicyV2 {
        GraphPolicyV2 {
            predicates: vec![GraphPredicateV2::Follows {
                follower: GraphParticipantV2::Reader,
                followed: GraphParticipantV2::Creator,
            }],
            operator: None,
        }
    }

    fn lock_id() -> LockId {
        LockId::from_hash(LockHash::from_bytes([9; 32]))
    }

    fn creator() -> CreatorPubky {
        CreatorPubky::from_str("pubkytkrq8zmwb8a3m9k15csu3q17qmfgqnp9dskbrg9uq1rydpyxp7qy").unwrap()
    }

    fn cipher() -> FoldedAuthSecretCipher {
        let master_key =
            RuntimeMasterKey::from_base64url("AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA")
                .unwrap();
        FoldedAuthSecretCipher::from_runtime_master_key(&master_key)
    }
}
