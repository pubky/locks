use std::sync::Arc;
use std::time::Duration;

use locks_core::ids::TaskId;
use locks_service::application::errors::ApplicationError;
use locks_service::application::mixed_v2_reducer::reduce_mixed_snapshot;
use locks_service::application::models::{
    MixedRootState, MixedTaskRecord, MixedTaskSnapshot, MixedTaskSnapshotV1, PaymentInvoiceState,
    PaymentOperationalErrorCategory, PaymentProviderCreateRequest, PaymentRequestStateV2,
    PaymentStateV2, VerificationTaskStatus, VerificationTerminalReason,
};
use locks_service::application::ports::{
    Clock, EntitlementRepository, MixedTaskClaimer, PaymentProviderClient, PaymentProviderError,
    PaymentProviderErrorKind,
};

use crate::graph_worker::{GraphRetryJitter, apply_observation, next_due as next_graph_due};
use crate::nexus_http_client::{NexusGraphVerifier, TimedNexusObservation};

const STATUS_REPOLL_SECONDS: i64 = 5;
const BACKOFF_SECONDS: [u64; 6] = [1, 2, 4, 8, 16, 30];
const PER_TASK_CONCURRENCY: usize = 8;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MixedWorkerTick {
    Idle,
    RetryScheduled(TaskId),
    Terminal(TaskId),
    PublicationDeferred(TaskId),
    ClaimLost,
}

pub struct MixedVerificationWorker {
    claimer: Arc<dyn MixedTaskClaimer>,
    entitlements: Arc<dyn EntitlementRepository>,
    provider: Arc<dyn PaymentProviderClient>,
    verifier: Arc<dyn NexusGraphVerifier>,
    clock: Arc<dyn Clock>,
    jitter: Arc<dyn GraphRetryJitter>,
    worker_id: String,
    claim_timeout: time::Duration,
}

impl MixedVerificationWorker {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        claimer: Arc<dyn MixedTaskClaimer>,
        entitlements: Arc<dyn EntitlementRepository>,
        provider: Arc<dyn PaymentProviderClient>,
        verifier: Arc<dyn NexusGraphVerifier>,
        clock: Arc<dyn Clock>,
        jitter: Arc<dyn GraphRetryJitter>,
        worker_id: String,
        claim_timeout: time::Duration,
    ) -> Self {
        Self {
            claimer,
            entitlements,
            provider,
            verifier,
            clock,
            jitter,
            worker_id,
            claim_timeout,
        }
    }

    pub async fn run_once(&self) -> Result<MixedWorkerTick, ApplicationError> {
        self.run_once_with_shutdown(None).await
    }

    async fn run_once_with_shutdown(
        &self,
        shutdown: Option<&tokio::sync::watch::Receiver<bool>>,
    ) -> Result<MixedWorkerTick, ApplicationError> {
        if shutdown.is_some_and(|value| *value.borrow()) {
            return Ok(MixedWorkerTick::Idle);
        }
        let claim_now = self.clock.now();
        let Some(claim) = self
            .claimer
            .claim_next_mixed_task(&self.worker_id, claim_now, claim_now + self.claim_timeout)
            .await?
        else {
            return Ok(MixedWorkerTick::Idle);
        };
        let task_id = claim.task.task_id;
        let claim_token = claim.claim_token;
        let revision = claim.task.evaluator_revision;
        if shutdown.is_some_and(|value| *value.borrow()) {
            self.claimer
                .release_claimed_mixed_task(&task_id, &self.worker_id, &claim_token)
                .await?;
            return Ok(MixedWorkerTick::ClaimLost);
        }
        if claim.task.status == VerificationTaskStatus::PublishingEntitlement {
            return self
                .publish(claim.task, claim_token, revision, shutdown)
                .await;
        }
        if claim.task.snapshot.as_v1().root != MixedRootState::Pending {
            return self
                .persist(
                    task_id,
                    claim_token,
                    revision,
                    claim.task.snapshot,
                    self.clock.now(),
                    shutdown,
                )
                .await;
        }
        let mut snapshot = claim.task.snapshot;
        let dispatch_now = self.clock.now();
        if dispatch_now >= snapshot.as_v1().deadline_at {
            reduce_mixed_snapshot(snapshot_v1_mut(&mut snapshot), dispatch_now)
                .map_err(invalid_snapshot)?;
            return self
                .persist_or_schedule(
                    task_id,
                    claim_token,
                    revision,
                    snapshot,
                    dispatch_now,
                    shutdown,
                )
                .await;
        }

        if matches!(
            snapshot.as_v1().payment.invoice,
            PaymentInvoiceState::Pending
        ) {
            let state = snapshot.as_v1();
            let request = PaymentProviderCreateRequest {
                bundle_id: state.bundle_id.clone(),
                lock_resource: state.pubky_lock_resource.clone(),
                reader: state.reader.clone(),
                payment: state.payment.payment.clone(),
                deadline: state.deadline_at,
            };
            match self.provider.create_invoice(&request).await {
                Ok(created) => {
                    let now = self.clock.now();
                    let mut payment = snapshot
                        .as_v1()
                        .payment_snapshot()
                        .map_err(invalid_snapshot)?;
                    if payment
                        .apply_create_response(
                            created.invoice_created_at,
                            created.proposal_expires_at,
                            created.deadline,
                        )
                        .is_err()
                    {
                        snapshot_v1_mut(&mut snapshot).root = MixedRootState::Failed(
                            VerificationTerminalReason::InvalidVerifierResponse,
                        );
                    } else {
                        snapshot_v1_mut(&mut snapshot).replace_payment_snapshot(payment);
                        reduce_mixed_snapshot(snapshot_v1_mut(&mut snapshot), now)
                            .map_err(invalid_snapshot)?;
                    }
                    return self
                        .persist_or_schedule(
                            task_id,
                            claim_token,
                            revision,
                            snapshot,
                            now,
                            shutdown,
                        )
                        .await;
                }
                Err(error) => {
                    return self
                        .handle_provider_error(
                            task_id,
                            claim_token,
                            revision,
                            snapshot,
                            error,
                            self.clock.now(),
                            shutdown,
                        )
                        .await;
                }
            }
        }

        let creator = snapshot.as_v1().creator.clone();
        let bundle = snapshot.as_v1().bundle_id.clone();
        match self.provider.invoice_status(&creator, &bundle).await {
            Ok(status) => {
                let now = self.clock.now();
                let mut payment = snapshot
                    .as_v1()
                    .payment_snapshot()
                    .map_err(invalid_snapshot)?;
                if payment.apply_status(status).is_err() {
                    snapshot_v1_mut(&mut snapshot).root =
                        MixedRootState::Failed(VerificationTerminalReason::InvalidVerifierResponse);
                    return self
                        .persist(task_id, claim_token, revision, snapshot, now, shutdown)
                        .await;
                }
                snapshot_v1_mut(&mut snapshot).replace_payment_snapshot(payment);
                let status = snapshot
                    .as_v1()
                    .payment
                    .provider_status
                    .as_ref()
                    .expect("status was applied to validated mixed snapshot");
                let eligible_live = matches!(
                    status.request_state,
                    PaymentRequestStateV2::Accepted
                        | PaymentRequestStateV2::ProofSubmitted
                        | PaymentRequestStateV2::ActiveRecurring
                ) && status.payment_state != PaymentStateV2::Expired;
                if !eligible_live {
                    reduce_mixed_snapshot(snapshot_v1_mut(&mut snapshot), now)
                        .map_err(invalid_snapshot)?;
                    return self
                        .persist_or_schedule(
                            task_id,
                            claim_token,
                            revision,
                            snapshot,
                            now,
                            shutdown,
                        )
                        .await;
                }
            }
            Err(error) => {
                return self
                    .handle_provider_error(
                        task_id,
                        claim_token,
                        revision,
                        snapshot,
                        error,
                        self.clock.now(),
                        shutdown,
                    )
                    .await;
            }
        }

        let dispatch_now = self.clock.now();
        let deadline = snapshot.as_v1().deadline_at;
        if dispatch_now >= deadline {
            reduce_mixed_snapshot(snapshot_v1_mut(&mut snapshot), dispatch_now)
                .map_err(invalid_snapshot)?;
            return self
                .persist_or_schedule(
                    task_id,
                    claim_token,
                    revision,
                    snapshot,
                    dispatch_now,
                    shutdown,
                )
                .await;
        }
        let mut graph = snapshot
            .as_v1()
            .graph_snapshot()
            .map_err(invalid_snapshot)?;
        let due: Vec<_> = graph
            .predicates
            .iter()
            .enumerate()
            .filter(|(_, state)| {
                state.first_true_observation.is_none()
                    && !state.terminal_invalid
                    && state.next_due_at.is_none_or(|due| due <= dispatch_now)
            })
            .map(|(index, state)| (index, state.predicate.clone()))
            .take(PER_TASK_CONCURRENCY)
            .collect();
        let creator = snapshot.as_v1().creator.clone();
        let reader = snapshot.as_v1().reader.clone();
        let mut set = tokio::task::JoinSet::new();
        for (index, predicate) in due {
            if self.clock.now() >= deadline {
                break;
            }
            let verifier = Arc::clone(&self.verifier);
            let clock = Arc::clone(&self.clock);
            let shutdown = shutdown.cloned();
            let creator = creator.clone();
            let reader = reader.clone();
            set.spawn(async move {
                if clock.now() >= deadline || shutdown.as_ref().is_some_and(|value| *value.borrow())
                {
                    return None;
                }
                Some((
                    index,
                    verifier
                        .verify_predicate(&predicate, &creator, &reader, deadline)
                        .await,
                ))
            });
        }
        while let Some(result) = set.join_next().await {
            let Some((
                index,
                TimedNexusObservation {
                    observation,
                    observed_at,
                },
            )) = result
                .map_err(|error| storage(format!("mixed graph verifier task failed: {error}")))?
            else {
                continue;
            };
            if observed_at <= deadline {
                apply_observation(
                    &mut graph,
                    index,
                    observation,
                    observed_at,
                    deadline,
                    self.jitter.as_ref(),
                )?;
            }
        }
        snapshot_v1_mut(&mut snapshot).replace_graph_snapshot(graph);
        let now = self.clock.now();
        reduce_mixed_snapshot(snapshot_v1_mut(&mut snapshot), now).map_err(invalid_snapshot)?;
        self.persist_or_schedule(task_id, claim_token, revision, snapshot, now, shutdown)
            .await
    }

    #[allow(clippy::too_many_arguments)]
    async fn handle_provider_error(
        &self,
        task_id: TaskId,
        claim_token: uuid::Uuid,
        revision: u64,
        mut snapshot: MixedTaskSnapshot,
        error: PaymentProviderError,
        observed_at: time::OffsetDateTime,
        shutdown: Option<&tokio::sync::watch::Receiver<bool>>,
    ) -> Result<MixedWorkerTick, ApplicationError> {
        match error.kind {
            PaymentProviderErrorKind::InvalidResponse => {
                snapshot_v1_mut(&mut snapshot).root =
                    MixedRootState::Failed(VerificationTerminalReason::InvalidVerifierResponse)
            }
            PaymentProviderErrorKind::Conflict => {
                snapshot_v1_mut(&mut snapshot).root =
                    MixedRootState::Failed(VerificationTerminalReason::VerificationConflict)
            }
            PaymentProviderErrorKind::NotFound | PaymentProviderErrorKind::Retryable => {
                let state = snapshot_v1_mut(&mut snapshot);
                let failure_at = observed_at.min(state.deadline_at);
                state.payment.operational_failure_count =
                    state.payment.operational_failure_count.saturating_add(1);
                state.payment.operational_failure_total =
                    state.payment.operational_failure_total.saturating_add(1);
                state
                    .payment
                    .operational_first_failure_at
                    .get_or_insert(failure_at);
                state.payment.operational_last_failure_at = Some(failure_at);
                state.payment.operational_last_category =
                    Some(PaymentOperationalErrorCategory::Transport);
                if observed_at < state.deadline_at {
                    let index =
                        usize::try_from(state.payment.operational_failure_count.saturating_sub(1))
                            .unwrap_or(usize::MAX)
                            .min(BACKOFF_SECONDS.len() - 1);
                    let seconds = error
                        .retry_after_seconds
                        .unwrap_or(BACKOFF_SECONDS[index])
                        .clamp(1, 60);
                    let due = (observed_at
                        + time::Duration::seconds(i64::try_from(seconds).unwrap_or(60)))
                    .min(state.deadline_at);
                    state.payment.next_due_at = Some(due);
                    return self
                        .schedule(task_id, claim_token, revision, snapshot, observed_at, due)
                        .await;
                }
                state.root = MixedRootState::Failed(
                    VerificationTerminalReason::VerificationUnavailableByDeadline,
                );
                state.satisfied_by.clear();
            }
        }
        self.persist(
            task_id,
            claim_token,
            revision,
            snapshot,
            observed_at,
            shutdown,
        )
        .await
    }

    async fn persist_or_schedule(
        &self,
        task_id: TaskId,
        claim_token: uuid::Uuid,
        revision: u64,
        snapshot: MixedTaskSnapshot,
        now: time::OffsetDateTime,
        shutdown: Option<&tokio::sync::watch::Receiver<bool>>,
    ) -> Result<MixedWorkerTick, ApplicationError> {
        if snapshot.as_v1().root != MixedRootState::Pending {
            return self
                .persist(task_id, claim_token, revision, snapshot, now, shutdown)
                .await;
        }
        let graph = snapshot
            .as_v1()
            .graph_snapshot()
            .map_err(invalid_snapshot)?;
        let payment_due = snapshot
            .as_v1()
            .payment
            .next_due_at
            .unwrap_or(now + time::Duration::seconds(STATUS_REPOLL_SECONDS));
        let due = payment_due
            .min(next_graph_due(&graph, now))
            .min(snapshot.as_v1().deadline_at)
            .max(now + time::Duration::milliseconds(1));
        self.schedule(task_id, claim_token, revision, snapshot, now, due)
            .await
    }

    async fn persist(
        &self,
        task_id: TaskId,
        claim_token: uuid::Uuid,
        revision: u64,
        snapshot: MixedTaskSnapshot,
        now: time::OffsetDateTime,
        shutdown: Option<&tokio::sync::watch::Receiver<bool>>,
    ) -> Result<MixedWorkerTick, ApplicationError> {
        let Some(persisted) = self
            .claimer
            .persist_claimed_mixed_snapshot(
                &task_id,
                &self.worker_id,
                &claim_token,
                now,
                revision,
                snapshot,
            )
            .await?
        else {
            return Ok(MixedWorkerTick::ClaimLost);
        };
        if persisted.status == VerificationTaskStatus::PublishingEntitlement {
            self.publish(persisted, claim_token, revision + 1, shutdown)
                .await
        } else {
            Ok(MixedWorkerTick::Terminal(task_id))
        }
    }

    async fn schedule(
        &self,
        task_id: TaskId,
        claim_token: uuid::Uuid,
        revision: u64,
        snapshot: MixedTaskSnapshot,
        now: time::OffsetDateTime,
        due: time::OffsetDateTime,
    ) -> Result<MixedWorkerTick, ApplicationError> {
        Ok(
            if self
                .claimer
                .schedule_mixed_task_retry(
                    &task_id,
                    &self.worker_id,
                    &claim_token,
                    now,
                    revision,
                    snapshot,
                    due,
                )
                .await?
                .is_some()
            {
                MixedWorkerTick::RetryScheduled(task_id)
            } else {
                MixedWorkerTick::ClaimLost
            },
        )
    }

    async fn publish(
        &self,
        task: MixedTaskRecord,
        claim_token: uuid::Uuid,
        revision: u64,
        shutdown: Option<&tokio::sync::watch::Receiver<bool>>,
    ) -> Result<MixedWorkerTick, ApplicationError> {
        let task_id = task.task_id;
        let payload = task.entitlement_to_publish.ok_or_else(|| {
            storage("publishing mixed task is missing durable entitlement payload")
        })?;
        let publication_token = task
            .entitlement_publication_token
            .ok_or_else(|| storage("publishing mixed task is missing external-effect fence"))?;
        let Some(mut action) = self
            .claimer
            .acquire_mixed_publication_action(
                &task_id,
                &self.worker_id,
                &claim_token,
                revision,
                publication_token,
            )
            .await?
        else {
            return Ok(MixedWorkerTick::ClaimLost);
        };
        if shutdown.is_some_and(|value| *value.borrow()) {
            action.release().await?;
            self.claimer
                .release_claimed_mixed_task(&task_id, &self.worker_id, &claim_token)
                .await?;
            return Ok(MixedWorkerTick::ClaimLost);
        }
        let outcome = async {
            match self
                .entitlements
                .get_verified_proof_bundle_v2(&payload.creator, &payload.bundle_id)
                .await
            {
                Ok(Some(existing)) if existing == payload => {}
                Ok(Some(_)) => {
                    return Err(storage(
                        "published v2 entitlement conflicts with durable mixed payload",
                    ));
                }
                Ok(None) => {
                    if self
                        .entitlements
                        .insert_verified_proof_bundle_v2(payload.clone())
                        .await
                        .is_err()
                    {
                        return Ok(MixedWorkerTick::PublicationDeferred(task_id));
                    }
                    if self
                        .entitlements
                        .get_verified_proof_bundle_v2(&payload.creator, &payload.bundle_id)
                        .await
                        .ok()
                        .flatten()
                        .as_ref()
                        != Some(&payload)
                    {
                        return Ok(MixedWorkerTick::PublicationDeferred(task_id));
                    }
                }
                Err(_) => return Ok(MixedWorkerTick::PublicationDeferred(task_id)),
            }
            Ok(if action.complete().await?.is_some() {
                MixedWorkerTick::Terminal(task_id)
            } else {
                MixedWorkerTick::ClaimLost
            })
        }
        .await;
        let released = action.release().await;
        match (outcome, released) {
            (Err(error), _) => Err(error),
            (Ok(_), Err(error)) => Err(error),
            (Ok(tick), Ok(())) => Ok(tick),
        }
    }

    pub async fn run_until_shutdown(
        &self,
        mut shutdown: tokio::sync::watch::Receiver<bool>,
        poll_interval: Duration,
    ) -> Result<(), ApplicationError> {
        loop {
            if *shutdown.borrow() {
                return Ok(());
            }
            let tick = self.run_once_with_shutdown(Some(&shutdown)).await?;
            if matches!(
                tick,
                MixedWorkerTick::Idle | MixedWorkerTick::PublicationDeferred(_)
            ) {
                tokio::select! {
                    changed = shutdown.changed() => if changed.is_err() || *shutdown.borrow() { return Ok(()); },
                    _ = tokio::time::sleep(poll_interval) => {}
                }
            }
        }
    }
}

fn snapshot_v1_mut(snapshot: &mut MixedTaskSnapshot) -> &mut MixedTaskSnapshotV1 {
    match snapshot {
        MixedTaskSnapshot::V1(value) => value,
    }
}
fn invalid_snapshot(message: String) -> ApplicationError {
    storage(format!("invalid mixed worker snapshot: {message}"))
}
fn storage(message: impl Into<String>) -> ApplicationError {
    ApplicationError::Storage {
        message: message.into(),
    }
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use async_trait::async_trait;
    use locks_core::ids::{BundleId, CreatorPubky, LockHash, LockId, PubkyLockResource, TaskId};
    use locks_core::lock_policy::{
        GraphParticipantV2, GraphPolicyV2, GraphPredicateV2, PaymentPolicyV2,
    };
    use locks_core::verification::{SUBMITTED_PROOF_BUNDLE_V2_VERSION, SubmittedProofBundleV2};
    use locks_service::application::models::{
        BitcoinOutpointV2, PaymentAssetV2, PaymentProviderStatus, PaymentSampleStatusV2,
        PaymentSampleV2,
    };
    use locks_service::application::ports::{MixedTaskRepository, PaymentProviderCreateResponse};
    use locks_service::infrastructure::memory::{
        entitlements::InMemoryEntitlementRepository, mixed_tasks::InMemoryMixedTaskRepository,
        verification_task_deletion_fence::InMemoryVerificationTaskDeletionFence,
    };
    use time::macros::datetime;

    use crate::nexus_http_client::NexusObservation;

    use super::*;

    const CREATOR: &str = "pubkytkrq8zmwb8a3m9k15csu3q17qmfgqnp9dskbrg9uq1rydpyxp7qy";
    const READER: &str = "pubkyorhzqdiexwmi6iidktucgud63ufa5nwtsuzdxe176a8izd6jsqky";
    const NOW: time::OffsetDateTime = datetime!(2026-11-05 12:00:00 UTC);

    struct FixedClock(Mutex<time::OffsetDateTime>);

    impl FixedClock {
        fn set(&self, now: time::OffsetDateTime) {
            *self.0.lock().unwrap() = now;
        }
    }

    impl Clock for FixedClock {
        fn now(&self) -> time::OffsetDateTime {
            *self.0.lock().unwrap()
        }
    }

    struct NoJitter;

    impl GraphRetryJitter for NoJitter {
        fn jitter(&self, base: Duration) -> Duration {
            base
        }
    }

    struct StatusProvider(PaymentProviderStatus);

    #[async_trait]
    impl PaymentProviderClient for StatusProvider {
        async fn create_invoice(
            &self,
            request: &PaymentProviderCreateRequest,
        ) -> Result<PaymentProviderCreateResponse, PaymentProviderError> {
            Ok(PaymentProviderCreateResponse {
                invoice_created_at: NOW + time::Duration::seconds(1),
                proposal_expires_at: NOW + time::Duration::seconds(60),
                deadline: request.deadline,
            })
        }

        async fn invoice_status(
            &self,
            _creator: &CreatorPubky,
            _bundle_id: &BundleId,
        ) -> Result<PaymentProviderStatus, PaymentProviderError> {
            Ok(self.0.clone())
        }
    }

    struct CountingNexus(AtomicUsize);

    #[async_trait]
    impl NexusGraphVerifier for CountingNexus {
        async fn verify_predicate(
            &self,
            _predicate: &GraphPredicateV2,
            _creator: &CreatorPubky,
            _reader: &CreatorPubky,
            _not_after: time::OffsetDateTime,
        ) -> TimedNexusObservation {
            self.0.fetch_add(1, Ordering::SeqCst);
            TimedNexusObservation {
                observation: NexusObservation::Valid {
                    value: true,
                    origin: "https://nexus.example".to_owned(),
                    status: 200,
                },
                observed_at: NOW,
            }
        }
    }

    #[tokio::test]
    async fn accepted_undetected_payment_never_publishes_graph_entitlement() {
        let clock = Arc::new(FixedClock(Mutex::new(NOW)));
        let fence = Arc::new(InMemoryVerificationTaskDeletionFence::with_clock(
            clock.clone(),
        ));
        let repository = Arc::new(InMemoryMixedTaskRepository::with_deletion_fence(fence));
        let task = mixed_task();
        repository.insert_mixed_task(task.clone()).await.unwrap();
        let nexus = Arc::new(CountingNexus(AtomicUsize::new(0)));
        let provider = Arc::new(StatusProvider(provider_status(
            PaymentRequestStateV2::Accepted,
        )));
        let entitlements = Arc::new(InMemoryEntitlementRepository::new());
        let worker = MixedVerificationWorker::new(
            repository.clone(),
            entitlements.clone(),
            provider,
            nexus.clone(),
            clock.clone(),
            Arc::new(NoJitter),
            "mixed-worker".to_owned(),
            time::Duration::minutes(1),
        );

        assert_eq!(
            worker.run_once().await.unwrap(),
            MixedWorkerTick::RetryScheduled(task.task_id)
        );
        clock.set(NOW + time::Duration::seconds(6));
        assert_eq!(
            worker.run_once().await.unwrap(),
            MixedWorkerTick::RetryScheduled(task.task_id)
        );
        assert_eq!(nexus.0.load(Ordering::SeqCst), 1);
        let pending = repository
            .get_mixed_task_by_handle(
                &task.snapshot.as_v1().creator,
                &task.snapshot.as_v1().bundle_id,
            )
            .await
            .unwrap()
            .unwrap();
        assert_eq!(pending.status, VerificationTaskStatus::Pending);
        assert!(pending.snapshot.as_v1().satisfied_by.is_empty());
        assert!(
            entitlements
                .get_verified_proof_bundle_v2(
                    &task.snapshot.as_v1().creator,
                    &task.snapshot.as_v1().bundle_id,
                )
                .await
                .unwrap()
                .is_none()
        );

        clock.set(NOW + time::Duration::minutes(5));
        assert_eq!(
            worker.run_once().await.unwrap(),
            MixedWorkerTick::Terminal(task.task_id)
        );
        let expired = repository
            .get_mixed_task_by_handle(
                &task.snapshot.as_v1().creator,
                &task.snapshot.as_v1().bundle_id,
            )
            .await
            .unwrap()
            .unwrap();
        assert_eq!(expired.status, VerificationTaskStatus::Failed);
        assert_eq!(
            expired.terminal_reason,
            Some(VerificationTerminalReason::VerificationUnavailableByDeadline)
        );
        assert!(
            entitlements
                .get_verified_proof_bundle_v2(
                    &task.snapshot.as_v1().creator,
                    &task.snapshot.as_v1().bundle_id,
                )
                .await
                .unwrap()
                .is_none()
        );
    }

    #[tokio::test]
    async fn detected_amount_matched_payment_and_graph_publish_zero_conf_entitlement() {
        let clock = Arc::new(FixedClock(Mutex::new(NOW)));
        let fence = Arc::new(InMemoryVerificationTaskDeletionFence::with_clock(
            clock.clone(),
        ));
        let repository = Arc::new(InMemoryMixedTaskRepository::with_deletion_fence(fence));
        let task = mixed_task();
        repository.insert_mixed_task(task.clone()).await.unwrap();
        let nexus = Arc::new(CountingNexus(AtomicUsize::new(0)));
        let mut status = provider_status(PaymentRequestStateV2::Accepted);
        status.payment_state = PaymentStateV2::Detected;
        status.current_sample = Some(PaymentSampleV2 {
            observed_at: NOW + time::Duration::seconds(5),
            status: PaymentSampleStatusV2::Detected,
            outpoint: Some(BitcoinOutpointV2 {
                txid: "10".repeat(32),
                vout: 0,
            }),
            confirmations: 0,
            amount_matched: true,
        });
        let entitlements = Arc::new(InMemoryEntitlementRepository::new());
        let worker = MixedVerificationWorker::new(
            repository.clone(),
            entitlements.clone(),
            Arc::new(StatusProvider(status)),
            nexus.clone(),
            clock.clone(),
            Arc::new(NoJitter),
            "mixed-worker".to_owned(),
            time::Duration::minutes(1),
        );

        assert_eq!(
            worker.run_once().await.unwrap(),
            MixedWorkerTick::RetryScheduled(task.task_id)
        );
        clock.set(NOW + time::Duration::seconds(6));
        assert_eq!(
            worker.run_once().await.unwrap(),
            MixedWorkerTick::Terminal(task.task_id)
        );
        assert_eq!(nexus.0.load(Ordering::SeqCst), 1);
        let entitlement = entitlements
            .get_verified_proof_bundle_v2(
                &task.snapshot.as_v1().creator,
                &task.snapshot.as_v1().bundle_id,
            )
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            entitlement.satisfied_by,
            vec![locks_core::verification::SatisfiedByV2::ZeroConfGraph]
        );
    }

    #[tokio::test]
    async fn both_successes_in_one_wave_publish_canonical_payment_then_graph_path() {
        let clock = Arc::new(FixedClock(Mutex::new(NOW)));
        let fence = Arc::new(InMemoryVerificationTaskDeletionFence::with_clock(
            clock.clone(),
        ));
        let repository = Arc::new(InMemoryMixedTaskRepository::with_deletion_fence(fence));
        let task = mixed_task();
        repository.insert_mixed_task(task.clone()).await.unwrap();
        let nexus = Arc::new(CountingNexus(AtomicUsize::new(0)));
        let mut status = provider_status(PaymentRequestStateV2::Accepted);
        let threshold = PaymentSampleV2 {
            observed_at: NOW + time::Duration::seconds(5),
            status: PaymentSampleStatusV2::Confirmed,
            outpoint: Some(BitcoinOutpointV2 {
                txid: "11".repeat(32),
                vout: 0,
            }),
            confirmations: 1,
            amount_matched: true,
        };
        status.current_sample = Some(threshold.clone());
        status.first_threshold_satisfied = Some(threshold);
        status.payment_state = PaymentStateV2::Confirmed;
        let worker = MixedVerificationWorker::new(
            repository.clone(),
            Arc::new(InMemoryEntitlementRepository::new()),
            Arc::new(StatusProvider(status)),
            nexus.clone(),
            clock.clone(),
            Arc::new(NoJitter),
            "mixed-worker".to_owned(),
            time::Duration::minutes(1),
        );

        assert_eq!(
            worker.run_once().await.unwrap(),
            MixedWorkerTick::RetryScheduled(task.task_id)
        );
        clock.set(NOW + time::Duration::seconds(6));
        assert_eq!(
            worker.run_once().await.unwrap(),
            MixedWorkerTick::Terminal(task.task_id)
        );
        assert_eq!(nexus.0.load(Ordering::SeqCst), 1);
        let completed = repository
            .get_mixed_task_by_handle(
                &task.snapshot.as_v1().creator,
                &task.snapshot.as_v1().bundle_id,
            )
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            completed.snapshot.as_v1().satisfied_by,
            vec![
                locks_core::verification::SatisfiedByV2::Payment,
                locks_core::verification::SatisfiedByV2::ZeroConfGraph,
            ]
        );
    }

    #[tokio::test]
    async fn exact_deadline_terminalizes_without_nexus_dispatch() {
        let clock = Arc::new(FixedClock(Mutex::new(NOW + time::Duration::minutes(5))));
        let fence = Arc::new(InMemoryVerificationTaskDeletionFence::with_clock(
            clock.clone(),
        ));
        let repository = Arc::new(InMemoryMixedTaskRepository::with_deletion_fence(fence));
        let task = mixed_task();
        repository.insert_mixed_task(task.clone()).await.unwrap();
        let nexus = Arc::new(CountingNexus(AtomicUsize::new(0)));
        let worker = MixedVerificationWorker::new(
            repository,
            Arc::new(InMemoryEntitlementRepository::new()),
            Arc::new(StatusProvider(provider_status(
                PaymentRequestStateV2::Accepted,
            ))),
            nexus.clone(),
            clock,
            Arc::new(NoJitter),
            "mixed-worker".to_owned(),
            time::Duration::minutes(1),
        );

        assert_eq!(
            worker.run_once().await.unwrap(),
            MixedWorkerTick::Terminal(task.task_id)
        );
        assert_eq!(nexus.0.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn ineligible_live_provider_state_never_dispatches_graph() {
        for request_state in [
            PaymentRequestStateV2::Proposed,
            PaymentRequestStateV2::RecoveryRequired,
        ] {
            let clock = Arc::new(FixedClock(Mutex::new(NOW)));
            let fence = Arc::new(InMemoryVerificationTaskDeletionFence::with_clock(
                clock.clone(),
            ));
            let repository = Arc::new(InMemoryMixedTaskRepository::with_deletion_fence(fence));
            let task = mixed_task();
            repository.insert_mixed_task(task.clone()).await.unwrap();
            let nexus = Arc::new(CountingNexus(AtomicUsize::new(0)));
            let provider = Arc::new(StatusProvider(provider_status(request_state)));
            let worker = MixedVerificationWorker::new(
                repository,
                Arc::new(InMemoryEntitlementRepository::new()),
                provider,
                nexus.clone(),
                clock.clone(),
                Arc::new(NoJitter),
                "mixed-worker".to_owned(),
                time::Duration::minutes(1),
            );

            assert_eq!(
                worker.run_once().await.unwrap(),
                MixedWorkerTick::RetryScheduled(task.task_id)
            );
            clock.set(NOW + time::Duration::seconds(6));
            assert_eq!(
                worker.run_once().await.unwrap(),
                MixedWorkerTick::RetryScheduled(task.task_id)
            );
            assert_eq!(nexus.0.load(Ordering::SeqCst), 0);
        }
    }

    fn mixed_task() -> MixedTaskRecord {
        let creator = CreatorPubky::from_str(CREATOR).unwrap();
        let reader = CreatorPubky::from_str(READER).unwrap();
        let lock_hash = [0; 32];
        let lock_id = LockId::from_hash(LockHash::from_bytes(lock_hash));
        let resource =
            PubkyLockResource::from_str(&format!("{creator}/pub/app.locks/{lock_id}.json"))
                .unwrap();
        let snapshot = MixedTaskSnapshotV1::new(
            SubmittedProofBundleV2 {
                version: SUBMITTED_PROOF_BUNDLE_V2_VERSION,
                bundle_id: BundleId::from_bytes([7; 16]),
                pubky_lock_resource: resource,
                reader_pubky: None,
            },
            lock_hash,
            reader,
            GraphPolicyV2 {
                operator: None,
                predicates: vec![GraphPredicateV2::Follows {
                    follower: GraphParticipantV2::Reader,
                    followed: GraphParticipantV2::Creator,
                }],
            },
            PaymentPolicyV2 {
                recipient_pubky: creator,
                amount: "50000".to_owned(),
                asset: "BTC".to_owned(),
                minimum_confirmations: 1,
            },
            NOW,
            300,
        )
        .unwrap();
        MixedTaskRecord {
            task_id: TaskId::from_str("123e4567-e89b-42d3-a456-426614174009").unwrap(),
            snapshot: MixedTaskSnapshot::V1(snapshot),
            evaluator_revision: 0,
            status: VerificationTaskStatus::Pending,
            terminal_reason: None,
            entitlement_to_publish: None,
            entitlement_publication_token: None,
            completed_at: None,
        }
    }

    fn provider_status(request_state: PaymentRequestStateV2) -> PaymentProviderStatus {
        PaymentProviderStatus {
            creator: CreatorPubky::from_str(CREATOR).unwrap(),
            bundle_id: BundleId::from_bytes([7; 16]),
            request_state,
            payment_state: PaymentStateV2::Undetected,
            invoice_created_at: NOW + time::Duration::seconds(1),
            proposal_expires_at: NOW + time::Duration::seconds(60),
            deadline: NOW + time::Duration::seconds(300),
            asset: PaymentAssetV2::Btc,
            minimum_confirmations: 1,
            current_sample: None,
            first_threshold_satisfied: None,
        }
    }
}
