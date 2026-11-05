use std::sync::Arc;
use std::time::Duration;

use locks_core::ids::TaskId;
use locks_service::application::errors::ApplicationError;
use locks_service::application::models::{
    PaymentOperationalErrorCategory, PaymentProviderCreateRequest, PaymentRootState,
    PaymentTaskRecord, PaymentTaskSnapshot, PaymentTaskSnapshotV1, VerificationTaskStatus,
    VerificationTerminalReason,
};
use locks_service::application::payment_v2_reducer::reduce_payment_snapshot;
use locks_service::application::ports::{
    Clock, EntitlementRepository, PaymentProviderClient, PaymentProviderError,
    PaymentProviderErrorKind, PaymentTaskClaimer,
};

const BACKOFF_SECONDS: [u64; 6] = [1, 2, 4, 8, 16, 30];
const STATUS_REPOLL_SECONDS: i64 = 5;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PaymentWorkerTick {
    Idle,
    RetryScheduled(TaskId),
    Terminal(TaskId),
    PublicationDeferred(TaskId),
    ClaimLost,
}

pub struct PaymentVerificationWorker {
    claimer: Arc<dyn PaymentTaskClaimer>,
    entitlements: Arc<dyn EntitlementRepository>,
    provider: Arc<dyn PaymentProviderClient>,
    clock: Arc<dyn Clock>,
    worker_id: String,
    claim_timeout: time::Duration,
}

impl PaymentVerificationWorker {
    pub fn new(
        claimer: Arc<dyn PaymentTaskClaimer>,
        entitlements: Arc<dyn EntitlementRepository>,
        provider: Arc<dyn PaymentProviderClient>,
        clock: Arc<dyn Clock>,
        worker_id: String,
        claim_timeout: time::Duration,
    ) -> Self {
        Self {
            claimer,
            entitlements,
            provider,
            clock,
            worker_id,
            claim_timeout,
        }
    }

    pub async fn run_once(&self) -> Result<PaymentWorkerTick, ApplicationError> {
        self.run_once_with_shutdown(None).await
    }

    async fn run_once_with_shutdown(
        &self,
        shutdown: Option<&tokio::sync::watch::Receiver<bool>>,
    ) -> Result<PaymentWorkerTick, ApplicationError> {
        if shutdown.is_some_and(|shutdown| *shutdown.borrow()) {
            return Ok(PaymentWorkerTick::Idle);
        }
        let claim_now = self.clock.now();
        let Some(claim) = self
            .claimer
            .claim_next_payment_task(&self.worker_id, claim_now, claim_now + self.claim_timeout)
            .await?
        else {
            return Ok(PaymentWorkerTick::Idle);
        };
        let task_id = claim.task.task_id;
        let claim_token = claim.claim_token;
        let expected_revision = claim.task.evaluator_revision;
        if shutdown.is_some_and(|shutdown| *shutdown.borrow()) {
            self.claimer
                .release_claimed_payment_task(&task_id, &self.worker_id, &claim_token)
                .await?;
            return Ok(PaymentWorkerTick::ClaimLost);
        }
        if claim.task.status == VerificationTaskStatus::PublishingEntitlement {
            return self
                .publish(claim.task, claim_token, expected_revision, shutdown)
                .await;
        }
        if claim.task.snapshot.as_v1().root != PaymentRootState::Pending {
            return self
                .persist(
                    task_id,
                    claim_token,
                    expected_revision,
                    claim.task.snapshot,
                    self.clock.now(),
                    shutdown,
                )
                .await;
        }
        let mut snapshot = claim.task.snapshot;
        let dispatch_now = self.clock.now();
        if dispatch_now >= snapshot.as_v1().deadline_at {
            reduce_payment_snapshot(snapshot_v1_mut(&mut snapshot), dispatch_now)
                .map_err(invalid_snapshot)?;
            return self
                .persist(
                    task_id,
                    claim_token,
                    expected_revision,
                    snapshot,
                    dispatch_now,
                    shutdown,
                )
                .await;
        }

        if matches!(
            snapshot.as_v1().invoice,
            locks_service::application::models::PaymentInvoiceState::Pending
        ) {
            let current = snapshot.as_v1();
            let request = PaymentProviderCreateRequest {
                bundle_id: current.bundle_id.clone(),
                lock_resource: current.pubky_lock_resource.clone(),
                reader: current.reader.clone(),
                payment: current.payment.clone(),
                deadline: current.deadline_at,
            };
            match self.provider.create_invoice(&request).await {
                Ok(created) => {
                    let now = self.clock.now();
                    if snapshot_v1_mut(&mut snapshot)
                        .apply_create_response(
                            created.invoice_created_at,
                            created.proposal_expires_at,
                            created.deadline,
                        )
                        .is_err()
                    {
                        snapshot_v1_mut(&mut snapshot).root = PaymentRootState::Failed(
                            VerificationTerminalReason::InvalidVerifierResponse,
                        );
                        return self
                            .persist(
                                task_id,
                                claim_token,
                                expected_revision,
                                snapshot,
                                now,
                                shutdown,
                            )
                            .await;
                    }
                    if now >= request.deadline {
                        reduce_payment_snapshot(snapshot_v1_mut(&mut snapshot), now)
                            .map_err(invalid_snapshot)?;
                        return self
                            .persist(
                                task_id,
                                claim_token,
                                expected_revision,
                                snapshot,
                                now,
                                shutdown,
                            )
                            .await;
                    }
                    return self
                        .schedule(
                            task_id,
                            claim_token,
                            expected_revision,
                            snapshot,
                            now,
                            (now + time::Duration::milliseconds(1)).min(request.deadline),
                        )
                        .await;
                }
                Err(error) => {
                    return self
                        .handle_provider_error(
                            task_id,
                            claim_token,
                            expected_revision,
                            snapshot,
                            error,
                            self.clock.now(),
                            shutdown,
                        )
                        .await;
                }
            }
        }

        let identity = snapshot.as_v1();
        match self
            .provider
            .invoice_status(&identity.creator, &identity.bundle_id)
            .await
        {
            Ok(status) => {
                let now = self.clock.now();
                if snapshot_v1_mut(&mut snapshot).apply_status(status).is_err() {
                    snapshot_v1_mut(&mut snapshot).root = PaymentRootState::Failed(
                        VerificationTerminalReason::InvalidVerifierResponse,
                    );
                    return self
                        .persist(
                            task_id,
                            claim_token,
                            expected_revision,
                            snapshot,
                            now,
                            shutdown,
                        )
                        .await;
                }
                reduce_payment_snapshot(snapshot_v1_mut(&mut snapshot), now)
                    .map_err(invalid_snapshot)?;
                if snapshot.as_v1().root == PaymentRootState::Pending {
                    let deadline = snapshot.as_v1().deadline_at;
                    let due = (now + time::Duration::seconds(STATUS_REPOLL_SECONDS)).min(deadline);
                    self.schedule(task_id, claim_token, expected_revision, snapshot, now, due)
                        .await
                } else {
                    self.persist(
                        task_id,
                        claim_token,
                        expected_revision,
                        snapshot,
                        now,
                        shutdown,
                    )
                    .await
                }
            }
            Err(error) => {
                self.handle_provider_error(
                    task_id,
                    claim_token,
                    expected_revision,
                    snapshot,
                    error,
                    self.clock.now(),
                    shutdown,
                )
                .await
            }
        }
    }

    #[allow(clippy::too_many_arguments)]
    async fn handle_provider_error(
        &self,
        task_id: TaskId,
        claim_token: uuid::Uuid,
        expected_revision: u64,
        mut snapshot: PaymentTaskSnapshot,
        error: PaymentProviderError,
        observed_at: time::OffsetDateTime,
        shutdown: Option<&tokio::sync::watch::Receiver<bool>>,
    ) -> Result<PaymentWorkerTick, ApplicationError> {
        match error.kind {
            PaymentProviderErrorKind::InvalidResponse => {
                snapshot_v1_mut(&mut snapshot).root =
                    PaymentRootState::Failed(VerificationTerminalReason::InvalidVerifierResponse);
                self.persist(
                    task_id,
                    claim_token,
                    expected_revision,
                    snapshot,
                    observed_at,
                    shutdown,
                )
                .await
            }
            PaymentProviderErrorKind::Conflict => {
                snapshot_v1_mut(&mut snapshot).root =
                    PaymentRootState::Failed(VerificationTerminalReason::VerificationConflict);
                self.persist(
                    task_id,
                    claim_token,
                    expected_revision,
                    snapshot,
                    observed_at,
                    shutdown,
                )
                .await
            }
            PaymentProviderErrorKind::Retryable => {
                let state = snapshot_v1_mut(&mut snapshot);
                state.operational_failure_count = state.operational_failure_count.saturating_add(1);
                state.operational_failure_total = state.operational_failure_total.saturating_add(1);
                state
                    .operational_first_failure_at
                    .get_or_insert(observed_at);
                state.operational_last_failure_at = Some(observed_at);
                state.operational_last_category = Some(PaymentOperationalErrorCategory::Transport);
                if observed_at >= state.deadline_at {
                    reduce_payment_snapshot(state, observed_at).map_err(invalid_snapshot)?;
                    return self
                        .persist(
                            task_id,
                            claim_token,
                            expected_revision,
                            snapshot,
                            observed_at,
                            shutdown,
                        )
                        .await;
                }
                let index = usize::try_from(state.operational_failure_count.saturating_sub(1))
                    .unwrap_or(usize::MAX)
                    .min(BACKOFF_SECONDS.len() - 1);
                let seconds = error
                    .retry_after_seconds
                    .unwrap_or(BACKOFF_SECONDS[index])
                    .clamp(1, 60);
                let due = (observed_at
                    + time::Duration::seconds(i64::try_from(seconds).unwrap_or(60)))
                .min(state.deadline_at);
                state.next_due_at = Some(due);
                self.schedule(
                    task_id,
                    claim_token,
                    expected_revision,
                    snapshot,
                    observed_at,
                    due,
                )
                .await
            }
        }
    }

    async fn persist(
        &self,
        task_id: TaskId,
        claim_token: uuid::Uuid,
        expected_revision: u64,
        snapshot: PaymentTaskSnapshot,
        now: time::OffsetDateTime,
        shutdown: Option<&tokio::sync::watch::Receiver<bool>>,
    ) -> Result<PaymentWorkerTick, ApplicationError> {
        let persisted = self
            .claimer
            .persist_claimed_payment_snapshot(
                &task_id,
                &self.worker_id,
                &claim_token,
                now,
                expected_revision,
                snapshot,
            )
            .await?;
        let Some(persisted) = persisted else {
            return Ok(PaymentWorkerTick::ClaimLost);
        };
        if persisted.status == VerificationTaskStatus::PublishingEntitlement {
            self.publish(persisted, claim_token, expected_revision + 1, shutdown)
                .await
        } else {
            Ok(PaymentWorkerTick::Terminal(task_id))
        }
    }

    async fn schedule(
        &self,
        task_id: TaskId,
        claim_token: uuid::Uuid,
        expected_revision: u64,
        snapshot: PaymentTaskSnapshot,
        now: time::OffsetDateTime,
        due: time::OffsetDateTime,
    ) -> Result<PaymentWorkerTick, ApplicationError> {
        let persisted = self
            .claimer
            .schedule_payment_task_retry(
                &task_id,
                &self.worker_id,
                &claim_token,
                now,
                expected_revision,
                snapshot,
                due,
            )
            .await?;
        Ok(if persisted.is_some() {
            PaymentWorkerTick::RetryScheduled(task_id)
        } else {
            PaymentWorkerTick::ClaimLost
        })
    }

    async fn publish(
        &self,
        task: PaymentTaskRecord,
        claim_token: uuid::Uuid,
        expected_revision: u64,
        shutdown: Option<&tokio::sync::watch::Receiver<bool>>,
    ) -> Result<PaymentWorkerTick, ApplicationError> {
        let task_id = task.task_id;
        let payload = task.entitlement_to_publish.ok_or_else(|| {
            storage("publishing payment task is missing durable entitlement payload")
        })?;
        let publication_token = task
            .entitlement_publication_token
            .ok_or_else(|| storage("publishing payment task is missing external-effect fence"))?;
        let Some(mut action) = self
            .claimer
            .acquire_payment_publication_action(
                &task_id,
                &self.worker_id,
                &claim_token,
                expected_revision,
                publication_token,
            )
            .await?
        else {
            return Ok(PaymentWorkerTick::ClaimLost);
        };
        if shutdown.is_some_and(|shutdown| *shutdown.borrow()) {
            action.release().await?;
            self.claimer
                .release_claimed_payment_task(&task_id, &self.worker_id, &claim_token)
                .await?;
            return Ok(PaymentWorkerTick::ClaimLost);
        }
        let outcome = async {
            let existing = match self
                .entitlements
                .get_verified_proof_bundle_v2(&payload.creator, &payload.bundle_id)
                .await
            {
                Ok(existing) => existing,
                Err(_) => return Ok(PaymentWorkerTick::PublicationDeferred(task_id)),
            };
            match existing {
                Some(existing) if existing == payload => {}
                Some(_) => {
                    return Err(storage(
                        "published v2 entitlement conflicts with durable payment payload",
                    ));
                }
                None => {
                    if self
                        .entitlements
                        .insert_verified_proof_bundle_v2(payload.clone())
                        .await
                        .is_err()
                    {
                        return Ok(PaymentWorkerTick::PublicationDeferred(task_id));
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
                        return Ok(PaymentWorkerTick::PublicationDeferred(task_id));
                    }
                }
            }
            Ok(if action.complete().await?.is_some() {
                PaymentWorkerTick::Terminal(task_id)
            } else {
                PaymentWorkerTick::ClaimLost
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
                PaymentWorkerTick::Idle | PaymentWorkerTick::PublicationDeferred(_)
            ) {
                tokio::select! {
                    changed = shutdown.changed() => if changed.is_err() || *shutdown.borrow() { return Ok(()); },
                    _ = tokio::time::sleep(poll_interval) => {}
                }
            }
        }
    }
}

fn snapshot_v1_mut(snapshot: &mut PaymentTaskSnapshot) -> &mut PaymentTaskSnapshotV1 {
    match snapshot {
        PaymentTaskSnapshot::V1(snapshot) => snapshot,
    }
}
fn invalid_snapshot(message: String) -> ApplicationError {
    storage(format!("invalid payment worker snapshot: {message}"))
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
    use locks_core::lock_policy::PaymentPolicyV2;
    use locks_core::verification::{SUBMITTED_PROOF_BUNDLE_V2_VERSION, SubmittedProofBundleV2};
    use locks_service::application::models::{PaymentInvoiceState, VerificationTaskStatus};
    use locks_service::application::ports::{
        AdmitPaymentTask, PaymentProviderCreateResponse, PaymentTaskRepository,
    };
    use locks_service::infrastructure::memory::{
        entitlements::InMemoryEntitlementRepository, payment_tasks::InMemoryPaymentTaskRepository,
        verification_task_deletion_fence::InMemoryVerificationTaskDeletionFence,
    };
    use time::macros::datetime;

    use super::*;

    const CREATOR: &str = "pubkytkrq8zmwb8a3m9k15csu3q17qmfgqnp9dskbrg9uq1rydpyxp7qy";
    const READER: &str = "pubkyorhzqdiexwmi6iidktucgud63ufa5nwtsuzdxe176a8izd6jsqky";
    const BUNDLE: &str = "000G40R40M30E209185GR38E1W";
    const ACCEPTED: time::OffsetDateTime = datetime!(2026-11-05 12:00:00 UTC);
    const DEADLINE: time::OffsetDateTime = datetime!(2026-11-05 12:05:00 UTC);

    #[derive(Debug)]
    struct MutableClock(Mutex<time::OffsetDateTime>);

    impl MutableClock {
        fn set(&self, now: time::OffsetDateTime) {
            *self.0.lock().unwrap() = now;
        }
    }

    impl Clock for MutableClock {
        fn now(&self) -> time::OffsetDateTime {
            *self.0.lock().unwrap()
        }
    }

    struct CreateProvider {
        response: Result<PaymentProviderCreateResponse, PaymentProviderError>,
        clock: Arc<MutableClock>,
        complete_at: Option<time::OffsetDateTime>,
        create_calls: AtomicUsize,
    }

    #[async_trait]
    impl PaymentProviderClient for CreateProvider {
        async fn create_invoice(
            &self,
            request: &PaymentProviderCreateRequest,
        ) -> Result<PaymentProviderCreateResponse, PaymentProviderError> {
            assert!(self.clock.now() < request.deadline);
            self.create_calls.fetch_add(1, Ordering::SeqCst);
            if let Some(complete_at) = self.complete_at {
                self.clock.set(complete_at);
            }
            self.response
        }

        async fn invoice_status(
            &self,
            _creator: &CreatorPubky,
            _bundle_id: &BundleId,
        ) -> Result<locks_service::application::models::PaymentProviderStatus, PaymentProviderError>
        {
            panic!("invoice status is not expected in create-response tests")
        }
    }

    fn valid_create_response() -> PaymentProviderCreateResponse {
        PaymentProviderCreateResponse {
            invoice_created_at: ACCEPTED + time::Duration::seconds(1),
            proposal_expires_at: ACCEPTED + time::Duration::seconds(60),
            deadline: DEADLINE,
        }
    }

    fn repository_with_clock(clock: Arc<MutableClock>) -> Arc<InMemoryPaymentTaskRepository> {
        let fence = Arc::new(InMemoryVerificationTaskDeletionFence::with_clock(clock));
        Arc::new(InMemoryPaymentTaskRepository::with_deletion_fence(fence))
    }

    async fn admit(repository: &InMemoryPaymentTaskRepository) -> PaymentTaskRecord {
        let creator = CreatorPubky::from_str(CREATOR).unwrap();
        let lock_hash = [7; 32];
        let resource = PubkyLockResource::from_str(&format!(
            "{CREATOR}/pub/app.locks/{}.json",
            LockId::from_hash(LockHash::from_bytes(lock_hash))
        ))
        .unwrap();
        repository
            .admit_payment_task(AdmitPaymentTask {
                task_id: TaskId::from_str("123e4567-e89b-42d3-a456-426614174001").unwrap(),
                submitted_proof_bundle: SubmittedProofBundleV2 {
                    version: SUBMITTED_PROOF_BUNDLE_V2_VERSION,
                    bundle_id: BundleId::from_str(BUNDLE).unwrap(),
                    pubky_lock_resource: resource,
                    reader_pubky: Some(CreatorPubky::from_str(READER).unwrap()),
                },
                lock_hash,
                payment: PaymentPolicyV2 {
                    recipient_pubky: creator,
                    amount: "50000".to_owned(),
                    asset: "BTC".to_owned(),
                    minimum_confirmations: 1,
                },
                verification_window_seconds: 300,
            })
            .await
            .unwrap()
    }

    fn worker(
        repository: Arc<InMemoryPaymentTaskRepository>,
        provider: Arc<CreateProvider>,
        clock: Arc<MutableClock>,
    ) -> PaymentVerificationWorker {
        PaymentVerificationWorker::new(
            repository,
            Arc::new(InMemoryEntitlementRepository::new()),
            provider,
            clock,
            "payment-worker".to_owned(),
            time::Duration::minutes(10),
        )
    }

    #[tokio::test]
    async fn create_response_at_or_after_deadline_is_persisted_before_terminalization() {
        for complete_at in [DEADLINE, DEADLINE + time::Duration::milliseconds(1)] {
            let clock = Arc::new(MutableClock(Mutex::new(ACCEPTED)));
            let repository = repository_with_clock(clock.clone());
            let admitted = admit(&repository).await;
            let provider = Arc::new(CreateProvider {
                response: Ok(valid_create_response()),
                clock: clock.clone(),
                complete_at: Some(complete_at),
                create_calls: AtomicUsize::new(0),
            });

            assert_eq!(
                worker(repository.clone(), provider.clone(), clock)
                    .run_once()
                    .await
                    .unwrap(),
                PaymentWorkerTick::Terminal(admitted.task_id)
            );
            assert_eq!(provider.create_calls.load(Ordering::SeqCst), 1);

            let persisted = repository
                .get_payment_task_by_handle(
                    &CreatorPubky::from_str(CREATOR).unwrap(),
                    &BundleId::from_str(BUNDLE).unwrap(),
                )
                .await
                .unwrap()
                .unwrap();
            assert_eq!(persisted.status, VerificationTaskStatus::Failed);
            assert!(matches!(
                persisted.snapshot.as_v1().invoice,
                PaymentInvoiceState::Created { .. }
            ));
            assert_eq!(
                persisted.snapshot.as_v1().root,
                PaymentRootState::Failed(
                    VerificationTerminalReason::VerificationUnavailableByDeadline
                )
            );
        }
    }

    #[tokio::test]
    async fn create_response_binding_mismatch_terminalizes_without_application_error() {
        let clock = Arc::new(MutableClock(Mutex::new(ACCEPTED)));
        let repository = repository_with_clock(clock.clone());
        let admitted = admit(&repository).await;
        let mut response = valid_create_response();
        response.deadline += time::Duration::seconds(1);
        let provider = Arc::new(CreateProvider {
            response: Ok(response),
            clock: clock.clone(),
            complete_at: None,
            create_calls: AtomicUsize::new(0),
        });

        assert_eq!(
            worker(repository.clone(), provider, clock)
                .run_once()
                .await
                .unwrap(),
            PaymentWorkerTick::Terminal(admitted.task_id)
        );
        let persisted = repository
            .get_payment_task_by_handle(
                &CreatorPubky::from_str(CREATOR).unwrap(),
                &BundleId::from_str(BUNDLE).unwrap(),
            )
            .await
            .unwrap()
            .unwrap();
        assert_eq!(persisted.status, VerificationTaskStatus::Failed);
        assert_eq!(
            persisted.snapshot.as_v1().root,
            PaymentRootState::Failed(VerificationTerminalReason::InvalidVerifierResponse)
        );
    }

    #[tokio::test]
    async fn retry_after_zero_schedules_a_positive_retry_delay() {
        let clock = Arc::new(MutableClock(Mutex::new(ACCEPTED)));
        let repository = repository_with_clock(clock.clone());
        let admitted = admit(&repository).await;
        let provider = Arc::new(CreateProvider {
            response: Err(PaymentProviderError {
                kind: PaymentProviderErrorKind::Retryable,
                retry_after_seconds: Some(0),
            }),
            clock: clock.clone(),
            complete_at: None,
            create_calls: AtomicUsize::new(0),
        });

        assert_eq!(
            worker(repository.clone(), provider, clock)
                .run_once()
                .await
                .unwrap(),
            PaymentWorkerTick::RetryScheduled(admitted.task_id)
        );
        let persisted = repository
            .get_payment_task_by_handle(
                &CreatorPubky::from_str(CREATOR).unwrap(),
                &BundleId::from_str(BUNDLE).unwrap(),
            )
            .await
            .unwrap()
            .unwrap();
        assert_eq!(persisted.status, VerificationTaskStatus::Pending);
        assert_eq!(
            persisted.snapshot.as_v1().next_due_at,
            Some(ACCEPTED + time::Duration::seconds(1))
        );
    }
}
