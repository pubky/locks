use locks_core::ids::{LockServerPubky, TaskId};
use locks_service::application::errors::ApplicationError;
use locks_service::application::ports::{
    Clock, ContentLockRepository, CriterionVerifier, EntitlementRepository,
    VerificationTaskClaimer, VerificationTaskRepository,
};
use locks_service::application::use_cases::complete_verification_task::{
    CompleteVerificationTaskRequest, CompleteVerificationTaskUseCase,
};
use locks_service::infrastructure::verifiers::registry::StaticCriterionVerifierRegistry;
use tokio::sync::watch;
use tracing::{debug, error, info};

use crate::app_state::AppState;

const PENDING_VERIFICATION_RETRY_DELAY_SECONDS: i64 = 30;

/// Result of one worker polling attempt.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WorkerTick {
    Idle,
    Completed(TaskId),
    RetryScheduled(TaskId),
    Failed(TaskId),
}

/// In-process verification worker.
pub struct VerificationWorker<'a> {
    tasks: &'a dyn VerificationTaskRepository,
    claimer: &'a dyn VerificationTaskClaimer,
    content_locks: &'a dyn ContentLockRepository,
    entitlements: &'a dyn EntitlementRepository,
    dev_static_verifier: &'a dyn CriterionVerifier,
    paykit_payment_verifier: Option<&'a dyn CriterionVerifier>,
    allow_dev_static_verifier: bool,
    clock: &'a dyn Clock,
    verified_by: LockServerPubky,
    worker_id: String,
    poll_interval: std::time::Duration,
    claim_timeout_seconds: u64,
}

impl<'a> VerificationWorker<'a> {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        tasks: &'a dyn VerificationTaskRepository,
        claimer: &'a dyn VerificationTaskClaimer,
        content_locks: &'a dyn ContentLockRepository,
        entitlements: &'a dyn EntitlementRepository,
        dev_static_verifier: &'a dyn CriterionVerifier,
        paykit_payment_verifier: Option<&'a dyn CriterionVerifier>,
        allow_dev_static_verifier: bool,
        clock: &'a dyn Clock,
        verified_by: LockServerPubky,
        worker_id: String,
        poll_interval: std::time::Duration,
        claim_timeout_seconds: u64,
    ) -> Self {
        Self {
            tasks,
            claimer,
            content_locks,
            entitlements,
            dev_static_verifier,
            paykit_payment_verifier,
            allow_dev_static_verifier,
            clock,
            verified_by,
            worker_id,
            poll_interval,
            claim_timeout_seconds,
        }
    }

    pub fn from_state(state: &'a AppState) -> Self {
        // Dev-static verification is a local/dev verifier only. Production-mode
        // workers intentionally build the registry without it; production
        // completion should happen through this worker path, not through the
        // dev-only HTTP completion route.
        Self::new(
            state.verification_tasks().as_ref(),
            state.verification_task_claimer().as_ref(),
            state.content_locks().as_ref(),
            state.entitlements().as_ref(),
            state.dev_static_verifier().as_ref(),
            state
                .paykit_payment_verifier()
                .map(|verifier| verifier.as_ref() as &dyn CriterionVerifier),
            state.config().runtime.environment.is_development(),
            state.clock().as_ref(),
            state.config().credentials.lock_server_public_key.clone(),
            state.config().worker.worker_id.clone(),
            std::time::Duration::from_millis(state.config().worker.poll_interval_ms),
            state.config().worker.claim_timeout_seconds,
        )
    }

    pub async fn run_once(&self) -> Result<WorkerTick, ApplicationError> {
        let now = self.clock.now();
        let claim_expires_at = now + claim_timeout(self.claim_timeout_seconds);
        let Some(claim) = self
            .claimer
            .claim_next_verification_task(&self.worker_id, now, claim_expires_at)
            .await?
        else {
            return Ok(WorkerTick::Idle);
        };

        let task_id = claim.task.task_id;
        let claim_token = claim.claim_token;
        debug!(%task_id, worker_id = %self.worker_id, "claimed verification task");
        let mut verifiers = StaticCriterionVerifierRegistry::new();
        if self.allow_dev_static_verifier {
            verifiers = verifiers.with_dev_static(self.dev_static_verifier);
        }
        if let Some(paykit_payment_verifier) = self.paykit_payment_verifier {
            verifiers = verifiers.with_paykit_payment(paykit_payment_verifier);
        }
        let use_case = CompleteVerificationTaskUseCase::new(
            self.tasks,
            self.content_locks,
            self.entitlements,
            &verifiers,
            self.clock,
            self.verified_by.clone(),
        );

        match use_case
            .execute_claimed(
                CompleteVerificationTaskRequest { task_id },
                claim,
                &self.worker_id,
                self.claimer,
            )
            .await
        {
            Ok(completed) => {
                info!(%task_id, status = ?completed.status, "completed verification task");
                Ok(WorkerTick::Completed(task_id))
            }
            Err(ApplicationError::VerificationPending) => {
                let retry_scheduled_at = self.clock.now();
                let next_attempt_at = retry_scheduled_at + retry_delay();
                let Some(_) = self
                    .claimer
                    .schedule_verification_task_retry(
                        &task_id,
                        &self.worker_id,
                        &claim_token,
                        retry_scheduled_at,
                        next_attempt_at,
                    )
                    .await?
                else {
                    info!(
                        %task_id,
                        worker_id = %self.worker_id,
                        "verification task claim no longer owned; retry not scheduled"
                    );
                    return Ok(WorkerTick::Idle);
                };
                debug!(
                    %task_id,
                    worker_id = %self.worker_id,
                    %next_attempt_at,
                    "scheduled verification task retry"
                );
                Ok(WorkerTick::RetryScheduled(task_id))
            }
            Err(ApplicationError::VerificationTaskClaimLost) => {
                info!(
                    %task_id,
                    worker_id = %self.worker_id,
                    "verification task claim no longer owned; terminal state not persisted"
                );
                Ok(WorkerTick::Idle)
            }
            Err(error) => {
                error!(%task_id, error = %error, "verification task failed");
                Ok(WorkerTick::Failed(task_id))
            }
        }
    }

    pub async fn run_until_shutdown(
        &self,
        mut shutdown: watch::Receiver<bool>,
    ) -> Result<(), ApplicationError> {
        loop {
            if *shutdown.borrow() {
                return Ok(());
            }

            match self.run_once().await? {
                WorkerTick::Idle => {
                    tokio::select! {
                        _ = shutdown.changed() => {
                            if *shutdown.borrow() {
                                return Ok(());
                            }
                        }
                        _ = tokio::time::sleep(self.poll_interval) => {}
                    }
                }
                WorkerTick::Completed(_)
                | WorkerTick::RetryScheduled(_)
                | WorkerTick::Failed(_) => {}
            }
        }
    }
}

fn claim_timeout(seconds: u64) -> time::Duration {
    time::Duration::seconds(i64::try_from(seconds).unwrap_or(i64::MAX))
}

fn retry_delay() -> time::Duration {
    time::Duration::seconds(PENDING_VERIFICATION_RETRY_DELAY_SECONDS)
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};

    use async_trait::async_trait;
    use locks_core::ids::{
        BundleId, CreatorPubky, GuardedResourceHash, LockServerPubky, PubkyLockResource, TaskId,
    };
    use locks_core::lock_policy::{
        AccessPolicy, CONTENT_LOCK_VERSION, ContentLock, Criterion, GuardedResource, LockLogic,
        LockServerConfig, VerifierType,
    };
    use locks_core::verification::{
        CriterionVerificationResult, Proof, SUBMITTED_PROOF_BUNDLE_VERSION, SubmittedProofBundle,
    };
    use locks_service::application::errors::ApplicationError;
    use locks_service::application::models::{
        CriterionVerificationRequest, VerificationTaskRecord, VerificationTaskStatus,
    };
    use locks_service::application::ports::{
        ContentLockRepository, CriterionVerifier, EntitlementRepository, VerificationTaskRepository,
    };
    use locks_service::infrastructure::memory::{
        content_locks::InMemoryContentLockRepository, entitlements::InMemoryEntitlementRepository,
        verification_task_claims::InMemoryVerificationTaskClaimer,
        verification_tasks::InMemoryVerificationTaskRepository,
    };
    use locks_service::infrastructure::verifiers::dev_static::DevStaticVerifier;
    use time::macros::datetime;
    use tokio::sync::watch;

    use crate::app_state::{AppState, SystemClock};
    use crate::config::{
        ContentLocksConfig, DatabaseConfig, LockServerCredentialsConfig, LockServerRuntimeConfig,
        LoggingConfig, PubkyConfig, RateLimitsConfig, RuntimeConfig, RuntimeEnvironment,
        SecretsConfig, WorkerConfig,
    };
    use crate::worker::{VerificationWorker, WorkerTick, retry_delay};

    const TASK_ID: &str = "018fc6ec-2f3d-4f7e-8b7d-6f5c4b3a2d10";
    const BUNDLE_ID: &str = "000G40R40M30E209185GR38E1W";

    #[tokio::test]
    async fn worker_completes_pending_task() {
        let fixture = WorkerFixture::new(content_lock(true)).await;
        fixture.seed_task().await;
        let worker = fixture.worker();

        assert_eq!(
            worker.run_once().await.unwrap(),
            WorkerTick::Completed(task_id())
        );

        let stored = fixture
            .tasks
            .get_verification_task(&task_id())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(stored.status, VerificationTaskStatus::Completed);
        assert!(
            fixture
                .entitlements
                .get_verified_proof_bundle(&creator(), &bundle_id())
                .await
                .unwrap()
                .is_some()
        );
    }

    #[tokio::test]
    async fn worker_marks_failed_verification_as_failed() {
        let fixture = WorkerFixture::new(content_lock(false)).await;
        fixture.seed_task().await;
        let worker = fixture.worker();

        let tick = worker.run_once().await.unwrap();

        assert_eq!(tick, WorkerTick::Failed(task_id()));
        let stored = fixture
            .tasks
            .get_verification_task(&task_id())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(stored.status, VerificationTaskStatus::Failed);
        assert_eq!(
            stored.failure_message,
            Some("entitlement not satisfied".to_owned())
        );
    }

    #[tokio::test]
    async fn worker_schedules_pending_verification_without_hot_looping_then_completes() {
        let fixture = WorkerFixture::new(content_lock(true)).await;
        fixture.seed_task().await;
        let verifier = RetryOnceVerifier::default();
        let worker = fixture.worker_with_verifier(&verifier);

        assert_eq!(
            worker.run_once().await.unwrap(),
            WorkerTick::RetryScheduled(task_id())
        );
        let stored = fixture
            .tasks
            .get_verification_task(&task_id())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(stored.status, VerificationTaskStatus::Pending);
        assert_eq!(stored.failure_message, None);
        assert_eq!(worker.run_once().await.unwrap(), WorkerTick::Idle);
    }

    #[tokio::test]
    async fn worker_without_dev_static_registration_fails_dev_static_tasks() {
        let fixture = WorkerFixture::new(content_lock(true)).await;
        fixture.seed_task().await;
        let worker = fixture.worker_without_dev_static_registration();

        let tick = worker.run_once().await.unwrap();

        assert_eq!(tick, WorkerTick::Failed(task_id()));
        let stored = fixture
            .tasks
            .get_verification_task(&task_id())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(stored.status, VerificationTaskStatus::Failed);
        assert_eq!(
            stored.failure_message,
            Some("verification failed".to_owned())
        );
        assert!(
            fixture
                .entitlements
                .get_verified_proof_bundle(&creator(), &bundle_id())
                .await
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn worker_from_state_registers_dev_static_only_in_dev_mode() {
        let dev_state =
            AppState::new_empty_in_memory(runtime_config(RuntimeEnvironment::Development));
        let production_state =
            AppState::new_empty_in_memory(runtime_config(RuntimeEnvironment::Production));

        assert!(VerificationWorker::from_state(&dev_state).allow_dev_static_verifier);
        assert!(!VerificationWorker::from_state(&production_state).allow_dev_static_verifier);
    }

    #[tokio::test]
    async fn worker_stops_on_shutdown() {
        let fixture = WorkerFixture::empty();
        let worker = fixture.worker();
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        shutdown_tx.send(true).unwrap();

        tokio::time::timeout(
            std::time::Duration::from_millis(100),
            worker.run_until_shutdown(shutdown_rx),
        )
        .await
        .expect("worker should stop promptly on shutdown")
        .unwrap();
    }

    #[test]
    fn pending_verification_retry_is_independent_of_queue_polling() {
        assert_eq!(retry_delay(), time::Duration::seconds(30));
    }

    #[tokio::test]
    async fn worker_tick_debug_does_not_expose_submitted_proof_payload() {
        let fixture = WorkerFixture::new(content_lock_with_payload(json_secret_payload())).await;
        fixture.seed_task_with_payload(json_secret_payload()).await;
        let worker = fixture.worker();

        let tick = worker.run_once().await.unwrap();
        let debug = format!("{tick:?}");

        assert!(!debug.contains("super-secret-proof-token"));
    }

    struct WorkerFixture {
        task: VerificationTaskRecord,
        tasks: Arc<InMemoryVerificationTaskRepository>,
        claimer: InMemoryVerificationTaskClaimer,
        content_locks: InMemoryContentLockRepository,
        entitlements: InMemoryEntitlementRepository,
        verifier: DevStaticVerifier,
        clock: SystemClock,
    }

    impl WorkerFixture {
        fn empty() -> Self {
            let tasks = Arc::new(InMemoryVerificationTaskRepository::new());
            Self {
                task: task_for(&content_lock(true), json_secret_payload()),
                claimer: InMemoryVerificationTaskClaimer::with_task_repository(
                    vec![],
                    tasks.clone(),
                ),
                tasks,
                content_locks: InMemoryContentLockRepository::new(),
                entitlements: InMemoryEntitlementRepository::new(),
                verifier: DevStaticVerifier,
                clock: SystemClock,
            }
        }

        async fn new(content_lock: ContentLock) -> Self {
            let task = task_for(&content_lock, json_secret_payload());
            let tasks = Arc::new(InMemoryVerificationTaskRepository::new());
            let fixture = Self {
                task: task.clone(),
                claimer: InMemoryVerificationTaskClaimer::with_task_repository(
                    vec![task],
                    tasks.clone(),
                ),
                tasks,
                content_locks: InMemoryContentLockRepository::new(),
                entitlements: InMemoryEntitlementRepository::new(),
                verifier: DevStaticVerifier,
                clock: SystemClock,
            };
            fixture.seed_content_lock(content_lock).await;
            fixture
        }

        fn worker(&self) -> VerificationWorker<'_> {
            self.worker_with_verifier(&self.verifier)
        }

        fn worker_with_verifier<'a>(
            &'a self,
            verifier: &'a dyn CriterionVerifier,
        ) -> VerificationWorker<'a> {
            VerificationWorker::new(
                self.tasks.as_ref(),
                &self.claimer,
                &self.content_locks,
                &self.entitlements,
                verifier,
                None,
                true,
                &self.clock,
                LockServerPubky::from_str(
                    "pubky7ir1ttte48bcp4zjychjyscicrwi1j34mtt91ptsafdbjmr8g9eo",
                )
                .unwrap(),
                "test-worker".to_owned(),
                std::time::Duration::from_millis(10),
                60,
            )
        }

        fn worker_without_dev_static_registration(&self) -> VerificationWorker<'_> {
            VerificationWorker::new(
                self.tasks.as_ref(),
                &self.claimer,
                &self.content_locks,
                &self.entitlements,
                &self.verifier,
                None,
                false,
                &self.clock,
                LockServerPubky::from_str(
                    "pubky7ir1ttte48bcp4zjychjyscicrwi1j34mtt91ptsafdbjmr8g9eo",
                )
                .unwrap(),
                "test-worker".to_owned(),
                std::time::Duration::from_millis(10),
                60,
            )
        }

        async fn seed_task(&self) {
            self.tasks
                .insert_verification_task(self.task.clone())
                .await
                .unwrap();
        }

        async fn seed_task_with_payload(&self, payload: serde_json::Value) {
            let mut task = self.task.clone();
            task.submitted_proof_bundle.proofs[0].payload = payload;
            self.tasks.insert_verification_task(task).await.unwrap();
        }

        async fn seed_content_lock(&self, content_lock: ContentLock) {
            let path = content_lock.content_lock_path().unwrap();
            let creator = creator();
            self.content_locks
                .upsert_content_lock(creator, path, content_lock)
                .await
                .unwrap();
        }
    }

    #[derive(Default)]
    struct RetryOnceVerifier {
        returned_pending: AtomicBool,
    }

    #[async_trait]
    impl CriterionVerifier for RetryOnceVerifier {
        async fn verify(
            &self,
            request: CriterionVerificationRequest,
        ) -> Result<CriterionVerificationResult, ApplicationError> {
            if !self.returned_pending.swap(true, Ordering::SeqCst) {
                return Err(ApplicationError::VerificationPending);
            }
            DevStaticVerifier.verify(request).await
        }
    }

    fn task_for(content_lock: &ContentLock, payload: serde_json::Value) -> VerificationTaskRecord {
        VerificationTaskRecord {
            task_id: task_id(),
            creator: creator(),
            submitted_proof_bundle: submitted_proof_bundle_for(content_lock, payload),
            status: VerificationTaskStatus::Pending,
            submitted_at: submitted_at(),
            started_at: None,
            completed_at: None,
            failure_message: None,
        }
    }

    fn submitted_proof_bundle_for(
        content_lock: &ContentLock,
        payload: serde_json::Value,
    ) -> SubmittedProofBundle {
        SubmittedProofBundle {
            version: SUBMITTED_PROOF_BUNDLE_VERSION,
            bundle_id: bundle_id(),
            pubky_lock_resource: PubkyLockResource::new(
                creator(),
                content_lock.content_lock_path().unwrap(),
            ),
            reader_public_key: None,
            proofs: vec![Proof {
                criterion_id: "criterion-1".to_owned(),
                verifier_type: VerifierType::DevStatic,
                payload,
            }],
        }
    }

    fn content_lock(satisfied: bool) -> ContentLock {
        content_lock_with_payload(serde_json::json!({ "satisfied": satisfied }))
    }

    fn content_lock_with_payload(params: serde_json::Value) -> ContentLock {
        ContentLock {
            version: CONTENT_LOCK_VERSION,
            creator: creator(),
            primary_resource: Some(GuardedResource {
                path: "/priv/locks.app/content/hello.txt".to_owned(),
                hash: GuardedResourceHash::from_bytes([7; 32]),
                content_type: "text/plain".to_owned(),
                size: 13,
            }),
            secondary_resources: Default::default(),
            criteria: vec![Criterion {
                criterion_id: "criterion-1".to_owned(),
                verifier_type: VerifierType::DevStatic,
                params,
            }],
            lock_logic: LockLogic::All {
                criteria: vec!["criterion-1".to_owned()],
            },
            access_policy: AccessPolicy {
                requested_credential_ttl_seconds: 900,
            },
            lock_server: LockServerConfig {
                override_: Some(
                    LockServerPubky::from_str(
                        "pubky7ir1ttte48bcp4zjychjyscicrwi1j34mtt91ptsafdbjmr8g9eo",
                    )
                    .unwrap(),
                ),
            },
            created_at: submitted_at(),
        }
    }

    fn runtime_config(environment: RuntimeEnvironment) -> LockServerRuntimeConfig {
        LockServerRuntimeConfig {
            bind_addr: "127.0.0.1:0".parse().unwrap(),
            credentials: LockServerCredentialsConfig {
                lock_server_secret_key: "/tmp/lock-server-test-secret.sess".into(),
                lock_server_public_key: LockServerPubky::from_str(
                    "pubky7ir1ttte48bcp4zjychjyscicrwi1j34mtt91ptsafdbjmr8g9eo",
                )
                .unwrap(),
                max_ttl_seconds: 3600,
            },
            worker: WorkerConfig {
                enabled: true,
                poll_interval_ms: 10,
                claim_timeout_seconds: 60,
                worker_id: "test-worker".to_owned(),
            },
            database: DatabaseConfig {
                url: "postgres://locks:locks@localhost/locks_test".to_owned(),
                max_connections: 1,
                run_migrations_on_startup: false,
            },
            runtime: RuntimeConfig { environment },
            creator_authority_acquisition:
                crate::config::CreatorAuthorityAcquisitionConfig::default(),
            secrets: SecretsConfig::default(),
            logging: LoggingConfig::default(),
            pubky: PubkyConfig::default(),
            pkdns: crate::config::PkdnsConfig::default(),
            rate_limits: RateLimitsConfig::default(),
            content_locks: ContentLocksConfig::default(),
            paykit: None,
        }
    }

    fn json_secret_payload() -> serde_json::Value {
        serde_json::json!({ "token": "super-secret-proof-token" })
    }

    fn task_id() -> TaskId {
        TaskId::from_str(TASK_ID).unwrap()
    }

    fn bundle_id() -> BundleId {
        BundleId::from_str(BUNDLE_ID).unwrap()
    }

    fn creator() -> CreatorPubky {
        CreatorPubky::from_str("pubkytkrq8zmwb8a3m9k15csu3q17qmfgqnp9dskbrg9uq1rydpyxp7qy").unwrap()
    }

    fn submitted_at() -> time::OffsetDateTime {
        datetime!(2026-05-29 12:00:00 UTC)
    }
}
