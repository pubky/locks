use std::collections::BTreeMap;
use std::str::FromStr;
use std::sync::Arc;
use std::sync::atomic::{AtomicI64, AtomicUsize, Ordering};

use async_trait::async_trait;
use locks_core::ids::{
    BundleId, ContentLockPath, CreatorPubky, GuardedResourceHash, PubkyLockResource, TaskId,
};
use locks_core::lock_policy::{
    AccessPolicy, ContentLockV2, GraphParticipantV2, GraphPolicyV2, GraphPredicateV2,
    GuardedResource, LockServerConfig,
};
use locks_core::verification::SubmittedProofBundleV2;
use locks_service::application::errors::ApplicationError;
use locks_service::application::models::{
    FOLDED_AUTH_POLL_ABANDONED_AFTER, FoldedAuthFlowSecret, FoldedAuthStateHash, FoldedAuthStatus,
    GraphTaskRecord, GraphTaskSnapshot, GraphTaskSnapshotV1, StartFoldedAuthAttempt,
    VerificationTaskStatus,
};
use locks_service::application::ports::{
    Clock, FoldedAuthApprovalPoll, FoldedAuthApprovalPollError, FoldedAuthApprovalPoller,
    FoldedAuthPollCall, FoldedAuthRepository, GraphTaskRepository,
};
use locks_service::application::use_cases::poll_folded_auth::{
    PollFoldedAuthAttempt, PollFoldedAuthOutcome,
};
use locks_service::infrastructure::folded_auth::FoldedAuthSecretCipher;
use locks_service::infrastructure::memory::folded_auth::InMemoryFoldedAuthRepository;
use locks_service::infrastructure::memory::graph_tasks::InMemoryGraphTaskRepository;
use locks_service::infrastructure::memory::verification_task_deletion_fence::InMemoryVerificationTaskDeletionFence;
use locks_service::infrastructure::runtime_master_key::RuntimeMasterKey;
use time::Duration;
use time::macros::datetime;
use tokio::sync::Notify;

const CREATOR: &str = "pubkytkrq8zmwb8a3m9k15csu3q17qmfgqnp9dskbrg9uq1rydpyxp7qy";
const BUNDLE_ID: &str = "000G40R40M30E209185GR38E1W";

#[tokio::test]
async fn pending_flow_restores_after_repository_reconstruction_without_exposing_secret() {
    let (graph, _clock) = graph_at(datetime!(2026-10-30 12:00:00 UTC));
    let cipher = folded_auth_cipher();
    let repository = InMemoryFoldedAuthRepository::new(graph, cipher.clone());
    let lock = graph_lock();
    let secret = FoldedAuthFlowSecret::new(b"relay-secret-flow-material".to_vec());
    let request = start_request(&lock, secret.clone());

    let started = repository.start_or_rotate(request).await.unwrap();
    assert_eq!(started.attempt.status, FoldedAuthStatus::Pending);
    assert!(!format!("{:?}", started.attempt).contains("relay-secret"));

    let rebuilt = repository.reconstruct(cipher);
    let restored = rebuilt
        .restore_pending(&started.attempt.state_hash)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(restored.flow_secret, secret);
    assert_eq!(restored.attempt.generation, 1);
    assert!(!format!("{restored:?}").contains("relay-secret"));
}

#[tokio::test]
async fn rotation_and_every_result_are_generation_fenced_and_terminal_states_erase_secret() {
    let (graph, clock) = graph_at(datetime!(2026-10-30 12:00:00 UTC));
    let repository = InMemoryFoldedAuthRepository::new(graph.clone(), folded_auth_cipher());
    let lock = graph_lock();
    let started_at = datetime!(2026-10-30 12:00:00 UTC);
    let first = repository
        .start_or_rotate(start_request_with(
            &lock,
            FoldedAuthStateHash::derive(b"state-one"),
            b"flow-one",
            started_at,
        ))
        .await
        .unwrap();
    assert!(
        graph
            .get_graph_task_by_handle(&creator(), &BundleId::from_str(BUNDLE_ID).unwrap())
            .await
            .unwrap()
            .is_none()
    );

    let second = repository
        .start_or_rotate(start_request_with(
            &lock,
            FoldedAuthStateHash::derive(b"state-two"),
            b"flow-two",
            started_at + Duration::SECOND,
        ))
        .await
        .unwrap();
    assert_eq!(second.attempt.generation, 2);
    assert_eq!(
        repository
            .get_by_state_hash(&first.attempt.state_hash)
            .await
            .unwrap()
            .unwrap()
            .status,
        FoldedAuthStatus::RestartRequired
    );
    assert!(
        repository
            .restore_pending(&first.attempt.state_hash)
            .await
            .unwrap()
            .is_none()
    );
    assert!(
        repository
            .confirm_pending_generation(&second.attempt.state_hash, 1, started_at)
            .await
            .unwrap()
            .is_none()
    );

    clock.set(second.attempt.expires_at);
    let expired = repository
        .confirm_pending_generation(&second.attempt.state_hash, 2, second.attempt.expires_at)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(expired.status, FoldedAuthStatus::Expired);
    assert!(
        repository
            .restore_pending(&second.attempt.state_hash)
            .await
            .unwrap()
            .is_none()
    );
    assert!(
        graph
            .get_graph_task_by_handle(&creator(), &BundleId::from_str(BUNDLE_ID).unwrap())
            .await
            .unwrap()
            .is_none()
    );
}

#[tokio::test]
async fn attempted_ambiguous_poll_durably_requires_restart_without_creating_task() {
    let (graph, _clock) = graph_at(datetime!(2026-10-30 12:00:00 UTC));
    let repository = Arc::new(InMemoryFoldedAuthRepository::new(
        graph.clone(),
        folded_auth_cipher(),
    ));
    let now = datetime!(2026-10-30 12:00:00 UTC);
    let started = repository
        .start_or_rotate(start_request_with(
            &graph_lock(),
            FoldedAuthStateHash::derive(b"ambiguous-state"),
            b"ambiguous-flow",
            now,
        ))
        .await
        .unwrap();
    let use_case = PollFoldedAuthAttempt::new(
        repository.clone(),
        Arc::new(AmbiguousPoller),
        Arc::new(FixedClock(now + Duration::SECOND)),
    );

    let outcome = use_case.execute(&started.attempt.state_hash).await.unwrap();
    assert!(matches!(outcome, PollFoldedAuthOutcome::RestartRequired(_)));
    assert_eq!(
        repository
            .get_by_state_hash(&started.attempt.state_hash)
            .await
            .unwrap()
            .unwrap()
            .status,
        FoldedAuthStatus::RestartRequired
    );
    assert!(
        graph
            .get_graph_task_by_handle(&creator(), &BundleId::from_str(BUNDLE_ID).unwrap())
            .await
            .unwrap()
            .is_none()
    );
}

struct FixedClock(time::OffsetDateTime);

impl Clock for FixedClock {
    fn now(&self) -> time::OffsetDateTime {
        self.0
    }
}

struct AmbiguousPoller;

#[async_trait]
impl FoldedAuthApprovalPoller for AmbiguousPoller {
    async fn poll_approval(&self, _flow_secret: &FoldedAuthFlowSecret) -> FoldedAuthPollCall {
        FoldedAuthPollCall::Attempted(Err(FoldedAuthApprovalPollError))
    }
}

#[tokio::test]
async fn accepted_grant_atomically_starts_graph_deadline_at_durable_acceptance() {
    let (graph, clock) = graph_at(datetime!(2026-10-30 12:00:00 UTC));
    let repository = Arc::new(InMemoryFoldedAuthRepository::new(
        graph.clone(),
        folded_auth_cipher(),
    ));
    let started_at = datetime!(2026-10-30 12:00:00 UTC);
    let accepted_at = started_at + Duration::minutes(5);
    let started = repository
        .start_or_rotate(start_request_with(
            &graph_lock(),
            FoldedAuthStateHash::derive(b"approved-state"),
            b"approved-flow",
            started_at,
        ))
        .await
        .unwrap();
    let use_case = PollFoldedAuthAttempt::new(
        repository.clone(),
        Arc::new(ApprovedPoller(reader())),
        Arc::new(FixedClock(accepted_at)),
    );

    clock.set(accepted_at);
    let outcome = use_case.execute(&started.attempt.state_hash).await.unwrap();
    let PollFoldedAuthOutcome::Accepted(task) = outcome else {
        panic!("expected accepted task");
    };
    assert_eq!(task.snapshot.as_v1().accepted_at, accepted_at);
    assert_eq!(
        task.snapshot.as_v1().deadline_at,
        accepted_at + Duration::minutes(5)
    );
    assert_eq!(task.snapshot.as_v1().reader, reader());
    assert_eq!(
        repository
            .get_by_state_hash(&started.attempt.state_hash)
            .await
            .unwrap()
            .unwrap()
            .status,
        FoldedAuthStatus::Accepted
    );
    assert!(
        repository
            .restore_pending(&started.attempt.state_hash)
            .await
            .unwrap()
            .is_none()
    );
    assert_eq!(
        graph
            .get_graph_task_by_handle(&creator(), &BundleId::from_str(BUNDLE_ID).unwrap())
            .await
            .unwrap(),
        Some(task.clone())
    );
    assert_eq!(
        use_case.execute(&started.attempt.state_hash).await.unwrap(),
        PollFoldedAuthOutcome::Accepted(task.clone())
    );
    clock.set(accepted_at + Duration::hours(24));
    assert_eq!(
        repository.purge_terminal_before(accepted_at).await.unwrap(),
        1
    );
    assert!(
        repository
            .get_by_state_hash(&started.attempt.state_hash)
            .await
            .unwrap()
            .is_none()
    );
    assert_eq!(
        graph
            .get_graph_task_by_handle(&creator(), &BundleId::from_str(BUNDLE_ID).unwrap())
            .await
            .unwrap(),
        Some(task)
    );
}

struct ApprovedPoller(CreatorPubky);

#[async_trait]
impl FoldedAuthApprovalPoller for ApprovedPoller {
    async fn poll_approval(&self, _flow_secret: &FoldedAuthFlowSecret) -> FoldedAuthPollCall {
        FoldedAuthPollCall::Attempted(Ok(FoldedAuthApprovalPoll::Approved {
            reader: self.0.clone(),
        }))
    }
}

struct MustNotPoll;

#[async_trait]
impl FoldedAuthApprovalPoller for MustNotPoll {
    async fn poll_approval(&self, _flow_secret: &FoldedAuthFlowSecret) -> FoldedAuthPollCall {
        panic!("durable dispatched marker must close recovery before polling")
    }
}

#[tokio::test]
async fn successful_exchange_then_accept_failure_restarts_after_abandoned_lease() {
    let (graph, clock) = graph_at(datetime!(2026-10-30 12:00:00 UTC));
    let cipher = folded_auth_cipher();
    let repository = Arc::new(InMemoryFoldedAuthRepository::new(
        graph.clone(),
        cipher.clone(),
    ));
    let now = datetime!(2026-10-30 12:00:00 UTC);
    let started = repository
        .start_or_rotate(start_request_with(
            &graph_lock(),
            FoldedAuthStateHash::derive(b"accept-storage-failure"),
            b"consumed-one-shot-flow",
            now,
        ))
        .await
        .unwrap();
    let duplicate_task_id = TaskId::from_str("018fc6ec-2f3d-4f7e-8b7d-6f5c4b3a2d10").unwrap();
    let other_task = GraphTaskRecord {
        task_id: duplicate_task_id,
        snapshot: GraphTaskSnapshot::V1(
            GraphTaskSnapshotV1::new(
                creator(),
                BundleId::from_str("100G40R40M30E209185GR38E1W").unwrap(),
                started.attempt.pubky_lock_resource.clone(),
                started.attempt.lock_hash,
                reader(),
                now,
                started.attempt.verification_window_seconds,
                started.attempt.graph.clone(),
            )
            .unwrap(),
        ),
        evaluator_revision: 0,
        status: VerificationTaskStatus::Pending,
        entitlement_to_publish: None,
        entitlement_publication_token: None,
        completed_at: None,
    };
    graph.insert_graph_task(other_task).await.unwrap();

    repository
        .mark_poll_dispatched(&started.attempt.state_hash, started.attempt.generation, now)
        .await
        .unwrap()
        .unwrap();
    assert!(
        repository
            .accept_and_insert_graph_task(
                &started.attempt.state_hash,
                started.attempt.generation,
                duplicate_task_id,
                reader(),
                now,
            )
            .await
            .is_err()
    );
    assert!(
        repository
            .get_by_state_hash(&started.attempt.state_hash)
            .await
            .unwrap()
            .unwrap()
            .poll_dispatched
    );

    let reconstructed = Arc::new(repository.reconstruct(cipher));
    let use_case = PollFoldedAuthAttempt::new(
        reconstructed.clone(),
        Arc::new(MustNotPoll),
        Arc::new(FixedClock(now + Duration::SECOND)),
    );
    clock.set(now + Duration::SECOND);
    assert!(matches!(
        use_case.execute(&started.attempt.state_hash).await.unwrap(),
        PollFoldedAuthOutcome::Pending(_)
    ));
    clock.set(now + FOLDED_AUTH_POLL_ABANDONED_AFTER);
    reconstructed
        .purge_terminal_before(now + FOLDED_AUTH_POLL_ABANDONED_AFTER)
        .await
        .unwrap();
    assert_eq!(
        reconstructed
            .get_by_state_hash(&started.attempt.state_hash)
            .await
            .unwrap()
            .unwrap()
            .status,
        FoldedAuthStatus::RestartRequired
    );
}

#[tokio::test]
async fn approval_returning_at_ttl_equality_expires_and_erases_without_task() {
    let (graph, clock) = graph_at(datetime!(2026-10-30 12:00:00 UTC));
    let repository = Arc::new(InMemoryFoldedAuthRepository::new(
        graph.clone(),
        folded_auth_cipher(),
    ));
    let started_at = datetime!(2026-10-30 12:00:00 UTC);
    let started = repository
        .start_or_rotate(start_request_with(
            &graph_lock(),
            FoldedAuthStateHash::derive(b"late-approval-state"),
            b"late-approval-flow",
            started_at,
        ))
        .await
        .unwrap();
    let use_case = PollFoldedAuthAttempt::new(
        repository.clone(),
        Arc::new(ApprovedPoller(reader())),
        Arc::new(SequenceClock {
            calls: AtomicUsize::new(0),
            before: started_at + Duration::minutes(9),
            after: started_at + Duration::minutes(10),
        }),
    );

    clock.set(started.attempt.expires_at);
    assert!(matches!(
        use_case.execute(&started.attempt.state_hash).await.unwrap(),
        PollFoldedAuthOutcome::Expired(_)
    ));
    assert!(
        repository
            .restore_pending(&started.attempt.state_hash)
            .await
            .unwrap()
            .is_none()
    );
    assert!(
        graph
            .get_graph_task_by_handle(&creator(), &BundleId::from_str(BUNDLE_ID).unwrap())
            .await
            .unwrap()
            .is_none()
    );
}

struct SequenceClock {
    calls: AtomicUsize,
    before: time::OffsetDateTime,
    after: time::OffsetDateTime,
}

impl Clock for SequenceClock {
    fn now(&self) -> time::OffsetDateTime {
        if self.calls.fetch_add(1, Ordering::SeqCst) == 0 {
            self.before
        } else {
            self.after
        }
    }
}

#[tokio::test]
async fn ttl_equality_expires_before_poll_and_time_overflow_is_rejected_atomically() {
    let (graph, clock) = graph_at(datetime!(2026-10-30 12:00:00 UTC));
    let repository = Arc::new(InMemoryFoldedAuthRepository::new(
        graph,
        folded_auth_cipher(),
    ));
    let now = datetime!(2026-10-30 12:00:00 UTC);
    let started = repository
        .start_or_rotate(start_request_with(
            &graph_lock(),
            FoldedAuthStateHash::derive(b"ttl-state"),
            b"ttl-flow",
            now,
        ))
        .await
        .unwrap();
    let use_case = PollFoldedAuthAttempt::new(
        repository.clone(),
        Arc::new(NotAttemptedPoller),
        Arc::new(FixedClock(now + Duration::minutes(10))),
    );
    clock.set(started.attempt.expires_at);
    assert!(matches!(
        use_case.execute(&started.attempt.state_hash).await.unwrap(),
        PollFoldedAuthOutcome::Expired(_)
    ));
    assert!(
        repository
            .restore_pending(&started.attempt.state_hash)
            .await
            .unwrap()
            .is_none()
    );

    let overflow_time = time::OffsetDateTime::from_unix_timestamp(253_402_300_799).unwrap();
    let overflow_hash = FoldedAuthStateHash::derive(b"overflow-state");
    clock.set(overflow_time);
    assert!(
        repository
            .start_or_rotate(start_request_with(
                &graph_lock(),
                overflow_hash.clone(),
                b"overflow-flow",
                overflow_time,
            ))
            .await
            .is_err()
    );
    assert!(
        repository
            .get_by_state_hash(&overflow_hash)
            .await
            .unwrap()
            .is_none()
    );
}

#[tokio::test]
async fn pre_poll_failure_remains_pending_and_retryable() {
    let (graph, _clock) = graph_at(datetime!(2026-10-30 12:00:00 UTC));
    let repository = Arc::new(InMemoryFoldedAuthRepository::new(
        graph,
        folded_auth_cipher(),
    ));
    let now = datetime!(2026-10-30 12:00:00 UTC);
    let started = repository
        .start_or_rotate(start_request_with(
            &graph_lock(),
            FoldedAuthStateHash::derive(b"pre-poll-state"),
            b"malformed-flow",
            now,
        ))
        .await
        .unwrap();
    let use_case = PollFoldedAuthAttempt::new(
        repository.clone(),
        Arc::new(NotAttemptedPoller),
        Arc::new(FixedClock(now + Duration::SECOND)),
    );

    assert!(matches!(
        use_case.execute(&started.attempt.state_hash).await,
        Err(ApplicationError::VerificationDependencyUnavailable)
    ));
    assert_eq!(
        repository
            .get_by_state_hash(&started.attempt.state_hash)
            .await
            .unwrap()
            .unwrap()
            .status,
        FoldedAuthStatus::Pending
    );
}

struct NotAttemptedPoller;

#[async_trait]
impl FoldedAuthApprovalPoller for NotAttemptedPoller {
    async fn poll_approval(&self, _flow_secret: &FoldedAuthFlowSecret) -> FoldedAuthPollCall {
        FoldedAuthPollCall::TransientBeforePoll(ApplicationError::VerificationDependencyUnavailable)
    }
}

struct UnrecoverableBeforePoll;

#[async_trait]
impl FoldedAuthApprovalPoller for UnrecoverableBeforePoll {
    async fn poll_approval(&self, _flow_secret: &FoldedAuthFlowSecret) -> FoldedAuthPollCall {
        FoldedAuthPollCall::UnrecoverableBeforePoll
    }
}

#[tokio::test]
async fn unrecoverable_restore_failure_requires_restart_and_erases_pending_secret() {
    let (graph, _clock) = graph_at(datetime!(2026-10-30 12:00:00 UTC));
    let repository = Arc::new(InMemoryFoldedAuthRepository::new(
        graph,
        folded_auth_cipher(),
    ));
    let now = datetime!(2026-10-30 12:00:00 UTC);
    let started = repository
        .start_or_rotate(start_request_with(
            &graph_lock(),
            FoldedAuthStateHash::derive(b"unrecoverable-state"),
            b"unrecoverable-flow",
            now,
        ))
        .await
        .unwrap();
    let use_case = PollFoldedAuthAttempt::new(
        repository.clone(),
        Arc::new(UnrecoverableBeforePoll),
        Arc::new(FixedClock(now + Duration::SECOND)),
    );

    assert!(matches!(
        use_case.execute(&started.attempt.state_hash).await.unwrap(),
        PollFoldedAuthOutcome::RestartRequired(_)
    ));
    assert_eq!(
        repository
            .get_by_state_hash(&started.attempt.state_hash)
            .await
            .unwrap()
            .unwrap()
            .status,
        FoldedAuthStatus::RestartRequired
    );
}

#[tokio::test]
async fn blocked_poll_holds_no_repository_lock_and_late_result_cannot_mutate_rotation() {
    let (graph, _clock) = graph_at(datetime!(2026-10-30 12:00:00 UTC));
    let repository = Arc::new(InMemoryFoldedAuthRepository::new(
        graph,
        folded_auth_cipher(),
    ));
    let now = datetime!(2026-10-30 12:00:00 UTC);
    let first = repository
        .start_or_rotate(start_request_with(
            &graph_lock(),
            FoldedAuthStateHash::derive(b"blocked-state-one"),
            b"blocked-flow-one",
            now,
        ))
        .await
        .unwrap();
    let entered = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let use_case = Arc::new(PollFoldedAuthAttempt::new(
        repository.clone(),
        Arc::new(BlockingAmbiguousPoller {
            entered: entered.clone(),
            release: release.clone(),
        }),
        Arc::new(FixedClock(now + Duration::SECOND)),
    ));
    let state_hash = first.attempt.state_hash.clone();
    let polling = tokio::spawn(async move { use_case.execute(&state_hash).await.unwrap() });
    entered.notified().await;

    let second = tokio::time::timeout(
        std::time::Duration::from_secs(1),
        repository.start_or_rotate(start_request_with(
            &graph_lock(),
            FoldedAuthStateHash::derive(b"blocked-state-two"),
            b"blocked-flow-two",
            now + Duration::SECOND,
        )),
    )
    .await
    .expect("rotation must not wait on approval I/O")
    .unwrap();
    release.notify_one();

    assert_eq!(polling.await.unwrap(), PollFoldedAuthOutcome::Stale);
    assert_eq!(
        repository
            .get_by_state_hash(&second.attempt.state_hash)
            .await
            .unwrap()
            .unwrap()
            .status,
        FoldedAuthStatus::Pending
    );
}

struct BlockingAmbiguousPoller {
    entered: Arc<Notify>,
    release: Arc<Notify>,
}

#[async_trait]
impl FoldedAuthApprovalPoller for BlockingAmbiguousPoller {
    async fn poll_approval(&self, _flow_secret: &FoldedAuthFlowSecret) -> FoldedAuthPollCall {
        self.entered.notify_one();
        self.release.notified().await;
        FoldedAuthPollCall::Attempted(Err(FoldedAuthApprovalPollError))
    }
}

#[derive(Debug)]
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

fn graph_at(now: time::OffsetDateTime) -> (Arc<InMemoryGraphTaskRepository>, Arc<MutableClock>) {
    let clock = Arc::new(MutableClock::new(now));
    let fence = Arc::new(InMemoryVerificationTaskDeletionFence::with_clock(
        clock.clone(),
    ));
    (
        Arc::new(InMemoryGraphTaskRepository::with_deletion_fence(fence)),
        clock,
    )
}

fn folded_auth_cipher() -> FoldedAuthSecretCipher {
    let master_key =
        RuntimeMasterKey::from_base64url("AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA").unwrap();
    FoldedAuthSecretCipher::from_runtime_master_key(&master_key)
}

fn reader() -> CreatorPubky {
    CreatorPubky::from_str("pubkyorhzqdiexwmi6iidktucgud63ufa5nwtsuzdxe176a8izd6jsqky").unwrap()
}

fn start_request(
    lock: &ContentLockV2,
    flow_secret: FoldedAuthFlowSecret,
) -> StartFoldedAuthAttempt {
    start_request_with(
        lock,
        FoldedAuthStateHash::derive(b"public-state-handle"),
        flow_secret.expose_bytes(),
        datetime!(2026-10-30 12:00:00 UTC),
    )
}

fn start_request_with(
    lock: &ContentLockV2,
    state_hash: FoldedAuthStateHash,
    flow_secret: &[u8],
    created_at: time::OffsetDateTime,
) -> StartFoldedAuthAttempt {
    let bundle = SubmittedProofBundleV2 {
        version: 2,
        bundle_id: BundleId::from_str(BUNDLE_ID).unwrap(),
        pubky_lock_resource: PubkyLockResource::new(
            creator(),
            ContentLockPath::from_lock_id(lock.lock_id().unwrap()),
        ),
        reader_pubky: None,
    };
    StartFoldedAuthAttempt {
        state_hash,
        submitted_proof_bundle: bundle.clone(),
        pubky_lock_resource: bundle.pubky_lock_resource.clone(),
        lock_hash: *lock.lock_hash().unwrap().as_bytes(),
        verification_window_seconds: lock.verification_window_seconds,
        graph: lock.graph.clone().unwrap(),
        flow_secret: FoldedAuthFlowSecret::new(flow_secret.to_vec()),
        created_at,
    }
}

fn graph_lock() -> ContentLockV2 {
    ContentLockV2 {
        version: 2,
        creator: creator(),
        primary_resource: Some(
            GuardedResource::new(
                "/priv/app.locks/content/item.txt",
                GuardedResourceHash::from_bytes([3; 32]),
                "text/plain",
                4,
            )
            .unwrap(),
        ),
        secondary_resources: BTreeMap::new(),
        payment: None,
        graph: Some(GraphPolicyV2 {
            predicates: vec![GraphPredicateV2::Follows {
                follower: GraphParticipantV2::Reader,
                followed: GraphParticipantV2::Creator,
            }],
            operator: None,
        }),
        verification_window_seconds: 300,
        access_policy: AccessPolicy {
            requested_credential_ttl_seconds: 300,
        },
        lock_server: LockServerConfig { override_: None },
        created_at: datetime!(2026-10-30 11:00:00 UTC),
    }
}

fn creator() -> CreatorPubky {
    CreatorPubky::from_str(CREATOR).unwrap()
}
