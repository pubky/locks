use std::{collections::BTreeMap, str::FromStr, sync::Arc};

use locks_core::{
    ids::{BundleId, CreatorPubky, GuardedResourceHash, PubkyLockResource, TaskId},
    lock_policy::{
        AccessPolicy, CONTENT_LOCK_VERSION, ContentLock, GuardedResource, LockLogic,
        LockServerConfig, VerifierType,
    },
    verification::{Proof, SUBMITTED_PROOF_BUNDLE_VERSION, SubmittedProofBundle},
};
use locks_service::{
    application::{
        models::{
            AccessCredential, AccessCredentialLookupKey, AccessCredentialRecord,
            ContentLockDeletionFailureCode, ContentLockDeletionJob, ContentLockDeletionPhase,
            ContentLockDeletionState, PrepareForceDeletionResult, VerificationTaskRecord,
            VerificationTaskStatus,
        },
        ports::{
            AccessCredentialStore, Clock, ContentLockDeletionRepository, VerificationTaskClaimer,
            VerificationTaskRepository,
        },
    },
    infrastructure::memory::{
        access_credentials::InMemoryAccessCredentialStore,
        content_lock_deletions::InMemoryContentLockDeletionRepository,
        verification_task_claims::InMemoryVerificationTaskClaimer,
        verification_task_deletion_fence::InMemoryVerificationTaskDeletionFence,
        verification_tasks::InMemoryVerificationTaskRepository,
    },
};
use serde_json::json;
use time::macros::datetime;
use uuid::Uuid;

const CREATOR: &str = "pubkytkrq8zmwb8a3m9k15csu3q17qmfgqnp9dskbrg9uq1rydpyxp7qy";
const NOW: time::OffsetDateTime = datetime!(2026-08-12 05:00:00 UTC);
const LEASE_END: time::OffsetDateTime = datetime!(2026-08-12 05:05:00 UTC);

#[derive(Debug)]
struct FixedClock(time::OffsetDateTime);

impl Clock for FixedClock {
    fn now(&self) -> time::OffsetDateTime {
        self.0
    }
}

#[tokio::test]
async fn frozen_manifest_identity_is_immutable_and_creator_lock_unique() {
    let repository = InMemoryContentLockDeletionRepository::with_verification_task_fence(Arc::new(
        InMemoryVerificationTaskDeletionFence::with_clock(Arc::new(FixedClock(NOW))),
    ));
    let lock = content_lock();
    let job = ContentLockDeletionJob::new(Uuid::new_v4(), lock.clone(), NOW).unwrap();

    repository.insert_job(job.clone()).await.unwrap();
    assert_eq!(
        repository
            .get_job(&job.creator, &job.lock_id)
            .await
            .unwrap(),
        Some(job.clone())
    );

    let duplicate = ContentLockDeletionJob::new(Uuid::new_v4(), lock, NOW).unwrap();
    assert!(repository.insert_job(duplicate).await.is_err());

    let mut duplicate_id =
        ContentLockDeletionJob::new(Uuid::new_v4(), content_lock(), NOW).unwrap();
    duplicate_id.job_id = job.job_id;
    assert!(repository.insert_job(duplicate_id).await.is_err());

    assert!(job.validate_frozen_identity().is_ok());
    let mut corrupted = job;
    corrupted
        .frozen_content_lock
        .access_policy
        .requested_credential_ttl_seconds += 1;
    assert!(corrupted.validate_frozen_identity().is_err());

    let mut malformed = ContentLockDeletionJob::new(Uuid::new_v4(), content_lock(), NOW).unwrap();
    malformed.state = ContentLockDeletionState::Running;
    assert!(repository.insert_job(malformed).await.is_err());
}

#[tokio::test]
async fn due_claims_reclaim_with_fresh_tokens_and_fence_stale_writes() {
    let repository = InMemoryContentLockDeletionRepository::with_verification_task_fence(Arc::new(
        InMemoryVerificationTaskDeletionFence::with_clock(Arc::new(FixedClock(NOW))),
    ));
    let job = ContentLockDeletionJob::new(Uuid::new_v4(), content_lock(), NOW).unwrap();
    repository.insert_job(job.clone()).await.unwrap();

    let first = repository
        .claim_next("worker-a", NOW, LEASE_END)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(first.job.state, ContentLockDeletionState::Running);
    assert_eq!(first.job.attempt_count, 1);
    assert!(
        repository
            .claim_next("worker-b", NOW, LEASE_END)
            .await
            .unwrap()
            .is_none()
    );

    let reclaimed = repository
        .claim_next(
            "worker-b",
            datetime!(2026-08-12 05:05:01 UTC),
            datetime!(2026-08-12 05:10:00 UTC),
        )
        .await
        .unwrap()
        .unwrap();
    assert_ne!(first.claim_token, reclaimed.claim_token);
    assert_eq!(reclaimed.job.attempt_count, 2);

    assert!(
        repository
            .advance_phase(
                job.job_id,
                "worker-a",
                first.claim_token,
                datetime!(2026-08-12 05:05:01 UTC),
                ContentLockDeletionPhase::StartPaymentDrain,
            )
            .await
            .unwrap()
            .is_none()
    );
    let advanced = repository
        .advance_phase(
            job.job_id,
            "worker-b",
            reclaimed.claim_token,
            datetime!(2026-08-12 05:06:00 UTC),
            ContentLockDeletionPhase::StartPaymentDrain,
        )
        .await
        .unwrap()
        .unwrap();
    assert_eq!(advanced.state, ContentLockDeletionState::Queued);
    assert_eq!(advanced.phase, ContentLockDeletionPhase::StartPaymentDrain);
    assert_eq!(advanced.attempt_count, 0);

    let next_claim = repository
        .claim_next(
            "worker-c",
            datetime!(2026-08-12 05:06:01 UTC),
            datetime!(2026-08-12 05:11:00 UTC),
        )
        .await
        .unwrap()
        .unwrap();
    assert!(
        repository
            .advance_phase(
                job.job_id,
                "worker-c",
                next_claim.claim_token,
                datetime!(2026-08-12 05:07:00 UTC),
                ContentLockDeletionPhase::DeleteContent,
            )
            .await
            .is_err()
    );
    let failed = repository
        .finish(
            job.job_id,
            "worker-c",
            next_claim.claim_token,
            datetime!(2026-08-12 05:07:00 UTC),
            Some(ContentLockDeletionFailureCode::TombstoneMissing),
        )
        .await
        .unwrap()
        .unwrap();
    assert_eq!(failed.state, ContentLockDeletionState::Failed);
    assert_eq!(
        failed.failure_code,
        Some(ContentLockDeletionFailureCode::TombstoneMissing)
    );
}

#[test]
fn failure_codes_are_a_closed_stable_vocabulary() {
    for (code, wire) in [
        (
            ContentLockDeletionFailureCode::TombstoneMissing,
            "tombstone_missing",
        ),
        (
            ContentLockDeletionFailureCode::TombstoneReplaced,
            "tombstone_replaced",
        ),
        (
            ContentLockDeletionFailureCode::RetryExhausted,
            "retry_exhausted",
        ),
        (
            ContentLockDeletionFailureCode::StateCorrupt,
            "state_corrupt",
        ),
    ] {
        assert_eq!(code.as_str(), wire);
        assert_eq!(
            wire.parse::<ContentLockDeletionFailureCode>().unwrap(),
            code
        );
    }
    assert!(
        "backend: secret"
            .parse::<ContentLockDeletionFailureCode>()
            .is_err()
    );
}

#[tokio::test]
async fn retry_due_time_and_force_receipts_are_durable_repository_facts() {
    let repository = InMemoryContentLockDeletionRepository::with_verification_task_fence(Arc::new(
        InMemoryVerificationTaskDeletionFence::with_clock(Arc::new(FixedClock(NOW))),
    ));
    let job = ContentLockDeletionJob::new(Uuid::new_v4(), content_lock(), NOW).unwrap();
    repository.insert_job(job.clone()).await.unwrap();
    let claimed = repository
        .claim_next("worker-a", NOW, LEASE_END)
        .await
        .unwrap()
        .unwrap();
    let retry_at = datetime!(2026-08-12 05:06:00 UTC);
    repository
        .schedule_retry(job.job_id, "worker-a", claimed.claim_token, NOW, retry_at)
        .await
        .unwrap()
        .unwrap();
    assert!(
        repository
            .claim_next("worker-b", NOW, LEASE_END)
            .await
            .unwrap()
            .is_none()
    );
    assert!(
        repository
            .claim_next("worker-b", retry_at, datetime!(2026-08-12 05:11:00 UTC))
            .await
            .unwrap()
            .is_some()
    );

    assert!(matches!(
        repository
            .prepare_force_deletion(&job.creator, &job.lock_id, NOW)
            .await
            .unwrap(),
        PrepareForceDeletionResult::Active(_)
    ));
    assert!(
        !repository
            .has_force_receipt(&job.creator, &job.lock_id)
            .await
            .unwrap()
    );
}

#[tokio::test]
async fn in_memory_deletion_enrolls_existing_ordinary_credentials_and_blocks_late_insertion() {
    let (verification_tasks, access, deletions) = in_memory_access_stack();
    let lock = content_lock();
    let job = ContentLockDeletionJob::new(Uuid::new_v4(), lock.clone(), NOW).unwrap();
    let task = verification_task(&job, VerificationTaskStatus::Completed);
    verification_tasks
        .insert_verification_task(task.clone())
        .await
        .unwrap();

    let ordinary = AccessCredential::new("ordinary-before-deletion");
    let ordinary_lookup = AccessCredentialLookupKey::derive(&ordinary);
    let original_expiry = NOW + time::Duration::minutes(10);
    access
        .insert_access_credential(
            &job.lock_id,
            ordinary_lookup.clone(),
            AccessCredentialRecord {
                creator: job.creator.clone(),
                bundle_id: task.submitted_proof_bundle.bundle_id.clone(),
                expires_at: original_expiry,
            },
        )
        .await
        .unwrap();
    deletions.insert_job(job.clone()).await.unwrap();

    let first = access
        .prepare_deletion_read(
            &ordinary_lookup,
            "/priv/locks.app/content/post.json",
            NOW,
            NOW + time::Duration::seconds(30),
        )
        .await
        .unwrap()
        .unwrap();
    let replay = access
        .prepare_deletion_read(
            &ordinary_lookup,
            "/priv/locks.app/content/post.json",
            NOW + time::Duration::minutes(1),
            NOW + time::Duration::minutes(2),
        )
        .await
        .unwrap()
        .unwrap();
    assert_eq!(first.claim_token, None);
    assert_eq!(replay, first);
    assert_eq!(
        access
            .get_access_credential(&ordinary_lookup)
            .await
            .unwrap()
            .unwrap()
            .expires_at,
        original_expiry
    );
    assert!(
        access
            .prepare_deletion_read(
                &ordinary_lookup,
                "/priv/locks.app/content/not-in-frozen-manifest.json",
                NOW,
                NOW + time::Duration::seconds(30),
            )
            .await
            .unwrap()
            .is_none()
    );

    assert_eq!(
        access
            .insert_access_credential(
                &job.lock_id,
                AccessCredentialLookupKey::derive(&AccessCredential::new("late")),
                AccessCredentialRecord {
                    creator: job.creator.clone(),
                    bundle_id: BundleId::from_str("000G40R40M30E209185GR38E1V").unwrap(),
                    expires_at: original_expiry,
                },
            )
            .await,
        Err(locks_service::application::errors::ApplicationError::ContentLockDeletionInProgress)
    );

    let drain_existing_claim = advance_to_phase(
        &deletions,
        &access,
        job.job_id,
        ContentLockDeletionPhase::DrainExistingCredentials,
    )
    .await;
    assert!(matches!(
        deletions
            .advance_phase(
                job.job_id,
                "worker-final",
                drain_existing_claim.claim_token,
                NOW,
                ContentLockDeletionPhase::IssueFinalCredentials,
            )
            .await,
        Err(
            locks_service::application::errors::ApplicationError::InvalidContentLockDeletionState { .. }
        )
    ));

    assert!(matches!(
        deletions
            .prepare_force_deletion(&job.creator, &job.lock_id, NOW)
            .await
            .unwrap(),
        PrepareForceDeletionResult::Active(_)
    ));
    assert_eq!(
        access
            .get_access_credential(&ordinary_lookup)
            .await
            .unwrap()
            .unwrap()
            .expires_at,
        original_expiry
    );
}

#[tokio::test]
async fn in_memory_cutoff_is_captured_under_the_shared_fence_not_from_the_caller() {
    let authoritative_cutoff = NOW + time::Duration::minutes(2);
    let fence = Arc::new(InMemoryVerificationTaskDeletionFence::with_clock(Arc::new(
        FixedClock(authoritative_cutoff),
    )));
    let verification_tasks = Arc::new(InMemoryVerificationTaskRepository::with_deletion_fence(
        Arc::clone(&fence),
    ));
    let access = Arc::new(
        InMemoryAccessCredentialStore::with_verification_task_repository_and_deletion_fence(
            verification_tasks.clone(),
            Arc::clone(&fence),
        ),
    );
    let deletions =
        InMemoryContentLockDeletionRepository::with_access_credentials_and_verification_task_fence(
            Arc::clone(&access),
            fence,
        );
    let job = ContentLockDeletionJob::new(Uuid::new_v4(), content_lock(), NOW).unwrap();
    let task = verification_task(&job, VerificationTaskStatus::Pending);
    let credential = AccessCredential::new("expired-while-waiting-for-cutoff-fence");
    let lookup = AccessCredentialLookupKey::derive(&credential);
    verification_tasks
        .insert_verification_task(task.clone())
        .await
        .unwrap();
    access
        .insert_access_credential(
            &job.lock_id,
            lookup.clone(),
            AccessCredentialRecord {
                creator: job.creator.clone(),
                bundle_id: task.submitted_proof_bundle.bundle_id,
                expires_at: NOW + time::Duration::minutes(1),
            },
        )
        .await
        .unwrap();

    deletions.insert_job(job.clone()).await.unwrap();

    assert_eq!(
        deletions
            .get_job(&job.creator, &job.lock_id)
            .await
            .unwrap()
            .unwrap()
            .deletion_started_at,
        authoritative_cutoff
    );
    assert!(!access.deletion_credential_enrolled(&lookup).await.unwrap());
}

#[tokio::test]
async fn failed_access_registration_leaves_no_job_or_task_ownership() {
    let lock = content_lock();
    let first_job = ContentLockDeletionJob::new(Uuid::new_v4(), lock.clone(), NOW).unwrap();
    let first_fence = Arc::new(InMemoryVerificationTaskDeletionFence::with_clock(Arc::new(
        FixedClock(NOW),
    )));
    let first_tasks = Arc::new(InMemoryVerificationTaskRepository::with_deletion_fence(
        Arc::clone(&first_fence),
    ));
    let access = Arc::new(
        InMemoryAccessCredentialStore::with_verification_task_repository_and_deletion_fence(
            first_tasks,
            Arc::clone(&first_fence),
        ),
    );
    let first_deletions =
        InMemoryContentLockDeletionRepository::with_access_credentials_and_verification_task_fence(
            Arc::clone(&access),
            first_fence,
        );
    tokio::time::timeout(
        std::time::Duration::from_secs(1),
        first_deletions.insert_job(first_job),
    )
    .await
    .expect("first deletion admission must not deadlock")
    .unwrap();

    let second_fence = Arc::new(InMemoryVerificationTaskDeletionFence::with_clock(Arc::new(
        FixedClock(NOW),
    )));
    let second_tasks = Arc::new(InMemoryVerificationTaskRepository::with_deletion_fence(
        Arc::clone(&second_fence),
    ));
    let second_job = ContentLockDeletionJob::new(Uuid::new_v4(), lock, NOW).unwrap();
    let task = verification_task(&second_job, VerificationTaskStatus::Pending);
    second_tasks
        .insert_verification_task(task.clone())
        .await
        .unwrap();
    let claimer =
        InMemoryVerificationTaskClaimer::with_deletion_fence(vec![task], Arc::clone(&second_fence));
    let second_deletions =
        InMemoryContentLockDeletionRepository::with_access_credentials_and_verification_task_fence(
            access,
            second_fence,
        );

    assert_eq!(
        tokio::time::timeout(
            std::time::Duration::from_secs(1),
            second_deletions.insert_job(second_job.clone()),
        )
        .await
        .expect("failed deletion admission must not deadlock"),
        Err(locks_service::application::errors::ApplicationError::ContentLockDeletionInProgress)
    );
    assert!(
        second_deletions
            .get_job(&second_job.creator, &second_job.lock_id)
            .await
            .unwrap()
            .is_none()
    );
    assert!(
        tokio::time::timeout(
            std::time::Duration::from_secs(1),
            claimer.claim_next_verification_task("worker", NOW, LEASE_END),
        )
        .await
        .expect("failed deletion admission must release task ownership locks")
        .unwrap()
        .is_some()
    );
}

#[tokio::test]
async fn in_memory_final_credential_is_exactly_replayable_and_reads_are_lease_fenced() {
    let (verification_tasks, access, deletions) = in_memory_access_stack();
    let job = ContentLockDeletionJob::new(Uuid::new_v4(), content_lock(), NOW).unwrap();
    let completed = verification_task(&job, VerificationTaskStatus::Pending)
        .transition_to(VerificationTaskStatus::InProgress, NOW, None)
        .unwrap()
        .transition_to(VerificationTaskStatus::Completed, NOW, None)
        .unwrap();
    let bundle_id = completed.submitted_proof_bundle.bundle_id.clone();
    verification_tasks
        .insert_verification_task(completed)
        .await
        .unwrap();
    deletions.insert_job(job.clone()).await.unwrap();
    let claimed = advance_to_final_issuance(&deletions, &access, job.job_id).await;
    let issuance_deadline = NOW + time::Duration::minutes(15);
    let read_deadline = NOW + time::Duration::minutes(30);
    assert!(
        access
            .initialize_final_access_windows(
                job.job_id,
                "worker-final",
                claimed.claim_token,
                NOW,
                issuance_deadline,
                read_deadline,
            )
            .await
            .unwrap()
    );
    assert!(
        access
            .initialize_final_access_windows(
                job.job_id,
                "worker-final",
                claimed.claim_token,
                NOW,
                NOW + time::Duration::minutes(20),
                NOW + time::Duration::minutes(40),
            )
            .await
            .unwrap()
    );

    let candidate = AccessCredential::new("final-secret-bearer");
    let first = access
        .issue_or_replay_final_credential(&job.creator, &bundle_id, NOW, candidate.clone())
        .await
        .unwrap()
        .unwrap();
    let replay = access
        .issue_or_replay_final_credential(
            &job.creator,
            &bundle_id,
            NOW + time::Duration::minutes(1),
            AccessCredential::new("different-candidate-must-not-win"),
        )
        .await
        .unwrap()
        .unwrap();
    assert_eq!(first, replay);
    assert_eq!(first.credential, candidate);
    assert_eq!(first.expires_at, read_deadline);
    assert!(!format!("{access:?}").contains("final-secret-bearer"));
    let boundary_replay = access
        .issue_or_replay_final_credential(
            &job.creator,
            &bundle_id,
            issuance_deadline,
            AccessCredential::new("boundary-candidate-must-not-win"),
        )
        .await
        .unwrap()
        .unwrap();
    assert_eq!(boundary_replay, first);

    let advanced = deletions
        .advance_phase(
            job.job_id,
            "worker-final",
            claimed.claim_token,
            NOW,
            ContentLockDeletionPhase::DrainFinalReads,
        )
        .await
        .unwrap()
        .unwrap();
    assert_eq!(advanced.phase, ContentLockDeletionPhase::DrainFinalReads);
    let phase_replay = access
        .issue_or_replay_final_credential(
            &job.creator,
            &bundle_id,
            issuance_deadline,
            AccessCredential::new("post-advance-candidate-must-not-win"),
        )
        .await
        .unwrap()
        .unwrap();
    assert_eq!(phase_replay, first);

    let lookup = AccessCredentialLookupKey::derive(&first.credential);
    let path = "/priv/locks.app/content/post.json";
    let first_claim = access
        .prepare_deletion_read(&lookup, path, NOW, NOW + time::Duration::minutes(1))
        .await
        .unwrap()
        .unwrap()
        .claim_token
        .unwrap();
    let equality_reclaim = access
        .prepare_deletion_read(
            &lookup,
            path,
            NOW + time::Duration::seconds(30),
            NOW + time::Duration::minutes(2),
        )
        .await
        .unwrap()
        .unwrap()
        .claim_token
        .unwrap();
    assert_ne!(equality_reclaim, first_claim);
    assert!(
        !access
            .release_deletion_read(&lookup, path, Uuid::new_v4(), NOW)
            .await
            .unwrap()
    );
    assert!(
        !access
            .release_deletion_read(
                &lookup,
                path,
                first_claim,
                NOW + time::Duration::seconds(30),
            )
            .await
            .unwrap()
    );
    assert!(
        access
            .release_deletion_read(
                &lookup,
                path,
                equality_reclaim,
                NOW + time::Duration::seconds(30),
            )
            .await
            .unwrap()
    );
    let stale_claim = access
        .prepare_deletion_read(
            &lookup,
            path,
            NOW + time::Duration::seconds(30),
            NOW + time::Duration::hours(1),
        )
        .await
        .unwrap()
        .unwrap()
        .claim_token
        .unwrap();
    let reclaim_time = NOW + time::Duration::seconds(60);
    assert!(
        !access
            .consume_deletion_read(&lookup, path, stale_claim, reclaim_time)
            .await
            .unwrap()
    );
    let recovered_claim = access
        .prepare_deletion_read(&lookup, path, reclaim_time, NOW + time::Duration::hours(1))
        .await
        .unwrap()
        .unwrap()
        .claim_token
        .unwrap();
    assert!(
        !access
            .consume_deletion_read(&lookup, path, stale_claim, reclaim_time)
            .await
            .unwrap()
    );
    assert!(
        access
            .consume_deletion_read(&lookup, path, recovered_claim, reclaim_time)
            .await
            .unwrap()
    );
    assert!(
        access
            .prepare_deletion_read(
                &lookup,
                path,
                NOW + time::Duration::minutes(3),
                NOW + time::Duration::minutes(4),
            )
            .await
            .unwrap()
            .is_none()
    );
}

#[tokio::test]
async fn mutable_task_completion_cannot_resolve_the_immutable_deletion_snapshot() {
    let (verification_tasks, access, deletions) = in_memory_access_stack();
    let job = ContentLockDeletionJob::new(Uuid::new_v4(), content_lock(), NOW).unwrap();
    let pending = verification_task(&job, VerificationTaskStatus::Pending);
    let bundle_id = pending.submitted_proof_bundle.bundle_id.clone();
    let task_id = pending.task_id;
    verification_tasks
        .insert_verification_task(pending.clone())
        .await
        .unwrap();
    deletions.insert_job(job.clone()).await.unwrap();

    let drain_claim = advance_to_phase(
        &deletions,
        &access,
        job.job_id,
        ContentLockDeletionPhase::DrainPayments,
    )
    .await;
    let completed = pending
        .transition_to(VerificationTaskStatus::InProgress, NOW, None)
        .unwrap()
        .transition_to(VerificationTaskStatus::Completed, NOW, None)
        .unwrap();
    verification_tasks
        .update_verification_task(completed)
        .await
        .unwrap();

    assert!(
        !access
            .final_credential_available(&job.creator, &bundle_id, NOW)
            .await
            .unwrap()
    );
    assert!(
        access
            .issue_or_replay_final_credential(
                &job.creator,
                &bundle_id,
                NOW,
                AccessCredential::new("mutable-completion-must-not-win"),
            )
            .await
            .unwrap()
            .is_none()
    );
    assert!(
        access
            .resolve_deletion_payment(
                job.job_id,
                "worker-final",
                drain_claim.claim_token,
                NOW,
                &task_id,
                VerificationTaskStatus::Completed,
            )
            .await
            .unwrap()
    );
    assert!(
        access
            .complete_deletion_payment_aggregate(
                job.job_id,
                "worker-final",
                drain_claim.claim_token,
                NOW,
            )
            .await
            .unwrap()
    );

    deletions
        .advance_phase(
            job.job_id,
            "worker-final",
            drain_claim.claim_token,
            NOW,
            ContentLockDeletionPhase::DrainExistingCredentials,
        )
        .await
        .unwrap()
        .unwrap();
    let existing_drain_claim = deletions
        .claim_next("worker-final", NOW, LEASE_END)
        .await
        .unwrap()
        .unwrap();
    deletions
        .advance_phase(
            job.job_id,
            "worker-final",
            existing_drain_claim.claim_token,
            NOW,
            ContentLockDeletionPhase::IssueFinalCredentials,
        )
        .await
        .unwrap()
        .unwrap();
    let final_claim = deletions
        .claim_next("worker-final", NOW, LEASE_END)
        .await
        .unwrap()
        .unwrap();
    access
        .initialize_final_access_windows(
            job.job_id,
            "worker-final",
            final_claim.claim_token,
            NOW,
            NOW + time::Duration::minutes(15),
            NOW + time::Duration::minutes(30),
        )
        .await
        .unwrap();
    assert!(
        access
            .final_credential_available(&job.creator, &bundle_id, NOW)
            .await
            .unwrap()
    );
}

#[tokio::test]
async fn in_memory_payment_drain_waits_for_pending_non_paykit_snapshot() {
    let (verification_tasks, access, deletions) = in_memory_access_stack();
    let job = ContentLockDeletionJob::new(Uuid::new_v4(), content_lock(), NOW).unwrap();
    let mut pending = verification_task(&job, VerificationTaskStatus::Pending);
    pending.submitted_proof_bundle.proofs[0].verifier_type = VerifierType::DevStatic;
    verification_tasks
        .insert_verification_task(pending)
        .await
        .unwrap();
    deletions.insert_job(job.clone()).await.unwrap();
    let drain_claim = advance_to_phase(
        &deletions,
        &access,
        job.job_id,
        ContentLockDeletionPhase::DrainPayments,
    )
    .await;

    assert!(matches!(
        tokio::time::timeout(
            std::time::Duration::from_secs(1),
            deletions.advance_phase(
                job.job_id,
                "worker-final",
                drain_claim.claim_token,
                NOW,
                ContentLockDeletionPhase::DrainExistingCredentials,
            ),
        )
        .await
        .expect("pending non-Paykit guard must not deadlock"),
        Err(
            locks_service::application::errors::ApplicationError::InvalidContentLockDeletionState { .. }
        )
    ));
}

#[tokio::test]
async fn in_memory_payment_drain_waits_for_pending_paykit_snapshot() {
    let (verification_tasks, access, deletions) = in_memory_access_stack();
    let job = ContentLockDeletionJob::new(Uuid::new_v4(), content_lock(), NOW).unwrap();
    verification_tasks
        .insert_verification_task(verification_task(&job, VerificationTaskStatus::Pending))
        .await
        .unwrap();
    deletions.insert_job(job.clone()).await.unwrap();
    let drain_claim = advance_to_phase(
        &deletions,
        &access,
        job.job_id,
        ContentLockDeletionPhase::DrainPayments,
    )
    .await;

    assert!(matches!(
        tokio::time::timeout(
            std::time::Duration::from_secs(1),
            deletions.advance_phase(
                job.job_id,
                "worker-final",
                drain_claim.claim_token,
                NOW,
                ContentLockDeletionPhase::DrainExistingCredentials,
            ),
        )
        .await
        .expect("pending Paykit guard must not deadlock"),
        Err(
            locks_service::application::errors::ApplicationError::InvalidContentLockDeletionState { .. }
        )
    ));
}

#[tokio::test]
async fn in_memory_payment_drain_waits_for_completed_aggregate() {
    let (verification_tasks, access, deletions) = in_memory_access_stack();
    let job = ContentLockDeletionJob::new(Uuid::new_v4(), content_lock(), NOW).unwrap();
    verification_tasks
        .insert_verification_task(verification_task(&job, VerificationTaskStatus::Completed))
        .await
        .unwrap();
    deletions.insert_job(job.clone()).await.unwrap();
    let drain_claim = advance_to_phase(
        &deletions,
        &access,
        job.job_id,
        ContentLockDeletionPhase::DrainPayments,
    )
    .await;

    assert!(
        !access
            .complete_deletion_payment_aggregate(
                job.job_id,
                "different-worker",
                drain_claim.claim_token,
                NOW,
            )
            .await
            .unwrap()
    );
    assert!(
        !access
            .complete_deletion_payment_aggregate(job.job_id, "worker-final", Uuid::new_v4(), NOW,)
            .await
            .unwrap()
    );
    assert!(matches!(
        tokio::time::timeout(
            std::time::Duration::from_secs(1),
            deletions.advance_phase(
                job.job_id,
                "worker-final",
                drain_claim.claim_token,
                NOW,
                ContentLockDeletionPhase::DrainExistingCredentials,
            ),
        )
        .await
        .expect("payment aggregate guard must not deadlock"),
        Err(
            locks_service::application::errors::ApplicationError::InvalidContentLockDeletionState { .. }
        )
    ));
}

#[tokio::test]
async fn in_memory_phase_and_success_finish_guards_preserve_access_obligations() {
    let (verification_tasks, access, deletions) = in_memory_access_stack();
    let job = ContentLockDeletionJob::new(Uuid::new_v4(), content_lock(), NOW).unwrap();
    let completed = verification_task(&job, VerificationTaskStatus::Pending)
        .transition_to(VerificationTaskStatus::InProgress, NOW, None)
        .unwrap()
        .transition_to(VerificationTaskStatus::Completed, NOW, None)
        .unwrap();
    let bundle_id = completed.submitted_proof_bundle.bundle_id.clone();
    verification_tasks
        .insert_verification_task(completed)
        .await
        .unwrap();
    deletions.insert_job(job.clone()).await.unwrap();

    let issue_claim = advance_to_phase(
        &deletions,
        &access,
        job.job_id,
        ContentLockDeletionPhase::IssueFinalCredentials,
    )
    .await;
    access
        .initialize_final_access_windows(
            job.job_id,
            "worker-final",
            issue_claim.claim_token,
            NOW,
            NOW + time::Duration::minutes(15),
            NOW + time::Duration::minutes(30),
        )
        .await
        .unwrap();
    let deadline = NOW + time::Duration::minutes(15);
    let deadline_claim = deletions
        .claim_next(
            "worker-final",
            deadline,
            deadline + time::Duration::minutes(5),
        )
        .await
        .unwrap()
        .unwrap();
    assert!(matches!(
        deletions
            .advance_phase(
                job.job_id,
                "worker-final",
                deadline_claim.claim_token,
                deadline,
                ContentLockDeletionPhase::DrainFinalReads,
            )
            .await,
        Err(
            locks_service::application::errors::ApplicationError::InvalidContentLockDeletionState { .. }
        )
    ));
    assert!(
        access
            .issue_or_replay_final_credential(
                &job.creator,
                &bundle_id,
                deadline,
                AccessCredential::new("fresh-at-deadline-must-not-issue"),
            )
            .await
            .unwrap()
            .is_none()
    );
    assert!(matches!(
        deletions
            .finish(
                job.job_id,
                "worker-final",
                deadline_claim.claim_token,
                deadline,
                None
            )
            .await,
        Err(
            locks_service::application::errors::ApplicationError::InvalidContentLockDeletionState { .. }
        )
    ));
}

#[tokio::test]
async fn in_memory_final_credential_eligibility_does_not_change_when_cutoff_credential_is_deleted()
{
    let (verification_tasks, access, deletions) = in_memory_access_stack();
    let job = ContentLockDeletionJob::new(Uuid::new_v4(), content_lock(), NOW).unwrap();
    let completed = verification_task(&job, VerificationTaskStatus::Pending)
        .transition_to(VerificationTaskStatus::InProgress, NOW, None)
        .unwrap()
        .transition_to(VerificationTaskStatus::Completed, NOW, None)
        .unwrap();
    let bundle_id = completed.submitted_proof_bundle.bundle_id.clone();
    verification_tasks
        .insert_verification_task(completed)
        .await
        .unwrap();

    let ordinary = AccessCredential::new("active-at-cutoff");
    let ordinary_lookup = AccessCredentialLookupKey::derive(&ordinary);
    let original_expiry = NOW + time::Duration::minutes(10);
    access
        .insert_access_credential(
            &job.lock_id,
            ordinary_lookup.clone(),
            AccessCredentialRecord {
                creator: job.creator.clone(),
                bundle_id: bundle_id.clone(),
                expires_at: original_expiry,
            },
        )
        .await
        .unwrap();
    deletions.insert_job(job.clone()).await.unwrap();
    let drain_existing_claim = advance_to_phase(
        &deletions,
        &access,
        job.job_id,
        ContentLockDeletionPhase::DrainExistingCredentials,
    )
    .await;
    assert!(matches!(
        deletions
            .advance_phase(
                job.job_id,
                "worker-final",
                drain_existing_claim.claim_token,
                NOW,
                ContentLockDeletionPhase::IssueFinalCredentials,
            )
            .await,
        Err(
            locks_service::application::errors::ApplicationError::InvalidContentLockDeletionState { .. }
        )
    ));

    let after_expiry = original_expiry;
    access
        .delete_access_credential(&ordinary_lookup)
        .await
        .unwrap();
    let after_expiry_claim = deletions
        .claim_next(
            "worker-final",
            after_expiry,
            after_expiry + time::Duration::minutes(5),
        )
        .await
        .unwrap()
        .unwrap();
    deletions
        .advance_phase(
            job.job_id,
            "worker-final",
            after_expiry_claim.claim_token,
            after_expiry,
            ContentLockDeletionPhase::IssueFinalCredentials,
        )
        .await
        .unwrap()
        .unwrap();
    let claimed = deletions
        .claim_next(
            "worker-final",
            after_expiry,
            after_expiry + time::Duration::minutes(5),
        )
        .await
        .unwrap()
        .unwrap();
    assert!(
        access
            .initialize_final_access_windows(
                job.job_id,
                "worker-final",
                claimed.claim_token,
                after_expiry,
                after_expiry + time::Duration::minutes(15),
                after_expiry + time::Duration::minutes(30),
            )
            .await
            .unwrap()
    );

    access
        .delete_access_credential(&ordinary_lookup)
        .await
        .unwrap();

    assert!(
        !access
            .final_credential_available(&job.creator, &bundle_id, after_expiry)
            .await
            .unwrap()
    );
    assert!(
        access
            .issue_or_replay_final_credential(
                &job.creator,
                &bundle_id,
                after_expiry,
                AccessCredential::new("must-remain-ineligible"),
            )
            .await
            .unwrap()
            .is_none()
    );
}

#[tokio::test]
async fn in_memory_final_credential_rejects_completed_non_paykit_snapshot() {
    let (verification_tasks, access, deletions) = in_memory_access_stack();
    let job = ContentLockDeletionJob::new(Uuid::new_v4(), content_lock(), NOW).unwrap();
    let mut task = verification_task(&job, VerificationTaskStatus::Completed);
    task.submitted_proof_bundle.proofs[0].verifier_type = VerifierType::DevStatic;
    let bundle_id = task.submitted_proof_bundle.bundle_id.clone();
    verification_tasks
        .insert_verification_task(task.clone())
        .await
        .unwrap();
    deletions.insert_job(job.clone()).await.unwrap();
    let claimed = advance_to_final_issuance(&deletions, &access, job.job_id).await;
    assert!(
        access
            .initialize_final_access_windows(
                job.job_id,
                "worker-final",
                claimed.claim_token,
                NOW,
                NOW + time::Duration::minutes(15),
                NOW + time::Duration::minutes(30),
            )
            .await
            .unwrap()
    );

    assert!(
        !access
            .final_credential_available(&job.creator, &bundle_id, NOW)
            .await
            .unwrap()
    );
    assert!(
        access
            .issue_or_replay_final_credential(
                &job.creator,
                &bundle_id,
                NOW,
                AccessCredential::new("must-not-be-issued"),
            )
            .await
            .unwrap()
            .is_none()
    );
}

fn in_memory_access_stack() -> (
    Arc<InMemoryVerificationTaskRepository>,
    Arc<InMemoryAccessCredentialStore>,
    InMemoryContentLockDeletionRepository,
) {
    let fence = Arc::new(InMemoryVerificationTaskDeletionFence::with_clock(Arc::new(
        FixedClock(NOW),
    )));
    let verification_tasks = Arc::new(InMemoryVerificationTaskRepository::with_deletion_fence(
        Arc::clone(&fence),
    ));
    let access = Arc::new(
        InMemoryAccessCredentialStore::with_verification_task_repository_and_deletion_fence(
            verification_tasks.clone(),
            Arc::clone(&fence),
        ),
    );
    let deletions =
        InMemoryContentLockDeletionRepository::with_access_credentials_and_verification_task_fence(
            Arc::clone(&access),
            fence,
        );
    (verification_tasks, access, deletions)
}

async fn advance_to_phase(
    deletions: &InMemoryContentLockDeletionRepository,
    access: &InMemoryAccessCredentialStore,
    job_id: Uuid,
    target: ContentLockDeletionPhase,
) -> locks_service::application::models::ClaimedContentLockDeletionJob {
    for next_phase in [
        ContentLockDeletionPhase::StartPaymentDrain,
        ContentLockDeletionPhase::DrainPayments,
        ContentLockDeletionPhase::DrainExistingCredentials,
        ContentLockDeletionPhase::IssueFinalCredentials,
    ] {
        let claimed = deletions
            .claim_next("worker-final", NOW, LEASE_END)
            .await
            .unwrap()
            .unwrap();
        if next_phase == ContentLockDeletionPhase::DrainExistingCredentials {
            assert!(
                access
                    .complete_deletion_payment_aggregate(
                        job_id,
                        "worker-final",
                        claimed.claim_token,
                        NOW,
                    )
                    .await
                    .unwrap()
            );
        }
        deletions
            .advance_phase(job_id, "worker-final", claimed.claim_token, NOW, next_phase)
            .await
            .unwrap()
            .unwrap();
        if next_phase == target {
            return deletions
                .claim_next("worker-final", NOW, LEASE_END)
                .await
                .unwrap()
                .unwrap();
        }
    }
    panic!("unsupported test target phase");
}

async fn advance_to_final_issuance(
    deletions: &InMemoryContentLockDeletionRepository,
    access: &InMemoryAccessCredentialStore,
    job_id: Uuid,
) -> locks_service::application::models::ClaimedContentLockDeletionJob {
    advance_to_phase(
        deletions,
        access,
        job_id,
        ContentLockDeletionPhase::IssueFinalCredentials,
    )
    .await
}

fn verification_task(
    job: &ContentLockDeletionJob,
    status: VerificationTaskStatus,
) -> VerificationTaskRecord {
    VerificationTaskRecord {
        task_id: TaskId::from_str("018fc6ec-2f3d-4f7e-8b7d-6f5c4b3a2d10").unwrap(),
        creator: job.creator.clone(),
        submitted_proof_bundle: SubmittedProofBundle {
            version: SUBMITTED_PROOF_BUNDLE_VERSION,
            bundle_id: BundleId::from_str("000G40R40M30E209185GR38E1W").unwrap(),
            pubky_lock_resource: PubkyLockResource::from_str(&format!(
                "{}/pub/locks.app/{}.json",
                job.creator, job.lock_id
            ))
            .unwrap(),
            reader_public_key: None,
            proofs: vec![Proof {
                criterion_id: "criterion-1".to_owned(),
                verifier_type: VerifierType::PaykitPayment,
                payload: json!({}),
            }],
        },
        status,
        submitted_at: NOW,
        started_at: None,
        completed_at: None,
        failure_message: None,
    }
}

fn content_lock() -> ContentLock {
    ContentLock {
        version: CONTENT_LOCK_VERSION,
        creator: CreatorPubky::from_str(CREATOR).unwrap(),
        primary_resource: Some(
            GuardedResource::new(
                "/priv/locks.app/content/post.json".to_owned(),
                GuardedResourceHash::from_bytes([7; 32]),
                "application/json".to_owned(),
                42,
            )
            .unwrap(),
        ),
        secondary_resources: BTreeMap::new(),
        criteria: vec![],
        lock_logic: LockLogic::All { criteria: vec![] },
        access_policy: AccessPolicy {
            requested_credential_ttl_seconds: 900,
        },
        lock_server: LockServerConfig { override_: None },
        created_at: datetime!(2026-08-12 04:00:00 UTC),
    }
}
