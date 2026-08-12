use std::{collections::BTreeMap, str::FromStr};

use locks_core::{
    ids::{CreatorPubky, GuardedResourceHash},
    lock_policy::{
        AccessPolicy, CONTENT_LOCK_VERSION, ContentLock, GuardedResource, LockLogic,
        LockServerConfig,
    },
};
use locks_service::{
    application::{
        models::{
            ContentLockDeletionFailureCode, ContentLockDeletionJob, ContentLockDeletionPhase,
            ContentLockDeletionState,
        },
        ports::ContentLockDeletionRepository,
    },
    infrastructure::memory::content_lock_deletions::InMemoryContentLockDeletionRepository,
};
use time::macros::datetime;
use uuid::Uuid;

const CREATOR: &str = "pubkytkrq8zmwb8a3m9k15csu3q17qmfgqnp9dskbrg9uq1rydpyxp7qy";
const NOW: time::OffsetDateTime = datetime!(2026-08-12 05:00:00 UTC);
const LEASE_END: time::OffsetDateTime = datetime!(2026-08-12 05:05:00 UTC);

#[tokio::test]
async fn frozen_manifest_identity_is_immutable_and_creator_lock_unique() {
    let repository = InMemoryContentLockDeletionRepository::new();
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
    let repository = InMemoryContentLockDeletionRepository::new();
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
    let repository = InMemoryContentLockDeletionRepository::new();
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

    assert!(
        repository
            .request_force(&job.creator, &job.lock_id, NOW)
            .await
            .unwrap()
    );
    assert!(
        !repository
            .request_force(&job.creator, &job.lock_id, NOW)
            .await
            .unwrap()
    );
    repository
        .record_force_receipt(&job.creator, &job.lock_id, NOW)
        .await
        .unwrap();
    repository
        .record_force_receipt(&job.creator, &job.lock_id, NOW)
        .await
        .unwrap();
    assert!(
        repository
            .has_force_receipt(&job.creator, &job.lock_id)
            .await
            .unwrap()
    );
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
