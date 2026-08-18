use std::collections::HashMap;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::path::PathBuf;
use std::str::FromStr;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use axum::body::{Body, to_bytes};
use axum::extract::ConnectInfo;
use axum::http::{Request, StatusCode, header};
use axum::routing::post;
use locks_core::content_lock_deletion::ContentLockDeletionTombstone;
use locks_core::ids::{
    BundleId, ContentLockPath, CreatorPubky, GuardedResourceHash, LockServerPubky,
    PubkyLockResource, TaskId,
};
use locks_core::lock_policy::{
    AccessPolicy, CONTENT_LOCK_VERSION, ContentLock, Criterion, GuardedResource, LockLogic,
    LockServerConfig, VerifierType,
};
use locks_core::verification::{Proof, SUBMITTED_PROOF_BUNDLE_VERSION, SubmittedProofBundle};
use locks_server::api::routes::router;
use locks_server::app_state::{AppState, ReaderPubkyResolver, RuntimeSecretCiphers};
use locks_server::config::{
    ContentLocksConfig, CreatorAuthorityAcquisitionConfig, DatabaseConfig, DeletionConfig,
    FilesystemLockServerIdentityProvider, LockServerCredentialsConfig, LockServerIdentityProvider,
    LockServerRuntimeConfig, LoggingConfig, PaykitConfig, PkdnsConfig, PubkyConfig,
    RateLimitsConfig, RuntimeConfig, RuntimeEnvironment, SecretsConfig, WorkerConfig,
};
use locks_server::deletion_worker::{ClaimedDeletionExecutor, RuntimeClaimedDeletionExecutor};
use locks_server::testing::TestServerApp;
use locks_server::worker::{VerificationWorker, WorkerTick};
use locks_service::application::models::{
    AccessCredential, AccessCredentialLookupKey, ClaimedContentLockDeletionJob,
    ContentLockDeletionJob, ContentLockDeletionPhase, ContentLockDeletionState,
    ContentLockOwnershipStatus, CreatorAuthorityAuthKind, CreatorAuthorityRecord,
    CreatorAuthoritySecret, FrontendSessionToken, GuardedResourceRecord,
    PrepareForceDeletionResult, VerificationTaskRecord, VerificationTaskStatus,
};
use locks_service::application::ports::{
    ContentLockDeletionActionAcquireResult, ContentLockDeletionActionClaim,
    ContentLockDeletionActionGuard, ContentLockDeletionActionOwnership,
    ContentLockDeletionRepository, ContentLockTombstoneRepository, GuardedResourceReadback,
    GuardedResourceRepository, TombstoneReadback, VerificationTaskRepository,
};
use locks_service::infrastructure::final_credentials::FinalCredentialCipher;
use locks_service::infrastructure::memory::{
    content_lock_tombstones::InMemoryContentLockTombstoneRepository,
    content_locks::InMemoryContentLockRepository, entitlements::InMemoryEntitlementRepository,
    guarded_resources::InMemoryGuardedResourceRepository,
    lock_service_pointers::InMemoryLockServicePointerRepository,
};
use locks_service::infrastructure::postgres::{
    CreatorAuthoritySecretCipher, PostgresContentLockDeletionActionOwnership,
    PostgresContentLockDeletionRepository, PostgresVerificationTaskRepository, run_migrations,
};
use serde_json::{Value, json};
use sqlx::postgres::PgPoolOptions;
use sqlx::{Connection, Executor, PgConnection, PgPool};
use time::{OffsetDateTime, macros::datetime};
use tower::ServiceExt;

const BUNDLE_ID: &str = "000G40R40M30E209185GR38E1W";

#[tokio::test]
async fn postgres_deletion_action_ownership_excludes_overlap_and_reacquires_after_release() {
    let Some(database) = TestDatabase::create().await else {
        return;
    };
    let first_owner = PostgresContentLockDeletionActionOwnership::new(database.pool().clone());
    let second_owner = PostgresContentLockDeletionActionOwnership::new(database.pool().clone());
    let deletions = PostgresContentLockDeletionRepository::new(database.pool().clone());
    let job = ContentLockDeletionJob::new(
        uuid::Uuid::new_v4(),
        content_lock(),
        OffsetDateTime::now_utc(),
    )
    .unwrap();
    deletions.insert_job(job).await.unwrap();
    let claimed = deletions
        .claim_next("ownership-worker", time::Duration::minutes(1))
        .await
        .unwrap()
        .unwrap();

    let first_guard = expect_action_acquired(
        first_owner
            .try_acquire(deletion_action_claim(&claimed, "ownership-worker", false))
            .await
            .unwrap(),
    );
    assert!(matches!(
        second_owner
            .try_acquire(deletion_action_claim(&claimed, "ownership-worker", false,))
            .await
            .unwrap(),
        ContentLockDeletionActionAcquireResult::Busy
    ));

    first_guard.release().await.unwrap();
    let replacement_guard = expect_action_acquired(
        second_owner
            .try_acquire(deletion_action_claim(&claimed, "ownership-worker", false))
            .await
            .unwrap(),
    );
    replacement_guard.release().await.unwrap();

    database.cleanup().await;
}

#[tokio::test]
async fn postgres_missed_final_issuance_terminalizes_with_closed_creator_failure_without_external_action()
 {
    let Some(database) = TestDatabase::create().await else {
        return;
    };
    let lock = content_lock();
    let external = Arc::new(CrashExternalRepository::with_tombstone_and_resources(&lock));
    let state = deletion_app_state(database.pool().clone(), Arc::clone(&external));
    let tasks = PostgresVerificationTaskRepository::new(database.pool().clone());
    let now = OffsetDateTime::now_utc();
    let mut task = VerificationTaskRecord {
        task_id: TaskId::from_str(&uuid::Uuid::new_v4().to_string()).unwrap(),
        creator: lock.creator.clone(),
        submitted_proof_bundle: submitted_proof_bundle_for(&lock),
        status: VerificationTaskStatus::Completed,
        submitted_at: now - time::Duration::hours(1),
        started_at: Some(now - time::Duration::hours(1)),
        completed_at: Some(now - time::Duration::minutes(30)),
        failure_message: None,
    };
    task.submitted_proof_bundle.proofs[0].verifier_type = VerifierType::PaykitPayment;
    tasks.insert_verification_task(task).await.unwrap();

    let job = ContentLockDeletionJob::new(uuid::Uuid::new_v4(), lock, now).unwrap();
    let deletions = PostgresContentLockDeletionRepository::new(database.pool().clone());
    deletions.insert_job(job.clone()).await.unwrap();
    sqlx::query(
        "UPDATE content_lock_deletion_task_snapshot
         SET resolved_status = 'completed', resolved_at = $2,
             final_credential_eligible_at = $2
         WHERE deletion_job_id = $1",
    )
    .bind(job.job_id)
    .bind(now - time::Duration::minutes(30))
    .execute(database.pool())
    .await
    .unwrap();
    sqlx::query(
        "UPDATE content_lock_deletion_jobs
         SET phase = 'issue_final_credentials', final_issuance_started_at = $2,
             final_credential_issuance_deadline = $3, final_read_deadline = $4
         WHERE job_id = $1",
    )
    .bind(job.job_id)
    .bind(now - time::Duration::minutes(20))
    .bind(now - time::Duration::minutes(5))
    .bind(now + time::Duration::minutes(10))
    .execute(database.pool())
    .await
    .unwrap();
    let claim = deletions
        .claim_next(
            "missed-issuance-worker",
            (now + time::Duration::minutes(5)) - (now),
        )
        .await
        .unwrap()
        .unwrap();

    assert_eq!(
        RuntimeClaimedDeletionExecutor::new(state.clone())
            .execute_claimed(claim, "missed-issuance-worker")
            .await
            .outcome,
        locks_service::application::use_cases::execute_content_lock_deletion_phase::DeletionPhaseExecutionOutcome::TerminalFailed
    );
    let failed = deletions
        .get_job(&job.creator, &job.lock_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(failed.state, ContentLockDeletionState::Failed);
    assert_eq!(
        failed.failure_code.map(|code| code.as_str()),
        Some("state_corrupt")
    );
    assert!(external.operations().is_empty());
    assert_eq!(external.resource_read_count(), 0);
    assert_eq!(external.resource_delete_count(), 0);

    let app = TestServerApp::from_state(state);
    let token = "missed-issuance-creator-session";
    app.insert_frontend_session_for_test(
        FrontendSessionToken::new(token),
        job.creator.clone(),
        now + time::Duration::hours(1),
    )
    .await
    .unwrap();
    let response = app
        .router()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri(format!("/creator/content-locks/{}/deletion", job.lock_id))
                .header(header::AUTHORIZATION, format!("Bearer {token}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response_json(response).await,
        json!({
            "lock_id": job.lock_id,
            "status": "failed",
            "failure_code": "state_corrupt"
        })
    );

    database.cleanup().await;
}

#[tokio::test]
async fn postgres_graceful_withdraw_crash_reclaims_without_republishing_or_stale_advance() {
    let Some(database) = TestDatabase::create().await else {
        return;
    };
    let external = Arc::new(CrashExternalRepository::with_original(content_lock()));
    let deletions = PostgresContentLockDeletionRepository::new(database.pool().clone());
    let ownership = PostgresContentLockDeletionActionOwnership::new(database.pool().clone());
    let now = OffsetDateTime::now_utc();
    let job = ContentLockDeletionJob::new(uuid::Uuid::new_v4(), content_lock(), now).unwrap();
    deletions.insert_job(job.clone()).await.unwrap();

    let (stale, crash_guard) =
        claim_deletion_action(&deletions, &ownership, "withdraw-crashed", false).await;
    let tombstone = ContentLockDeletionTombstone::new(job.lock_id.clone(), now);
    assert_eq!(
        external
            .withdraw_content_lock(
                job.creator.clone(),
                ContentLockPath::from_lock_id(job.lock_id.clone()),
                &job.frozen_content_lock,
                &tombstone,
            )
            .await
            .unwrap(),
        TombstoneReadback::Exact
    );
    drop(crash_guard);

    let fresh = expire_and_reclaim_deletion(
        database.pool(),
        &deletions,
        job.job_id,
        "withdraw-reclaimed",
    )
    .await;
    assert_ne!(fresh.claim_token, stale.claim_token);
    assert!(matches!(
        deletions
            .advance_phase(
                job.job_id,
                "withdraw-crashed",
                stale.claim_token,
                ContentLockDeletionPhase::StartPaymentDrain,
            )
            .await
            .unwrap(),
        locks_service::application::models::AdvanceContentLockDeletionPhaseResult::ClaimLost
    ));

    let recreated = deletion_app_state(database.pool().clone(), Arc::clone(&external));
    assert_eq!(
        RuntimeClaimedDeletionExecutor::new(recreated)
            .execute_claimed(fresh, "withdraw-reclaimed")
            .await
            .outcome,
        locks_service::application::use_cases::execute_content_lock_deletion_phase::DeletionPhaseExecutionOutcome::Progressed
    );
    assert_eq!(external.tombstone_write_count(), 1);
    assert_eq!(external.withdraw_call_count(), 2);
    assert_eq!(
        deletions
            .get_job(&job.creator, &job.lock_id)
            .await
            .unwrap()
            .unwrap()
            .phase,
        ContentLockDeletionPhase::StartPaymentDrain
    );
    let release_check = deletions
        .claim_next("withdraw-release-check", time::Duration::minutes(1))
        .await
        .unwrap()
        .unwrap();
    expect_action_acquired(
        ownership
            .try_acquire(deletion_action_claim(
                &release_check,
                "withdraw-release-check",
                false,
            ))
            .await
            .unwrap(),
    )
    .release()
    .await
    .unwrap();

    database.cleanup().await;
}

#[tokio::test]
async fn postgres_guarded_generation_verification_crash_reclaims_and_replays_without_deletion() {
    let Some(database) = TestDatabase::create().await else {
        return;
    };
    let lock = content_lock();
    let external = Arc::new(CrashExternalRepository::with_tombstone_and_resources(&lock));
    let state = deletion_app_state(database.pool().clone(), Arc::clone(&external));
    let deletions = PostgresContentLockDeletionRepository::new(database.pool().clone());
    let ownership = PostgresContentLockDeletionActionOwnership::new(database.pool().clone());
    let now = OffsetDateTime::now_utc();
    let job = ContentLockDeletionJob::new(uuid::Uuid::new_v4(), lock.clone(), now).unwrap();
    deletions.insert_job(job.clone()).await.unwrap();
    set_deletion_phase(database.pool(), job.job_id, "delete_content").await;

    let (stale, crash_guard) =
        claim_deletion_action(&deletions, &ownership, "verify-crashed", false).await;
    let primary = lock.primary_resource.as_ref().unwrap();
    assert_eq!(
        external
            .read_guarded_resource_generation(&job.creator, &primary.path, &primary.hash)
            .await
            .unwrap(),
        GuardedResourceReadback::Exact
    );
    drop(crash_guard);

    let fresh =
        expire_and_reclaim_deletion(database.pool(), &deletions, job.job_id, "verify-reclaimed")
            .await;
    assert!(matches!(
        deletions
            .advance_phase(
                job.job_id,
                "verify-crashed",
                stale.claim_token,
                ContentLockDeletionPhase::DeleteTombstone,
            )
            .await
            .unwrap(),
        locks_service::application::models::AdvanceContentLockDeletionPhaseResult::ClaimLost
    ));
    assert_eq!(
        RuntimeClaimedDeletionExecutor::new(state)
            .execute_claimed(fresh, "verify-reclaimed")
            .await
            .outcome,
        locks_service::application::use_cases::execute_content_lock_deletion_phase::DeletionPhaseExecutionOutcome::Progressed
    );
    assert_eq!(external.resource_delete_count(), 0);
    assert_eq!(external.resource_read_count(), 2);
    assert_eq!(
        deletions
            .get_job(&job.creator, &job.lock_id)
            .await
            .unwrap()
            .unwrap()
            .phase,
        ContentLockDeletionPhase::DeleteTombstone
    );

    let tombstone_crashed = deletions
        .claim_next("tombstone-crashed", time::Duration::seconds(1))
        .await
        .unwrap()
        .unwrap();
    let tombstone_crash_guard = expect_action_acquired(
        ownership
            .try_acquire(deletion_action_claim(
                &tombstone_crashed,
                "tombstone-crashed",
                false,
            ))
            .await
            .unwrap(),
    );
    let tombstone = ContentLockDeletionTombstone::new(job.lock_id.clone(), job.deletion_started_at);
    assert_eq!(
        external
            .read_tombstone(
                &job.creator,
                &ContentLockPath::from_lock_id(job.lock_id.clone()),
                &tombstone,
            )
            .await
            .unwrap(),
        TombstoneReadback::Exact
    );
    drop(tombstone_crash_guard);

    expire_deletion_claim(database.pool(), job.job_id).await;
    let recreated_deletions = PostgresContentLockDeletionRepository::new(database.pool().clone());
    let retained_tombstone_claim = recreated_deletions
        .claim_next("tombstone-reclaimed", time::Duration::minutes(1))
        .await
        .unwrap()
        .unwrap();
    assert_ne!(
        retained_tombstone_claim.claim_token,
        tombstone_crashed.claim_token
    );
    assert!(matches!(
        recreated_deletions
            .advance_phase(
                job.job_id,
                "tombstone-crashed",
                tombstone_crashed.claim_token,
                ContentLockDeletionPhase::PurgeOperationalState,
            )
            .await
            .unwrap(),
        locks_service::application::models::AdvanceContentLockDeletionPhaseResult::ClaimLost
    ));
    let recreated = deletion_app_state(database.pool().clone(), Arc::clone(&external));
    assert_eq!(
        RuntimeClaimedDeletionExecutor::new(recreated)
            .execute_claimed(retained_tombstone_claim, "tombstone-reclaimed")
            .await
            .outcome,
        locks_service::application::use_cases::execute_content_lock_deletion_phase::DeletionPhaseExecutionOutcome::Progressed
    );
    assert_eq!(external.tombstone_read_count(), 4);
    assert_eq!(external.resource_delete_count(), 0);
    assert_eq!(
        recreated_deletions
            .get_job(&job.creator, &job.lock_id)
            .await
            .unwrap()
            .unwrap()
            .phase,
        ContentLockDeletionPhase::PurgeOperationalState
    );
    let release_check = recreated_deletions
        .claim_next("tombstone-release-check", time::Duration::minutes(1))
        .await
        .unwrap()
        .unwrap();
    expect_action_acquired(
        ownership
            .try_acquire(deletion_action_claim(
                &release_check,
                "tombstone-release-check",
                false,
            ))
            .await
            .unwrap(),
    )
    .release()
    .await
    .unwrap();

    database.cleanup().await;
}

#[tokio::test]
async fn postgres_active_force_public_delete_crash_reclaims_before_private_cleanup() {
    let Some(database) = TestDatabase::create().await else {
        return;
    };
    let lock = content_lock();
    let external = Arc::new(CrashExternalRepository::with_original_and_resources(&lock));
    let deletions = PostgresContentLockDeletionRepository::new(database.pool().clone());
    let ownership = PostgresContentLockDeletionActionOwnership::new(database.pool().clone());
    let now = OffsetDateTime::now_utc();
    let job = ContentLockDeletionJob::new(uuid::Uuid::new_v4(), lock, now).unwrap();
    deletions.insert_job(job.clone()).await.unwrap();
    assert!(matches!(
        deletions
            .prepare_force_deletion(&job.creator, &job.lock_id)
            .await
            .unwrap(),
        PrepareForceDeletionResult::Active(_)
    ));

    let (stale, crash_guard) =
        claim_deletion_action(&deletions, &ownership, "public-crashed", true).await;
    external
        .force_delete_content_lock_and_verify_absent(
            &job.creator,
            &ContentLockPath::from_lock_id(job.lock_id.clone()),
        )
        .await
        .unwrap();
    assert_eq!(external.operations(), vec!["public"]);
    drop(crash_guard);

    expire_deletion_claim(database.pool(), job.job_id).await;
    let recreated_deletions = PostgresContentLockDeletionRepository::new(database.pool().clone());
    let fresh = recreated_deletions
        .claim_next("public-reclaimed", time::Duration::minutes(1))
        .await
        .unwrap()
        .unwrap();
    let fresh_action_claim = fresh.clone();
    assert_ne!(fresh.claim_token, stale.claim_token);
    assert!(
        !deletions
            .complete_force_deletion(job.job_id, "public-crashed", stale.claim_token,)
            .await
            .unwrap()
    );

    let recreated = deletion_app_state(database.pool().clone(), Arc::clone(&external));
    assert_eq!(
        RuntimeClaimedDeletionExecutor::new(recreated)
            .execute_claimed(fresh, "public-reclaimed")
            .await
            .outcome,
        locks_service::application::use_cases::execute_content_lock_deletion_phase::DeletionPhaseExecutionOutcome::Progressed
    );
    assert!(
        recreated_deletions
            .has_force_receipt(&job.creator, &job.lock_id)
            .await
            .unwrap()
    );
    assert!(
        recreated_deletions
            .get_job(&job.creator, &job.lock_id)
            .await
            .unwrap()
            .is_none()
    );
    assert_eq!(external.operations(), vec!["public", "public", "private"]);
    assert_eq!(external.resource_delete_count(), 1);
    assert!(matches!(
        ownership
            .try_acquire(deletion_action_claim(
                &fresh_action_claim,
                "public-reclaimed",
                true,
            ))
            .await
            .unwrap(),
        ContentLockDeletionActionAcquireResult::ClaimLost
    ));

    database.cleanup().await;
}

#[tokio::test]
async fn postgres_active_force_private_delete_crash_reclaims_to_terminal_receipt() {
    let Some(database) = TestDatabase::create().await else {
        return;
    };
    let lock = content_lock();
    let external = Arc::new(CrashExternalRepository::with_original_and_resources(&lock));
    let deletions = PostgresContentLockDeletionRepository::new(database.pool().clone());
    let ownership = PostgresContentLockDeletionActionOwnership::new(database.pool().clone());
    let now = OffsetDateTime::now_utc();
    let job = ContentLockDeletionJob::new(uuid::Uuid::new_v4(), lock.clone(), now).unwrap();
    deletions.insert_job(job.clone()).await.unwrap();
    assert!(matches!(
        deletions
            .prepare_force_deletion(&job.creator, &job.lock_id)
            .await
            .unwrap(),
        PrepareForceDeletionResult::Active(_)
    ));

    let (stale, crash_guard) =
        claim_deletion_action(&deletions, &ownership, "private-crashed", true).await;
    external
        .force_delete_content_lock_and_verify_absent(
            &job.creator,
            &ContentLockPath::from_lock_id(job.lock_id.clone()),
        )
        .await
        .unwrap();
    external
        .delete_guarded_resource(&job.creator, &lock.primary_resource.as_ref().unwrap().path)
        .await
        .unwrap();
    assert_eq!(external.operations(), vec!["public", "private"]);
    drop(crash_guard);

    expire_deletion_claim(database.pool(), job.job_id).await;
    let recreated_deletions = PostgresContentLockDeletionRepository::new(database.pool().clone());
    let fresh = recreated_deletions
        .claim_next("private-reclaimed", time::Duration::minutes(1))
        .await
        .unwrap()
        .unwrap();
    let fresh_action_claim = fresh.clone();
    assert_ne!(fresh.claim_token, stale.claim_token);
    assert!(
        !deletions
            .complete_force_deletion(job.job_id, "private-crashed", stale.claim_token,)
            .await
            .unwrap()
    );

    let recreated = deletion_app_state(database.pool().clone(), Arc::clone(&external));
    assert_eq!(
        RuntimeClaimedDeletionExecutor::new(recreated)
            .execute_claimed(fresh, "private-reclaimed")
            .await
            .outcome,
        locks_service::application::use_cases::execute_content_lock_deletion_phase::DeletionPhaseExecutionOutcome::Progressed
    );
    assert!(
        recreated_deletions
            .has_force_receipt(&job.creator, &job.lock_id)
            .await
            .unwrap()
    );
    assert!(
        recreated_deletions
            .get_job(&job.creator, &job.lock_id)
            .await
            .unwrap()
            .is_none()
    );
    assert!(
        recreated_deletions
            .claim_next("after-terminal", time::Duration::minutes(1))
            .await
            .unwrap()
            .is_none()
    );
    assert_eq!(
        external.operations(),
        vec!["public", "private", "public", "private"]
    );
    assert_eq!(external.resource_delete_count(), 2);
    assert!(matches!(
        ownership
            .try_acquire(deletion_action_claim(
                &fresh_action_claim,
                "private-reclaimed",
                true,
            ))
            .await
            .unwrap(),
        ContentLockDeletionActionAcquireResult::ClaimLost
    ));

    database.cleanup().await;
}

#[tokio::test]
async fn postgres_runtime_state_survives_app_state_recreation() {
    let Some(database) = TestDatabase::create().await else {
        return;
    };
    let content_lock = content_lock();
    let lock_id = content_lock.lock_id().unwrap();
    let guarded_paths = vec![content_lock.primary_resource.as_ref().unwrap().path.clone()];

    let first_state = app_state(database.pool().clone());
    first_state
        .content_lock_ownership()
        .reserve_paths(&creator(), &guarded_paths, &lock_id)
        .await
        .unwrap();
    first_state
        .content_lock_ownership()
        .mark_paths_published(&creator(), &guarded_paths, &lock_id)
        .await
        .unwrap();
    seed_content_lock(&first_state, content_lock.clone()).await;
    let first_router = router(first_state.clone());
    submit_task(&first_router, submitted_proof_bundle_for(&content_lock)).await;

    let recreated_state = app_state(database.pool().clone());
    let recreated_task = recreated_state
        .verification_tasks()
        .get_verification_task_by_handle(&creator(), &bundle_id())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(recreated_task.status, VerificationTaskStatus::Pending);
    let ownership = recreated_state
        .content_lock_ownership()
        .get_path_ownership(&creator(), &guarded_paths[0])
        .await
        .unwrap()
        .unwrap();
    assert_eq!(ownership.lock_id, lock_id);
    assert_eq!(ownership.status, ContentLockOwnershipStatus::Published);

    seed_content_lock(&recreated_state, content_lock.clone()).await;
    let worker = VerificationWorker::from_state(&recreated_state);
    assert_eq!(
        worker.run_once().await.unwrap(),
        WorkerTick::Completed(recreated_task.task_id)
    );

    let recreated_router = router(recreated_state.clone());
    let credential = issue_credential(&recreated_router).await;
    let credential_key = AccessCredentialLookupKey::derive(&AccessCredential::new(credential));

    let final_state = app_state(database.pool().clone());
    let stored_credential = final_state
        .access_credentials()
        .get_access_credential(&credential_key)
        .await
        .unwrap();
    assert!(stored_credential.is_some());

    database.cleanup().await;
}

#[tokio::test]
async fn manual_completion_hides_legacy_paykit_admission_without_authoritative_window() {
    let Some(database) = TestDatabase::create().await else {
        return;
    };
    let submitted = paykit_submission_for(&paykit_content_lock());
    let task = VerificationTaskRecord {
        task_id: TaskId::from_str(&uuid::Uuid::new_v4().to_string()).unwrap(),
        creator: submitted.pubky_lock_resource.creator().clone(),
        submitted_proof_bundle: submitted.clone(),
        status: VerificationTaskStatus::Pending,
        submitted_at: datetime!(2026-08-12 06:00:00 UTC),
        started_at: None,
        completed_at: None,
        failure_message: None,
    };
    PostgresVerificationTaskRepository::new(database.pool().clone())
        .insert_verification_task(task.clone())
        .await
        .unwrap();
    sqlx::query(
        "INSERT INTO paykit_task_admissions
             (verification_task_id, ready, ready_at)
         VALUES ($1::uuid, TRUE, now())",
    )
    .bind(task.task_id.to_string())
    .execute(database.pool())
    .await
    .unwrap();

    let response = router(app_state(database.pool().clone()))
        .oneshot(json_request(
            "POST",
            "/verification-task-completions",
            json!({
                "creator": submitted.pubky_lock_resource.creator(),
                "bundle_id": submitted.bundle_id,
            }),
        ))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
    assert_eq!(
        response_json(response).await,
        json!({
            "error": {
                "code": "verification_task_not_found",
                "message": "verification task not found"
            }
        })
    );

    database.cleanup().await;
}

#[tokio::test]
async fn postgres_runtime_readyz_returns_ready_without_leaking_runtime_details() {
    let Some(database) = TestDatabase::create().await else {
        return;
    };
    let state = app_state(database.pool().clone());
    state.record_worker_readiness(
        locks_server::app_state::WorkerKind::Verification,
        locks_server::app_state::WorkerReadinessEvidence::Ready,
    );
    state.record_worker_readiness(
        locks_server::app_state::WorkerKind::Deletion,
        locks_server::app_state::WorkerReadinessEvidence::Ready,
    );
    let response = router(state)
        .oneshot(empty_request("GET", "/readyz"))
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let body = response_json(response).await;
    assert_eq!(body["status"], "ready");
    assert_eq!(body["runtime_storage"], "persisted");
    assert_eq!(body["worker_enabled"], true);
    assert_eq!(body.as_object().unwrap().len(), 3);
    assert_no_keys(
        &body,
        &[
            "database_url",
            "lock_server_secret_key",
            "lock_server_public_key",
            "worker_id",
            "task_count",
            "secret_path",
            "credentials",
            "error",
            "task_id",
            "credential",
            "submitted_proof_bundle",
        ],
    );

    database.cleanup().await;
}

#[tokio::test]
async fn postgres_runtime_encrypts_creator_authority_secrets_at_rest() {
    let Some(database) = TestDatabase::create().await else {
        return;
    };
    let state = app_state(database.pool().clone());

    state
        .creator_authorities()
        .upsert_creator_authority(creator_authority_record("legacy-cookie-session-secret"))
        .await
        .unwrap();

    let stored_secret: String = sqlx::query_scalar("SELECT secret FROM creator_authorities")
        .fetch_one(database.pool())
        .await
        .unwrap();
    assert!(stored_secret.starts_with("v1.xchacha20poly1305:"));
    assert!(!stored_secret.contains("legacy-cookie-session-secret"));

    let loaded = state
        .creator_authorities()
        .get_creator_authority(&creator())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        loaded.secret.expose_secret(),
        "legacy-cookie-session-secret"
    );

    database.cleanup().await;
}

#[tokio::test]
async fn deletion_first_proof_submission_returns_409_without_calling_paykit() {
    let Some(database) = TestDatabase::create().await else {
        return;
    };
    let invoice_calls = Arc::new(AtomicUsize::new(0));
    let paykit_state = Arc::clone(&invoice_calls);
    let paykit_app = axum::Router::new().route(
        "/invoices",
        post(move || {
            let paykit_state = Arc::clone(&paykit_state);
            async move {
                paykit_state.fetch_add(1, Ordering::SeqCst);
                StatusCode::OK
            }
        }),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let paykit_url = format!("http://{}", listener.local_addr().unwrap());
    tokio::spawn(async move { axum::serve(listener, paykit_app).await.unwrap() });

    let temp_dir = tempfile::tempdir().unwrap();
    let secret_path = temp_dir.path().join("lock-server.keypair-seed");
    let public_key = FilesystemLockServerIdentityProvider
        .generate_secret(&secret_path)
        .unwrap();
    let mut config = test_config();
    config.credentials.lock_server_secret_key = secret_path;
    config.credentials.lock_server_public_key = public_key;
    config.paykit = Some(PaykitConfig {
        server_url: paykit_url,
        minimum_confirmations: 0,
    });
    let state = AppState::new_with_postgres_runtime_and_creator_repositories(
        config,
        database.pool().clone(),
        RuntimeSecretCiphers::new(
            CreatorAuthoritySecretCipher::new([7; 32]),
            FinalCredentialCipher::new([8; 32]),
        ),
        Arc::new(InMemoryContentLockRepository::new()),
        Arc::new(InMemoryContentLockTombstoneRepository::new()),
        Arc::new(InMemoryGuardedResourceRepository::new()),
        Arc::new(InMemoryLockServicePointerRepository::new()),
        Arc::new(InMemoryEntitlementRepository::new()),
    )
    .with_reader_pubky_resolver(Arc::new(AlwaysResolvesReader));
    let lock = paykit_content_lock();
    seed_content_lock(&state, lock.clone()).await;
    PostgresContentLockDeletionRepository::new(database.pool().clone())
        .insert_job(
            ContentLockDeletionJob::new(
                uuid::Uuid::new_v4(),
                lock.clone(),
                datetime!(2026-08-12 06:00:00 UTC),
            )
            .unwrap(),
        )
        .await
        .unwrap();

    let response = router(state)
        .oneshot(json_request(
            "POST",
            "/proof-bundles",
            json!({ "submitted_proof_bundle": paykit_submission_for(&lock) }),
        ))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::CONFLICT);
    assert_eq!(
        response_json(response).await,
        json!({
            "error": {
                "code": "content_lock_deletion_in_progress",
                "message": "content lock deletion is in progress"
            }
        })
    );
    assert_eq!(invoice_calls.load(Ordering::SeqCst), 0);

    database.cleanup().await;
}

#[tokio::test]
async fn snapshotted_unready_paykit_replay_ignores_tombstoned_lock_and_reader_resolution() {
    let Some(database) = TestDatabase::create().await else {
        return;
    };
    let invoice_calls = Arc::new(AtomicUsize::new(0));
    let paykit_state = Arc::clone(&invoice_calls);
    let paykit_app = axum::Router::new().route(
        "/invoices",
        post(move || {
            let call = paykit_state.fetch_add(1, Ordering::SeqCst);
            async move {
                if call == 0 {
                    (
                        StatusCode::INTERNAL_SERVER_ERROR,
                        axum::Json(json!({ "error": "injected" })),
                    )
                } else {
                    (
                        StatusCode::OK,
                        axum::Json(json!({
                            "invoice_created_at": "2026-08-12T10:00:00Z",
                            "payment_deadline": "2026-08-13T10:00:00Z",
                        })),
                    )
                }
            }
        }),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let paykit_url = format!("http://{}", listener.local_addr().unwrap());
    tokio::spawn(async move { axum::serve(listener, paykit_app).await.unwrap() });

    let temp_dir = tempfile::tempdir().unwrap();
    let secret_path = temp_dir.path().join("lock-server.keypair-seed");
    let public_key = FilesystemLockServerIdentityProvider
        .generate_secret(&secret_path)
        .unwrap();
    let mut config = test_config();
    config.credentials.lock_server_secret_key = secret_path;
    config.credentials.lock_server_public_key = public_key;
    config.paykit = Some(PaykitConfig {
        server_url: paykit_url,
        minimum_confirmations: 0,
    });
    let initial_state = AppState::new_with_postgres_runtime_and_creator_repositories(
        config.clone(),
        database.pool().clone(),
        RuntimeSecretCiphers::new(
            CreatorAuthoritySecretCipher::new([7; 32]),
            FinalCredentialCipher::new([8; 32]),
        ),
        Arc::new(InMemoryContentLockRepository::new()),
        Arc::new(InMemoryContentLockTombstoneRepository::new()),
        Arc::new(InMemoryGuardedResourceRepository::new()),
        Arc::new(InMemoryLockServicePointerRepository::new()),
        Arc::new(InMemoryEntitlementRepository::new()),
    )
    .with_reader_pubky_resolver(Arc::new(AlwaysResolvesReader));
    let lock = paykit_content_lock();
    let submitted = paykit_submission_for(&lock);
    seed_content_lock(&initial_state, lock.clone()).await;

    let first = router(initial_state)
        .oneshot(json_request(
            "POST",
            "/proof-bundles",
            json!({ "submitted_proof_bundle": submitted.clone() }),
        ))
        .await
        .unwrap();
    assert_eq!(first.status(), StatusCode::BAD_GATEWAY);
    PostgresContentLockDeletionRepository::new(database.pool().clone())
        .insert_job(
            ContentLockDeletionJob::new(
                uuid::Uuid::new_v4(),
                lock,
                datetime!(2026-08-12 06:00:00 UTC),
            )
            .unwrap(),
        )
        .await
        .unwrap();

    let tombstoned_state = AppState::new_with_postgres_runtime_and_creator_repositories(
        config,
        database.pool().clone(),
        RuntimeSecretCiphers::new(
            CreatorAuthoritySecretCipher::new([7; 32]),
            FinalCredentialCipher::new([8; 32]),
        ),
        Arc::new(InMemoryContentLockRepository::new()),
        Arc::new(InMemoryContentLockTombstoneRepository::new()),
        Arc::new(InMemoryGuardedResourceRepository::new()),
        Arc::new(InMemoryLockServicePointerRepository::new()),
        Arc::new(InMemoryEntitlementRepository::new()),
    )
    .with_reader_pubky_resolver(Arc::new(NeverResolvesReader));
    let replay_router = router(tombstoned_state);
    let replay = replay_router
        .clone()
        .oneshot(json_request(
            "POST",
            "/proof-bundles",
            json!({ "submitted_proof_bundle": submitted.clone() }),
        ))
        .await
        .unwrap();
    assert_eq!(replay.status(), StatusCode::OK);
    assert_eq!(response_json(replay).await["status"], "pending");
    assert_eq!(invoice_calls.load(Ordering::SeqCst), 2);

    let ready_replay = replay_router
        .clone()
        .oneshot(json_request(
            "POST",
            "/proof-bundles",
            json!({ "submitted_proof_bundle": submitted.clone() }),
        ))
        .await
        .unwrap();
    assert_eq!(ready_replay.status(), StatusCode::OK);
    assert_eq!(invoice_calls.load(Ordering::SeqCst), 2);

    let mut changed = submitted;
    changed.reader_public_key = Some(
        CreatorPubky::from_str("pubky7ir1ttte48bcp4zjychjyscicrwi1j34mtt91ptsafdbjmr8g9eo")
            .unwrap(),
    );
    let conflict = replay_router
        .oneshot(json_request(
            "POST",
            "/proof-bundles",
            json!({ "submitted_proof_bundle": changed }),
        ))
        .await
        .unwrap();
    assert_eq!(conflict.status(), StatusCode::CONFLICT);
    assert_eq!(invoice_calls.load(Ordering::SeqCst), 2);

    database.cleanup().await;
}

#[derive(Debug)]
struct CrashExternalRepository {
    // 0 = frozen original, 1 = exact tombstone, 2 = absent, 3 = replacement.
    public_state: AtomicUsize,
    tombstone_writes: AtomicUsize,
    withdraw_calls: AtomicUsize,
    tombstone_reads: AtomicUsize,
    resource_reads: AtomicUsize,
    resource_deletes: AtomicUsize,
    resources: Mutex<HashMap<String, GuardedResourceRecord>>,
    operations: Mutex<Vec<&'static str>>,
}

impl CrashExternalRepository {
    fn with_original(_lock: ContentLock) -> Self {
        Self::new(0, None)
    }

    fn with_tombstone_and_resources(lock: &ContentLock) -> Self {
        Self::new(1, Some(lock))
    }

    fn with_original_and_resources(lock: &ContentLock) -> Self {
        Self::new(0, Some(lock))
    }

    fn new(public_state: usize, lock: Option<&ContentLock>) -> Self {
        let mut resources = HashMap::new();
        if let Some(lock) = lock
            && let Some(primary) = &lock.primary_resource
        {
            resources.insert(
                primary.path.clone(),
                GuardedResourceRecord {
                    creator: lock.creator.clone(),
                    path: primary.path.clone(),
                    hash: primary.hash,
                    content_type: primary.content_type.clone(),
                    size: primary.size,
                    bytes: vec![7; primary.size as usize],
                },
            );
        }
        Self {
            public_state: AtomicUsize::new(public_state),
            tombstone_writes: AtomicUsize::new(0),
            withdraw_calls: AtomicUsize::new(0),
            tombstone_reads: AtomicUsize::new(0),
            resource_reads: AtomicUsize::new(0),
            resource_deletes: AtomicUsize::new(0),
            resources: Mutex::new(resources),
            operations: Mutex::new(Vec::new()),
        }
    }

    fn tombstone_write_count(&self) -> usize {
        self.tombstone_writes.load(Ordering::SeqCst)
    }

    fn withdraw_call_count(&self) -> usize {
        self.withdraw_calls.load(Ordering::SeqCst)
    }

    fn tombstone_read_count(&self) -> usize {
        self.tombstone_reads.load(Ordering::SeqCst)
    }

    fn resource_read_count(&self) -> usize {
        self.resource_reads.load(Ordering::SeqCst)
    }

    fn resource_delete_count(&self) -> usize {
        self.resource_deletes.load(Ordering::SeqCst)
    }

    fn operations(&self) -> Vec<&'static str> {
        self.operations.lock().unwrap().clone()
    }

    fn tombstone_readback(&self) -> TombstoneReadback {
        match self.public_state.load(Ordering::SeqCst) {
            1 => TombstoneReadback::Exact,
            2 => TombstoneReadback::Missing,
            _ => TombstoneReadback::Replaced,
        }
    }
}

#[async_trait]
impl ContentLockTombstoneRepository for CrashExternalRepository {
    async fn withdraw_content_lock(
        &self,
        _creator: CreatorPubky,
        _content_lock_path: ContentLockPath,
        _frozen_original: &ContentLock,
        _tombstone: &ContentLockDeletionTombstone,
    ) -> Result<TombstoneReadback, locks_service::application::errors::ApplicationError> {
        self.withdraw_calls.fetch_add(1, Ordering::SeqCst);
        if self
            .public_state
            .compare_exchange(0, 1, Ordering::SeqCst, Ordering::SeqCst)
            .is_ok()
        {
            self.tombstone_writes.fetch_add(1, Ordering::SeqCst);
        }
        Ok(self.tombstone_readback())
    }

    async fn read_tombstone(
        &self,
        _creator: &CreatorPubky,
        _content_lock_path: &ContentLockPath,
        _expected: &ContentLockDeletionTombstone,
    ) -> Result<TombstoneReadback, locks_service::application::errors::ApplicationError> {
        self.tombstone_reads.fetch_add(1, Ordering::SeqCst);
        Ok(self.tombstone_readback())
    }

    async fn force_delete_content_lock_and_verify_absent(
        &self,
        _creator: &CreatorPubky,
        _content_lock_path: &ContentLockPath,
    ) -> Result<(), locks_service::application::errors::ApplicationError> {
        self.operations.lock().unwrap().push("public");
        self.public_state.store(2, Ordering::SeqCst);
        Ok(())
    }
}

#[async_trait]
impl GuardedResourceRepository for CrashExternalRepository {
    async fn upsert_guarded_resource(
        &self,
        resource: GuardedResourceRecord,
    ) -> Result<(), locks_service::application::errors::ApplicationError> {
        self.resources
            .lock()
            .unwrap()
            .insert(resource.path.clone(), resource);
        Ok(())
    }

    async fn get_guarded_resource(
        &self,
        _creator: &CreatorPubky,
        path: &str,
        hash: &GuardedResourceHash,
    ) -> Result<Option<GuardedResourceRecord>, locks_service::application::errors::ApplicationError>
    {
        Ok(self
            .resources
            .lock()
            .unwrap()
            .get(path)
            .filter(|record| record.hash == *hash)
            .cloned())
    }

    async fn get_current_guarded_resource(
        &self,
        _creator: &CreatorPubky,
        path: &str,
    ) -> Result<Option<GuardedResourceRecord>, locks_service::application::errors::ApplicationError>
    {
        self.resource_reads.fetch_add(1, Ordering::SeqCst);
        Ok(self.resources.lock().unwrap().get(path).cloned())
    }

    async fn delete_guarded_resource(
        &self,
        _creator: &CreatorPubky,
        path: &str,
    ) -> Result<bool, locks_service::application::errors::ApplicationError> {
        self.operations.lock().unwrap().push("private");
        self.resource_deletes.fetch_add(1, Ordering::SeqCst);
        Ok(self.resources.lock().unwrap().remove(path).is_some())
    }
}

fn deletion_action_claim<'a>(
    claimed: &ClaimedContentLockDeletionJob,
    worker_id: &'a str,
    force: bool,
) -> ContentLockDeletionActionClaim<'a> {
    ContentLockDeletionActionClaim {
        job_id: claimed.job.job_id,
        worker_id,
        claim_token: claimed.claim_token,
        expected_phase: claimed.job.phase,
        force,
    }
}

fn expect_action_acquired(
    result: ContentLockDeletionActionAcquireResult,
) -> Box<dyn ContentLockDeletionActionGuard> {
    match result {
        ContentLockDeletionActionAcquireResult::Acquired(guard) => guard,
        ContentLockDeletionActionAcquireResult::Busy => {
            panic!("live deletion action claim was unexpectedly contended")
        }
        ContentLockDeletionActionAcquireResult::ClaimLost => {
            panic!("live deletion action claim was unexpectedly lost")
        }
    }
}

async fn claim_deletion_action(
    deletions: &PostgresContentLockDeletionRepository,
    ownership: &PostgresContentLockDeletionActionOwnership,
    worker_id: &str,
    force: bool,
) -> (
    ClaimedContentLockDeletionJob,
    Box<dyn ContentLockDeletionActionGuard>,
) {
    let claimed = deletions
        .claim_next(worker_id, time::Duration::seconds(1))
        .await
        .unwrap()
        .unwrap();
    let guard = expect_action_acquired(
        ownership
            .try_acquire(deletion_action_claim(&claimed, worker_id, force))
            .await
            .unwrap(),
    );
    (claimed, guard)
}

async fn set_deletion_phase(pool: &PgPool, job_id: uuid::Uuid, phase: &str) {
    sqlx::query("UPDATE content_lock_deletion_jobs SET phase = $2 WHERE job_id = $1")
        .bind(job_id)
        .bind(phase)
        .execute(pool)
        .await
        .unwrap();
}

async fn expire_deletion_claim(pool: &PgPool, job_id: uuid::Uuid) {
    sqlx::query(
        "UPDATE content_lock_deletion_jobs
         SET claim_expires_at = clock_timestamp() - interval '1 second'
         WHERE job_id = $1",
    )
    .bind(job_id)
    .execute(pool)
    .await
    .unwrap();
}

async fn expire_and_reclaim_deletion(
    pool: &PgPool,
    deletions: &PostgresContentLockDeletionRepository,
    job_id: uuid::Uuid,
    worker_id: &str,
) -> ClaimedContentLockDeletionJob {
    expire_deletion_claim(pool, job_id).await;
    deletions
        .claim_next(worker_id, time::Duration::minutes(1))
        .await
        .unwrap()
        .unwrap()
}

fn deletion_app_state(pool: PgPool, external: Arc<CrashExternalRepository>) -> AppState {
    AppState::new_with_postgres_runtime_and_creator_repositories(
        test_config(),
        pool,
        RuntimeSecretCiphers::new(
            CreatorAuthoritySecretCipher::new([7; 32]),
            FinalCredentialCipher::new([8; 32]),
        ),
        Arc::new(InMemoryContentLockRepository::new()),
        external.clone(),
        external,
        Arc::new(InMemoryLockServicePointerRepository::new()),
        Arc::new(InMemoryEntitlementRepository::new()),
    )
}

struct TestDatabase {
    pool: PgPool,
    schema_name: String,
    database_url: String,
}

impl TestDatabase {
    async fn create() -> Option<Self> {
        let Ok(database_url) = std::env::var("TEST_DATABASE_URL") else {
            eprintln!("skipping Postgres E2E test because TEST_DATABASE_URL is not set");
            return None;
        };
        let schema_name = format!("locks_e2e_{}", uuid::Uuid::new_v4().simple());
        let mut admin_connection = PgConnection::connect(&database_url)
            .await
            .expect("connect to TEST_DATABASE_URL");
        admin_connection
            .execute(format!("CREATE SCHEMA {schema_name}").as_str())
            .await
            .expect("create isolated schema");

        let search_path = schema_name.clone();
        let pool = PgPoolOptions::new()
            .max_connections(5)
            .after_connect(move |connection, _metadata| {
                let search_path = search_path.clone();
                Box::pin(async move {
                    connection
                        .execute(format!("SET search_path TO {search_path}").as_str())
                        .await?;
                    Ok(())
                })
            })
            .connect(&database_url)
            .await
            .expect("connect isolated schema pool");
        run_migrations(&pool)
            .await
            .expect("run migrations in isolated schema");

        Some(Self {
            pool,
            schema_name,
            database_url,
        })
    }

    fn pool(&self) -> &PgPool {
        &self.pool
    }

    async fn cleanup(self) {
        self.pool.close().await;
        let mut admin_connection = PgConnection::connect(&self.database_url)
            .await
            .expect("connect to TEST_DATABASE_URL for cleanup");
        admin_connection
            .execute(format!("DROP SCHEMA IF EXISTS {} CASCADE", self.schema_name).as_str())
            .await
            .expect("drop isolated schema");
    }
}

fn app_state(pool: PgPool) -> AppState {
    AppState::new_with_postgres_runtime_and_creator_repositories(
        test_config(),
        pool,
        RuntimeSecretCiphers::new(
            CreatorAuthoritySecretCipher::new([7; 32]),
            FinalCredentialCipher::new([8; 32]),
        ),
        std::sync::Arc::new(InMemoryContentLockRepository::new()),
        std::sync::Arc::new(InMemoryContentLockTombstoneRepository::new()),
        std::sync::Arc::new(InMemoryGuardedResourceRepository::new()),
        std::sync::Arc::new(InMemoryLockServicePointerRepository::new()),
        std::sync::Arc::new(InMemoryEntitlementRepository::new()),
    )
}

fn creator_authority_record(secret: &str) -> CreatorAuthorityRecord {
    CreatorAuthorityRecord {
        creator: creator(),
        auth_kind: CreatorAuthorityAuthKind::LegacyCookie,
        granted_scopes: vec![
            "/pub/locks.app/:rw".to_owned(),
            "/priv/locks.app/:rw".to_owned(),
        ],
        secret: CreatorAuthoritySecret::new(secret.to_owned()),
        session_expires_at: None,
        last_revalidated_at: Some(datetime!(2026-05-29 12:00:00 UTC)),
    }
}

fn test_config() -> LockServerRuntimeConfig {
    LockServerRuntimeConfig {
        bind_addr: "127.0.0.1:0".parse().unwrap(),
        credentials: LockServerCredentialsConfig {
            lock_server_secret_key: PathBuf::from("/tmp/lock-server-test-secret.sess"),
            lock_server_public_key: LockServerPubky::from_str(
                "pubky7ir1ttte48bcp4zjychjyscicrwi1j34mtt91ptsafdbjmr8g9eo",
            )
            .unwrap(),
            max_ttl_seconds: 900,
        },
        database: DatabaseConfig {
            url: "postgres://locks:locks@localhost/locks_test".to_owned(),
            max_connections: 10,
            run_migrations_on_startup: true,
        },
        deletion: DeletionConfig::default(),
        deletion_worker: locks_server::config::DeletionWorkerConfig::default(),
        worker: WorkerConfig {
            enabled: true,
            poll_interval_ms: 250,
            claim_timeout_seconds: 60,
            worker_id: "e2e-worker".to_owned(),
        },
        runtime: RuntimeConfig {
            environment: RuntimeEnvironment::Development,
        },
        creator_authority_acquisition: CreatorAuthorityAcquisitionConfig::default(),
        secrets: SecretsConfig::default(),
        rate_limits: RateLimitsConfig::default(),
        logging: LoggingConfig::default(),
        pubky: PubkyConfig::default(),
        pkdns: PkdnsConfig::default(),
        content_locks: ContentLocksConfig::default(),
        paykit: None,
    }
}

async fn seed_content_lock(state: &AppState, content_lock: ContentLock) {
    state
        .content_locks()
        .upsert_content_lock(
            content_lock.creator.clone(),
            content_lock.content_lock_path().unwrap(),
            content_lock,
        )
        .await
        .unwrap();
}

async fn submit_task(router: &axum::Router, bundle: SubmittedProofBundle) {
    let response = router
        .clone()
        .oneshot(json_request(
            "POST",
            "/proof-bundles",
            json!({ "submitted_proof_bundle": bundle }),
        ))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let json = response_json(response).await;
    assert_eq!(json["creator"], creator().to_string());
    assert_eq!(json["bundle_id"], BUNDLE_ID);
    assert_eq!(json["status"], "pending");
    assert!(json.get("task_id").is_none());
}

async fn issue_credential(router: &axum::Router) -> String {
    let response = router
        .clone()
        .oneshot(json_request(
            "POST",
            "/access-credentials",
            json!({ "creator": creator(), "bundle_id": BUNDLE_ID }),
        ))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    response_json(response).await["credential"]
        .as_str()
        .unwrap()
        .to_owned()
}

fn submitted_proof_bundle_for(content_lock: &ContentLock) -> SubmittedProofBundle {
    SubmittedProofBundle {
        version: SUBMITTED_PROOF_BUNDLE_VERSION,
        bundle_id: bundle_id(),
        pubky_lock_resource: PubkyLockResource::new(
            content_lock.creator.clone(),
            content_lock.content_lock_path().unwrap(),
        ),
        reader_public_key: None,
        proofs: vec![Proof {
            criterion_id: "criterion-1".to_owned(),
            verifier_type: VerifierType::DevStatic,
            payload: json!({ "e2e": true }),
        }],
    }
}

fn content_lock() -> ContentLock {
    ContentLock {
        version: CONTENT_LOCK_VERSION,
        creator: creator(),
        primary_resource: Some(GuardedResource {
            path: "/priv/locks.app/content/postgres-runtime.txt".to_owned(),
            hash: GuardedResourceHash::from_bytes([9; 32]),
            content_type: "text/plain".to_owned(),
            size: 22,
        }),
        secondary_resources: Default::default(),
        criteria: vec![Criterion {
            criterion_id: "criterion-1".to_owned(),
            verifier_type: VerifierType::DevStatic,
            params: json!({ "satisfied": true }),
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
        created_at: datetime!(2026-05-29 12:00:00 UTC),
    }
}

fn paykit_content_lock() -> ContentLock {
    let mut lock = content_lock();
    lock.criteria = vec![Criterion {
        criterion_id: "criterion-1".to_owned(),
        verifier_type: VerifierType::PaykitPayment,
        params: json!({
            "recipient_pubky": creator().to_string(),
            "amount": "50000",
            "asset": "BTC",
            "payment_in": 24
        }),
    }];
    lock
}

fn paykit_submission_for(content_lock: &ContentLock) -> SubmittedProofBundle {
    SubmittedProofBundle {
        version: SUBMITTED_PROOF_BUNDLE_VERSION,
        bundle_id: bundle_id(),
        pubky_lock_resource: PubkyLockResource::new(
            content_lock.creator.clone(),
            content_lock.content_lock_path().unwrap(),
        ),
        reader_public_key: Some(creator()),
        proofs: vec![Proof {
            criterion_id: "criterion-1".to_owned(),
            verifier_type: VerifierType::PaykitPayment,
            payload: json!({}),
        }],
    }
}

#[derive(Debug)]
struct AlwaysResolvesReader;

#[async_trait]
impl ReaderPubkyResolver for AlwaysResolvesReader {
    async fn reader_has_homeserver(&self, _reader: &CreatorPubky) -> bool {
        true
    }
}

#[derive(Debug)]
struct NeverResolvesReader;

#[async_trait]
impl ReaderPubkyResolver for NeverResolvesReader {
    async fn reader_has_homeserver(&self, _reader: &CreatorPubky) -> bool {
        false
    }
}

fn creator() -> CreatorPubky {
    CreatorPubky::from_str("pubkytkrq8zmwb8a3m9k15csu3q17qmfgqnp9dskbrg9uq1rydpyxp7qy").unwrap()
}

fn bundle_id() -> BundleId {
    BundleId::from_str(BUNDLE_ID).unwrap()
}

fn json_request(method: &str, uri: &str, body: Value) -> Request<Body> {
    let mut request = Request::builder()
        .method(method)
        .uri(uri)
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from(serde_json::to_vec(&body).unwrap()))
        .unwrap();
    insert_connect_info(&mut request);
    request
}

fn empty_request(method: &str, uri: &str) -> Request<Body> {
    let mut request = Request::builder()
        .method(method)
        .uri(uri)
        .body(Body::empty())
        .unwrap();
    insert_connect_info(&mut request);
    request
}

fn insert_connect_info(request: &mut Request<Body>) {
    request.extensions_mut().insert(ConnectInfo(SocketAddr::new(
        IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)),
        12345,
    )));
}

fn assert_no_keys(body: &Value, forbidden_keys: &[&str]) {
    for key in forbidden_keys {
        assert!(body.get(key).is_none(), "response leaked key {key}");
    }
}

async fn response_json(response: axum::response::Response) -> Value {
    serde_json::from_slice(&response_bytes(response).await).unwrap()
}

async fn response_bytes(response: axum::response::Response) -> Vec<u8> {
    to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap()
        .to_vec()
}
