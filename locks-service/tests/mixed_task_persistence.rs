use std::str::FromStr;

use locks_core::ids::{BundleId, CreatorPubky, LockHash, LockId, PubkyLockResource, TaskId};
use locks_core::lock_policy::{
    GraphParticipantV2, GraphPolicyV2, GraphPredicateV2, PaymentPolicyV2,
};
use locks_core::verification::{SUBMITTED_PROOF_BUNDLE_V2_VERSION, SubmittedProofBundleV2};
use locks_service::application::errors::ApplicationError;
use locks_service::application::models::{
    MixedTaskRecord, MixedTaskSnapshot, MixedTaskSnapshotV1, VerificationTaskStatus,
};
use locks_service::application::ports::{MixedTaskClaimer, MixedTaskRepository};
use locks_service::infrastructure::memory::mixed_tasks::InMemoryMixedTaskRepository;
use locks_service::infrastructure::postgres::{PostgresMixedTaskRepository, run_migrations};
use sqlx::postgres::PgPoolOptions;
use sqlx::{Connection, Executor, PgConnection};
use time::macros::datetime;

const CREATOR: &str = "pubkytkrq8zmwb8a3m9k15csu3q17qmfgqnp9dskbrg9uq1rydpyxp7qy";
const READER: &str = "pubkyorhzqdiexwmi6iidktucgud63ufa5nwtsuzdxe176a8izd6jsqky";
const NOW: time::OffsetDateTime = datetime!(2026-11-05 12:00:00 UTC);

#[tokio::test]
async fn memory_mixed_replay_conflict_claim_and_revision_fences_are_closed() {
    let repository = InMemoryMixedTaskRepository::new();
    let task = task([1; 16]);
    let inserted = repository.insert_mixed_task(task.clone()).await.unwrap();
    assert_eq!(inserted, task);
    assert_eq!(
        repository.insert_mixed_task(task.clone()).await.unwrap(),
        task
    );

    let mut changed = task.clone();
    changed.snapshot.as_v1_mut().payment.payment.amount = "50001".to_owned();
    changed.snapshot.as_v1_mut().payment.payment.amount_sats = 50001;
    assert!(matches!(
        repository.insert_mixed_task(changed).await,
        Err(ApplicationError::VerificationTaskConflict)
    ));

    let claim = repository
        .claim_next_mixed_task("worker", NOW, NOW + time::Duration::seconds(30))
        .await
        .unwrap()
        .unwrap();
    let scheduled = repository
        .schedule_mixed_task_retry(
            &task.task_id,
            "worker",
            &claim.claim_token,
            NOW,
            0,
            task.snapshot.clone(),
            NOW + time::Duration::seconds(1),
        )
        .await
        .unwrap()
        .unwrap();
    assert_eq!(scheduled.evaluator_revision, 1);
    assert!(
        repository
            .schedule_mixed_task_retry(
                &task.task_id,
                "worker",
                &claim.claim_token,
                NOW,
                0,
                task.snapshot,
                NOW + time::Duration::seconds(2),
            )
            .await
            .unwrap()
            .is_none()
    );
}

#[tokio::test]
async fn postgres_mixed_replay_claim_and_restart_preserve_exact_snapshot() {
    let database_url = std::env::var("TEST_DATABASE_URL")
        .expect("TEST_DATABASE_URL must be set for explicit Postgres tests");
    let schema = format!("locks_mixed_test_{}", uuid::Uuid::new_v4().simple());
    let mut admin = PgConnection::connect(&database_url).await.unwrap();
    admin
        .execute(format!("CREATE SCHEMA {schema}").as_str())
        .await
        .unwrap();
    let schema_for_pool = schema.clone();
    let pool = PgPoolOptions::new()
        .max_connections(5)
        .after_connect(move |connection, _| {
            let schema = schema_for_pool.clone();
            Box::pin(async move {
                connection
                    .execute(format!("SET search_path TO {schema}").as_str())
                    .await?;
                Ok(())
            })
        })
        .connect(&database_url)
        .await
        .unwrap();
    run_migrations(&pool).await.unwrap();

    let repository = PostgresMixedTaskRepository::new(pool.clone());
    let task = task([2; 16]);
    repository.insert_mixed_task(task.clone()).await.unwrap();
    assert_eq!(
        repository.insert_mixed_task(task.clone()).await.unwrap(),
        task
    );

    let restarted = PostgresMixedTaskRepository::new(pool.clone());
    assert_eq!(
        restarted
            .get_mixed_task_by_handle(
                &task.snapshot.as_v1().creator,
                &task.snapshot.as_v1().bundle_id,
            )
            .await
            .unwrap(),
        Some(task.clone())
    );
    let now = time::OffsetDateTime::now_utc();
    let claim = restarted
        .claim_next_mixed_task("worker", now, now + time::Duration::seconds(30))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(claim.task.task_id, task.task_id);

    pool.close().await;
    admin
        .execute(format!("DROP SCHEMA {schema} CASCADE").as_str())
        .await
        .unwrap();
}

fn task(bundle: [u8; 16]) -> MixedTaskRecord {
    let creator = CreatorPubky::from_str(CREATOR).unwrap();
    let lock_id = LockId::from_hash(LockHash::from_bytes([0; 32]));
    let resource =
        PubkyLockResource::from_str(&format!("{creator}/pub/app.locks/{lock_id}.json")).unwrap();
    let snapshot = MixedTaskSnapshotV1::new(
        SubmittedProofBundleV2 {
            version: SUBMITTED_PROOF_BUNDLE_V2_VERSION,
            bundle_id: BundleId::from_bytes(bundle),
            pubky_lock_resource: resource,
            reader_pubky: None,
        },
        [0; 32],
        CreatorPubky::from_str(READER).unwrap(),
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
        task_id: TaskId::from_str(&uuid::Uuid::new_v4().to_string()).unwrap(),
        snapshot: MixedTaskSnapshot::V1(snapshot),
        evaluator_revision: 0,
        status: VerificationTaskStatus::Pending,
        terminal_reason: None,
        entitlement_to_publish: None,
        entitlement_publication_token: None,
        completed_at: None,
    }
}
