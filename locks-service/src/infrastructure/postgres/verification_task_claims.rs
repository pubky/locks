use async_trait::async_trait;
use locks_core::ids::TaskId;
use sqlx::PgPool;

use crate::application::errors::ApplicationError;
use crate::application::models::{
    ClaimedVerificationTask, VerificationTaskRecord, VerificationTaskStatus,
    VerificationTerminalReason,
};
use crate::application::ports::VerificationTaskClaimer;
use crate::infrastructure::postgres::verification_tasks::{
    VERIFICATION_TASK_ROW_COLUMNS, VerificationTaskRow, row_to_task, status_to_database,
    verified_proof_bundle_to_json,
};

/// Postgres-backed worker lease claimer for verification tasks.
#[derive(Debug, Clone)]
pub struct PostgresVerificationTaskClaimer {
    pool: PgPool,
}

impl PostgresVerificationTaskClaimer {
    /// Creates a task claimer backed by the provided migrated Postgres pool.
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }
}

#[async_trait]
impl VerificationTaskClaimer for PostgresVerificationTaskClaimer {
    async fn claim_next_verification_task(
        &self,
        worker_id: &str,
        now: time::OffsetDateTime,
        claim_expires_at: time::OffsetDateTime,
    ) -> Result<Option<ClaimedVerificationTask>, ApplicationError> {
        let claim_token = uuid::Uuid::new_v4();
        let sql = format!(
            "UPDATE verification_tasks
            SET
                status = CASE WHEN status = 'pending' THEN 'in_progress' ELSE status END,
                claimed_by = $1,
                claim_expires_at = clock_timestamp() + ($2 - $3),
                claim_token = $4,
                next_attempt_at = NULL,
                started_at = COALESCE(started_at, $3),
                attempt_count = attempt_count + 1,
                updated_at = $3
            WHERE task_id = (
                SELECT task_id
                FROM verification_tasks
                WHERE ((status = 'pending'
                        AND (next_attempt_at IS NULL OR next_attempt_at <= clock_timestamp()))
                       OR (status IN ('in_progress', 'publishing_entitlement')
                           AND claim_expires_at <= clock_timestamp()))
                  AND creator = split_part(submitted_proof_bundle->>'pubky_lock_resource', '/', 1)
                  AND bundle_id = submitted_proof_bundle->>'bundle_id'
                  AND invoice_admission_phase = 'ready'
                ORDER BY submitted_at
                FOR UPDATE SKIP LOCKED
                LIMIT 1
            )
            RETURNING {VERIFICATION_TASK_ROW_COLUMNS}"
        );
        let row = sqlx::query_as::<_, VerificationTaskRow>(&sql)
            .bind(worker_id)
            .bind(claim_expires_at)
            .bind(now)
            .bind(claim_token)
            .fetch_optional(&self.pool)
            .await
            .map_err(storage_error)?;

        row.map(row_to_task)
            .transpose()
            .map(|task| task.map(|task| ClaimedVerificationTask { task, claim_token }))
    }

    async fn schedule_verification_task_retry(
        &self,
        task_id: &TaskId,
        worker_id: &str,
        claim_token: &uuid::Uuid,
        now: time::OffsetDateTime,
        next_attempt_at: time::OffsetDateTime,
    ) -> Result<Option<VerificationTaskRecord>, ApplicationError> {
        let sql = format!(
            "UPDATE verification_tasks
            SET
                status = 'pending',
                started_at = NULL,
                completed_at = NULL,
                failure_message = NULL,
                claimed_by = NULL,
                claim_token = NULL,
                claim_expires_at = NULL,
                next_attempt_at = clock_timestamp() + ($5 - $4),
                last_attempt_error = NULL,
                updated_at = $4
            WHERE task_id = $1::uuid
              AND status = 'in_progress'
              AND claimed_by = $2
              AND claim_token = $3
              AND claim_expires_at > clock_timestamp()
            RETURNING {VERIFICATION_TASK_ROW_COLUMNS}"
        );
        let row = sqlx::query_as::<_, VerificationTaskRow>(&sql)
            .bind(task_id.to_string())
            .bind(worker_id)
            .bind(claim_token)
            .bind(now)
            .bind(next_attempt_at)
            .fetch_optional(&self.pool)
            .await
            .map_err(storage_error)?;

        row.map(row_to_task).transpose()
    }

    async fn persist_claimed_verification_task_transition(
        &self,
        task: VerificationTaskRecord,
        worker_id: &str,
        claim_token: &uuid::Uuid,
        now: time::OffsetDateTime,
    ) -> Result<Option<VerificationTaskRecord>, ApplicationError> {
        if !matches!(
            task.status,
            VerificationTaskStatus::PublishingEntitlement
                | VerificationTaskStatus::Completed
                | VerificationTaskStatus::Failed
                | VerificationTaskStatus::Expired
        ) {
            return Err(ApplicationError::InvalidVerificationTaskState {
                message: "claimed task transition must publish or terminalize".to_owned(),
            });
        }
        let expected_status = match task.status {
            VerificationTaskStatus::PublishingEntitlement => "in_progress",
            VerificationTaskStatus::Completed => "publishing_entitlement",
            VerificationTaskStatus::Failed | VerificationTaskStatus::Expired => "in_progress",
            VerificationTaskStatus::Pending | VerificationTaskStatus::InProgress => unreachable!(),
        };
        let entitlement_to_publish = task
            .entitlement_to_publish
            .as_ref()
            .map(verified_proof_bundle_to_json)
            .transpose()?;
        let retain_claim = task.status == VerificationTaskStatus::PublishingEntitlement;
        let sql = format!(
            "UPDATE verification_tasks
             SET status = $5,
                 started_at = $6,
                 completed_at = $7,
                 failure_message = $8,
                 terminal_reason = $9,
                 entitlement_to_publish = $10,
                 claimed_by = CASE WHEN $11 THEN claimed_by ELSE NULL END,
                 claim_token = CASE WHEN $11 THEN claim_token ELSE NULL END,
                 claim_expires_at = CASE WHEN $11 THEN claim_expires_at ELSE NULL END,
                 next_attempt_at = NULL,
                 last_attempt_error = NULL,
                 updated_at = $4
             WHERE task_id = $1::uuid
               AND status = $12
               AND claimed_by = $2
               AND claim_token = $3
               AND claim_expires_at > clock_timestamp()
             RETURNING {VERIFICATION_TASK_ROW_COLUMNS}"
        );
        let row = sqlx::query_as::<_, VerificationTaskRow>(&sql)
            .bind(task.task_id.to_string())
            .bind(worker_id)
            .bind(claim_token)
            .bind(now)
            .bind(status_to_database(task.status))
            .bind(task.started_at)
            .bind(task.completed_at)
            .bind(task.failure_message)
            .bind(task.terminal_reason.map(VerificationTerminalReason::as_str))
            .bind(entitlement_to_publish)
            .bind(retain_claim)
            .bind(expected_status)
            .fetch_optional(&self.pool)
            .await
            .map_err(storage_error)?;

        row.map(row_to_task).transpose()
    }
}

fn storage_error(error: sqlx::Error) -> ApplicationError {
    ApplicationError::Storage {
        message: error.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use serde_json::json;
    use time::macros::datetime;

    use locks_core::ids::{BundleId, CreatorPubky, PubkyLockResource, TaskId};
    use locks_core::lock_policy::VerifierType;
    use locks_core::verification::{
        EntitlementLifetime, Proof, SUBMITTED_PROOF_BUNDLE_VERSION, SubmittedProofBundle,
        VERIFIED_PROOF_BUNDLE_VERSION, VerificationResult, VerifiedProofBundle,
    };

    use super::PostgresVerificationTaskClaimer;
    use crate::application::models::{
        INVOICE_ADMISSION_INTENT_VERSION, InvoiceAdmissionIntentV1, VerificationTaskRecord,
        VerificationTaskStatus, VerificationTerminalReason,
    };
    use crate::application::ports::{
        InvoiceAdmissionRepository, VerificationTaskClaimer, VerificationTaskRepository,
    };
    use crate::infrastructure::postgres::testing::TestDatabase;
    use crate::infrastructure::postgres::verification_tasks::PostgresVerificationTaskRepository;

    const LOCK_ID: &str = "000G40R40M30E209185GR38E1W8124GK2GAHC5RR34D1P70X3RFG";
    const BUNDLE_ID: &str = "000G40R40M30E209185GR38E1W";
    const BUNDLE_ID_2: &str = "000G40R40M30E209185GR38E1X";
    const BUNDLE_ID_3: &str = "000G40R40M30E209185GR38E1Y";
    const NOW: time::OffsetDateTime = datetime!(2026-05-29 12:10:00 UTC);
    const CLAIM_EXPIRES_AT: time::OffsetDateTime = datetime!(2026-05-29 12:15:00 UTC);

    #[tokio::test]
    async fn claims_oldest_pending_task_first() {
        let database = TestDatabase::create().await;
        let repository = PostgresVerificationTaskRepository::new(database.pool().clone());
        let claimer = PostgresVerificationTaskClaimer::new(database.pool().clone());
        let older = task(
            "018fc6ec-2f3d-4f7e-8b7d-6f5c4b3a2d10",
            VerificationTaskStatus::Pending,
            datetime!(2026-05-29 12:00:00 UTC),
        );
        let newer = task(
            "018fc6ec-2f3d-4f7e-8b7d-6f5c4b3a2d11",
            VerificationTaskStatus::Pending,
            datetime!(2026-05-29 12:01:00 UTC),
        );
        repository.insert_verification_task(newer).await.unwrap();
        repository
            .insert_verification_task(older.clone())
            .await
            .unwrap();

        let claimed = claimer
            .claim_next_verification_task("worker-a", NOW, CLAIM_EXPIRES_AT)
            .await
            .unwrap()
            .expect("oldest pending task is claimed");

        assert_eq!(claimed.task.task_id, older.task_id);
        assert_eq!(claimed.task.status, VerificationTaskStatus::InProgress);
        assert_eq!(claimed.task.started_at, Some(NOW));

        database.cleanup().await;
    }

    #[tokio::test]
    async fn invoice_pending_task_is_claimable_only_for_invoice_admission() {
        let database = TestDatabase::create().await;
        let repository = PostgresVerificationTaskRepository::new(database.pool().clone());
        let verification_claimer = PostgresVerificationTaskClaimer::new(database.pool().clone());
        let mut pending = task(
            "018fc6ec-2f3d-4f7e-8b7d-6f5c4b3a2d10",
            VerificationTaskStatus::Pending,
            datetime!(2026-05-29 12:00:00 UTC),
        );
        let reader =
            CreatorPubky::from_str("pubkyorhzqdiexwmi6iidktucgud63ufa5nwtsuzdxe176a8izd6jsqky")
                .unwrap();
        pending.submitted_proof_bundle.reader_public_key = Some(reader.clone());
        let intent = InvoiceAdmissionIntentV1 {
            version: INVOICE_ADMISSION_INTENT_VERSION,
            creator: pending.creator.clone(),
            bundle_id: pending.submitted_proof_bundle.bundle_id.clone(),
            lock_resource: pending.submitted_proof_bundle.pubky_lock_resource.clone(),
            reader,
        };
        repository
            .insert_invoice_pending_task(pending.clone(), intent)
            .await
            .unwrap();

        assert_eq!(
            verification_claimer
                .claim_next_verification_task("verification-worker", NOW, CLAIM_EXPIRES_AT)
                .await
                .unwrap(),
            None
        );
        let admission = repository
            .claim_next_invoice_admission("invoice-worker", NOW, time::Duration::minutes(1))
            .await
            .unwrap()
            .expect("invoice-pending task is eligible for admission work");
        assert_eq!(admission.admission.task.task_id, pending.task_id);

        database.cleanup().await;
    }

    #[tokio::test]
    async fn does_not_claim_terminal_tasks() {
        let database = TestDatabase::create().await;
        let repository = PostgresVerificationTaskRepository::new(database.pool().clone());
        let claimer = PostgresVerificationTaskClaimer::new(database.pool().clone());
        for record in [
            terminal_task(
                "018fc6ec-2f3d-4f7e-8b7d-6f5c4b3a2d10",
                VerificationTaskStatus::Completed,
            ),
            terminal_task(
                "018fc6ec-2f3d-4f7e-8b7d-6f5c4b3a2d11",
                VerificationTaskStatus::Failed,
            ),
            terminal_task(
                "018fc6ec-2f3d-4f7e-8b7d-6f5c4b3a2d12",
                VerificationTaskStatus::Expired,
            ),
        ] {
            repository.insert_verification_task(record).await.unwrap();
        }

        assert_eq!(
            claimer
                .claim_next_verification_task("worker-a", NOW, CLAIM_EXPIRES_AT)
                .await
                .unwrap(),
            None
        );

        database.cleanup().await;
    }

    #[tokio::test]
    async fn reclaims_expired_in_progress_task_without_resetting_started_at() {
        let database = TestDatabase::create().await;
        let repository = PostgresVerificationTaskRepository::new(database.pool().clone());
        let claimer = PostgresVerificationTaskClaimer::new(database.pool().clone());
        let started_at = datetime!(2026-05-29 12:00:00 UTC);
        let in_progress = task(
            "018fc6ec-2f3d-4f7e-8b7d-6f5c4b3a2d10",
            VerificationTaskStatus::Pending,
            started_at,
        )
        .transition_to(VerificationTaskStatus::InProgress, started_at, None)
        .unwrap();
        repository
            .insert_verification_task(in_progress.clone())
            .await
            .unwrap();
        mark_claim_expired(database.pool(), &in_progress.task_id).await;

        let reclaimed = claimer
            .claim_next_verification_task("worker-b", NOW, CLAIM_EXPIRES_AT)
            .await
            .unwrap()
            .expect("expired in-progress task is reclaimed");

        assert_eq!(reclaimed.task.task_id, in_progress.task_id);
        assert_eq!(reclaimed.task.status, VerificationTaskStatus::InProgress);
        assert_eq!(reclaimed.task.started_at, Some(started_at));

        database.cleanup().await;
    }

    #[tokio::test]
    async fn does_not_reclaim_non_expired_in_progress_task() {
        let database = TestDatabase::create().await;
        let repository = PostgresVerificationTaskRepository::new(database.pool().clone());
        let claimer = PostgresVerificationTaskClaimer::new(database.pool().clone());
        let started_at = datetime!(2026-05-29 12:00:00 UTC);
        let in_progress = task(
            "018fc6ec-2f3d-4f7e-8b7d-6f5c4b3a2d10",
            VerificationTaskStatus::Pending,
            started_at,
        )
        .transition_to(VerificationTaskStatus::InProgress, started_at, None)
        .unwrap();
        repository
            .insert_verification_task(in_progress.clone())
            .await
            .unwrap();
        mark_claim_active(database.pool(), &in_progress.task_id).await;

        assert_eq!(
            claimer
                .claim_next_verification_task("worker-b", NOW, CLAIM_EXPIRES_AT)
                .await
                .unwrap(),
            None
        );

        database.cleanup().await;
    }

    #[tokio::test]
    async fn concurrent_claim_attempts_do_not_return_same_task_twice() {
        let database = TestDatabase::create().await;
        let repository = PostgresVerificationTaskRepository::new(database.pool().clone());
        let claimer_a = PostgresVerificationTaskClaimer::new(database.pool().clone());
        let claimer_b = PostgresVerificationTaskClaimer::new(database.pool().clone());
        let pending = task(
            "018fc6ec-2f3d-4f7e-8b7d-6f5c4b3a2d10",
            VerificationTaskStatus::Pending,
            datetime!(2026-05-29 12:00:00 UTC),
        );
        repository
            .insert_verification_task(pending.clone())
            .await
            .unwrap();

        let (claim_a, claim_b) = tokio::join!(
            claimer_a.claim_next_verification_task("worker-a", NOW, CLAIM_EXPIRES_AT),
            claimer_b.claim_next_verification_task("worker-b", NOW, CLAIM_EXPIRES_AT),
        );
        let claimed = [claim_a.unwrap(), claim_b.unwrap()];

        assert_eq!(claimed.iter().filter(|claim| claim.is_some()).count(), 1);
        assert_eq!(
            claimed
                .iter()
                .flatten()
                .next()
                .map(|claim| claim.task.task_id),
            Some(pending.task_id)
        );

        database.cleanup().await;
    }

    #[tokio::test]
    async fn stale_claim_token_cannot_reschedule_after_same_worker_id_reclaims() {
        let database = TestDatabase::create().await;
        let repository = PostgresVerificationTaskRepository::new(database.pool().clone());
        let claimer = PostgresVerificationTaskClaimer::new(database.pool().clone());
        let pending = task(
            "018fc6ec-2f3d-4f7e-8b7d-6f5c4b3a2d10",
            VerificationTaskStatus::Pending,
            datetime!(2026-05-29 12:00:00 UTC),
        );
        repository
            .insert_verification_task(pending.clone())
            .await
            .unwrap();
        let first = claimer
            .claim_next_verification_task("worker-a", NOW, CLAIM_EXPIRES_AT)
            .await
            .unwrap()
            .unwrap();
        mark_claim_expired(database.pool(), &pending.task_id).await;
        let reclaimed_at = CLAIM_EXPIRES_AT + time::Duration::milliseconds(1);
        let second = claimer
            .claim_next_verification_task(
                "worker-a",
                reclaimed_at,
                reclaimed_at + time::Duration::minutes(5),
            )
            .await
            .unwrap()
            .unwrap();

        assert_ne!(first.claim_token, second.claim_token);
        assert_eq!(
            claimer
                .schedule_verification_task_retry(
                    &pending.task_id,
                    "worker-a",
                    &first.claim_token,
                    reclaimed_at,
                    reclaimed_at + time::Duration::seconds(10),
                )
                .await
                .unwrap(),
            None
        );
        assert!(
            claimer
                .schedule_verification_task_retry(
                    &pending.task_id,
                    "worker-a",
                    &second.claim_token,
                    reclaimed_at,
                    reclaimed_at + time::Duration::seconds(10),
                )
                .await
                .unwrap()
                .is_some()
        );

        database.cleanup().await;
    }

    #[tokio::test]
    async fn stale_claim_token_cannot_persist_terminal_state_after_same_worker_id_reclaims() {
        let database = TestDatabase::create().await;
        let repository = PostgresVerificationTaskRepository::new(database.pool().clone());
        let claimer = PostgresVerificationTaskClaimer::new(database.pool().clone());
        let pending = task(
            "018fc6ec-2f3d-4f7e-8b7d-6f5c4b3a2d10",
            VerificationTaskStatus::Pending,
            datetime!(2026-05-29 12:00:00 UTC),
        );
        repository
            .insert_verification_task(pending.clone())
            .await
            .unwrap();
        let first = claimer
            .claim_next_verification_task("worker-a", NOW, CLAIM_EXPIRES_AT)
            .await
            .unwrap()
            .unwrap();
        mark_claim_expired(database.pool(), &pending.task_id).await;
        let reclaimed_at = CLAIM_EXPIRES_AT + time::Duration::milliseconds(1);
        let second = claimer
            .claim_next_verification_task(
                "worker-a",
                reclaimed_at,
                reclaimed_at + time::Duration::minutes(5),
            )
            .await
            .unwrap()
            .unwrap();
        let publishing = second
            .task
            .clone()
            .begin_entitlement_publication(entitlement_for(&second.task))
            .unwrap();
        let completed = publishing
            .complete_entitlement_publication(reclaimed_at + time::Duration::seconds(1))
            .unwrap();
        let failed = second
            .task
            .clone()
            .transition_to(
                VerificationTaskStatus::Failed,
                reclaimed_at + time::Duration::seconds(1),
                Some("stale failure".to_owned()),
            )
            .unwrap();
        let expired = second
            .task
            .clone()
            .expire(
                VerificationTerminalReason::PaymentRequestRejected,
                reclaimed_at + time::Duration::seconds(1),
            )
            .unwrap();

        for stale_transition in [publishing.clone(), completed.clone(), failed, expired] {
            assert_eq!(
                claimer
                    .persist_claimed_verification_task_transition(
                        stale_transition,
                        "worker-a",
                        &first.claim_token,
                        reclaimed_at,
                    )
                    .await
                    .unwrap(),
                None
            );
        }
        assert_eq!(
            claimer
                .persist_claimed_verification_task_transition(
                    publishing,
                    "worker-a",
                    &second.claim_token,
                    reclaimed_at,
                )
                .await
                .unwrap()
                .map(|task| task.status),
            Some(VerificationTaskStatus::PublishingEntitlement)
        );
        assert_eq!(
            claimer
                .persist_claimed_verification_task_transition(
                    completed.clone(),
                    "worker-a",
                    &second.claim_token,
                    reclaimed_at,
                )
                .await
                .unwrap(),
            Some(completed)
        );

        database.cleanup().await;
    }

    #[tokio::test]
    async fn retry_schedule_is_due_time_gated_owner_fenced_and_preserves_attempt_count() {
        let database = TestDatabase::create().await;
        let repository = PostgresVerificationTaskRepository::new(database.pool().clone());
        let claimer = PostgresVerificationTaskClaimer::new(database.pool().clone());
        let pending = task(
            "018fc6ec-2f3d-4f7e-8b7d-6f5c4b3a2d10",
            VerificationTaskStatus::Pending,
            datetime!(2026-05-29 12:00:00 UTC),
        );
        repository
            .insert_verification_task(pending.clone())
            .await
            .unwrap();
        let claim = claimer
            .claim_next_verification_task("worker-a", NOW, CLAIM_EXPIRES_AT)
            .await
            .unwrap()
            .expect("pending task is claimed");
        let next_attempt_at = NOW + time::Duration::seconds(10);

        assert_eq!(
            claimer
                .schedule_verification_task_retry(
                    &pending.task_id,
                    "worker-b",
                    &claim.claim_token,
                    NOW,
                    next_attempt_at,
                )
                .await
                .unwrap(),
            None
        );
        mark_claim_expired(database.pool(), &pending.task_id).await;
        assert_eq!(
            claimer
                .schedule_verification_task_retry(
                    &pending.task_id,
                    "worker-a",
                    &claim.claim_token,
                    CLAIM_EXPIRES_AT + time::Duration::milliseconds(1),
                    next_attempt_at,
                )
                .await
                .unwrap(),
            None
        );
        mark_claim_active(database.pool(), &pending.task_id).await;
        let scheduled = claimer
            .schedule_verification_task_retry(
                &pending.task_id,
                "worker-a",
                &claim.claim_token,
                NOW,
                next_attempt_at,
            )
            .await
            .unwrap()
            .expect("owning worker schedules retry");
        assert_eq!(scheduled.status, VerificationTaskStatus::Pending);
        assert_eq!(scheduled.started_at, None);
        assert_eq!(scheduled.failure_message, None);
        assert_eq!(attempt_count(database.pool(), &pending.task_id).await, 1);

        assert_eq!(
            claimer
                .claim_next_verification_task(
                    "worker-b",
                    next_attempt_at - time::Duration::milliseconds(1),
                    CLAIM_EXPIRES_AT,
                )
                .await
                .unwrap(),
            None
        );
        sqlx::query(
            "UPDATE verification_tasks
             SET next_attempt_at = clock_timestamp() - INTERVAL '1 millisecond'
             WHERE task_id = $1::uuid",
        )
        .bind(pending.task_id.to_string())
        .execute(database.pool())
        .await
        .unwrap();
        assert!(
            claimer
                .claim_next_verification_task(
                    "worker-b",
                    next_attempt_at,
                    CLAIM_EXPIRES_AT + time::Duration::seconds(10),
                )
                .await
                .unwrap()
                .is_some()
        );
        assert_eq!(attempt_count(database.pool(), &pending.task_id).await, 2);

        database.cleanup().await;
    }

    async fn attempt_count(pool: &sqlx::PgPool, task_id: &TaskId) -> i32 {
        sqlx::query_scalar("SELECT attempt_count FROM verification_tasks WHERE task_id = $1::uuid")
            .bind(task_id.to_string())
            .fetch_one(pool)
            .await
            .expect("load attempt count")
    }

    async fn mark_claim_expired(pool: &sqlx::PgPool, task_id: &TaskId) {
        sqlx::query(
            "UPDATE verification_tasks
            SET claimed_by = 'worker-a',
                claim_token = COALESCE(claim_token, $2),
                claim_expires_at = clock_timestamp() - INTERVAL '1 millisecond'
            WHERE task_id = $1::uuid",
        )
        .bind(task_id.to_string())
        .bind(uuid::Uuid::new_v4())
        .execute(pool)
        .await
        .expect("mark claim expired");
    }

    async fn mark_claim_active(pool: &sqlx::PgPool, task_id: &TaskId) {
        sqlx::query(
            "UPDATE verification_tasks
            SET claimed_by = 'worker-a',
                claim_token = COALESCE(claim_token, $2),
                claim_expires_at = clock_timestamp() + INTERVAL '5 minutes'
            WHERE task_id = $1::uuid",
        )
        .bind(task_id.to_string())
        .bind(uuid::Uuid::new_v4())
        .execute(pool)
        .await
        .expect("mark claim active");
    }

    fn terminal_task(task_id: &str, status: VerificationTaskStatus) -> VerificationTaskRecord {
        let started_at = datetime!(2026-05-29 12:00:00 UTC);
        let in_progress = task(task_id, VerificationTaskStatus::Pending, started_at)
            .transition_to(VerificationTaskStatus::InProgress, started_at, None)
            .unwrap();
        match status {
            VerificationTaskStatus::Completed => in_progress
                .begin_entitlement_publication(entitlement_for(&in_progress))
                .unwrap()
                .complete_entitlement_publication(datetime!(2026-05-29 12:01:00 UTC))
                .unwrap(),
            VerificationTaskStatus::Failed => in_progress
                .transition_to(
                    VerificationTaskStatus::Failed,
                    datetime!(2026-05-29 12:01:00 UTC),
                    Some("failed".to_owned()),
                )
                .unwrap(),
            VerificationTaskStatus::Expired => in_progress
                .expire(
                    VerificationTerminalReason::PaymentRequestRejected,
                    datetime!(2026-05-29 12:01:00 UTC),
                )
                .unwrap(),
            VerificationTaskStatus::Pending
            | VerificationTaskStatus::InProgress
            | VerificationTaskStatus::PublishingEntitlement => unreachable!(),
        }
    }

    fn task(
        task_id: &str,
        status: VerificationTaskStatus,
        submitted_at: time::OffsetDateTime,
    ) -> VerificationTaskRecord {
        VerificationTaskRecord {
            task_id: TaskId::from_str(task_id).unwrap(),
            creator: CreatorPubky::from_str(creator_for_task_id(task_id)).unwrap(),
            submitted_proof_bundle: SubmittedProofBundle {
                version: SUBMITTED_PROOF_BUNDLE_VERSION,
                bundle_id: BundleId::from_str(bundle_id_for_task_id(task_id)).unwrap(),
                pubky_lock_resource: PubkyLockResource::from_str(&format!(
                    "{}/pub/app.locks/{LOCK_ID}.json",
                    creator_for_task_id(task_id)
                ))
                .unwrap(),
                reader_public_key: None,
                proofs: vec![Proof {
                    criterion_id: "criterion-1".to_owned(),
                    verifier_type: VerifierType::DevStatic,
                    payload: json!({ "satisfied": true }),
                }],
            },
            status,
            submitted_at,
            started_at: None,
            completed_at: None,
            failure_message: None,
            terminal_reason: None,
            entitlement_to_publish: None,
        }
    }

    fn entitlement_for(task: &VerificationTaskRecord) -> VerifiedProofBundle {
        VerifiedProofBundle {
            version: VERIFIED_PROOF_BUNDLE_VERSION,
            bundle_id: task.submitted_proof_bundle.bundle_id.clone(),
            pubky_lock_resource: task.submitted_proof_bundle.pubky_lock_resource.clone(),
            verification_result: VerificationResult { criteria: vec![] },
            entitlement_lifetime: EntitlementLifetime::Unbounded,
        }
    }

    fn bundle_id_for_task_id(task_id: &str) -> &'static str {
        match task_id.chars().last() {
            Some('1') => BUNDLE_ID_2,
            Some('2') => BUNDLE_ID_3,
            _ => BUNDLE_ID,
        }
    }

    fn creator_for_task_id(task_id: &str) -> &'static str {
        match task_id.chars().last() {
            Some('1') => "pubkyorhzqdiexwmi6iidktucgud63ufa5nwtsuzdxe176a8izd6jsqky",
            Some('2') => "pubky7ir1ttte48bcp4zjychjyscicrwi1j34mtt91ptsafdbjmr8g9eo",
            _ => "pubkytkrq8zmwb8a3m9k15csu3q17qmfgqnp9dskbrg9uq1rydpyxp7qy",
        }
    }
}
