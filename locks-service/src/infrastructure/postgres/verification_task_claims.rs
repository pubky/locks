use async_trait::async_trait;
use locks_core::ids::TaskId;
use sqlx::PgPool;

use crate::application::errors::ApplicationError;
use crate::application::models::{
    ClaimedVerificationTask, VerificationTaskRecord, VerificationTaskStatus,
};
use crate::application::ports::VerificationTaskClaimer;
use crate::infrastructure::postgres::verification_tasks::{
    VERIFICATION_TASK_ROW_COLUMNS, VerificationTaskRow, row_to_task, status_to_database,
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
    async fn begin_claimed_entitlement_publication(
        &self,
        task_id: &TaskId,
        worker_id: &str,
        claim_token: &uuid::Uuid,
    ) -> Result<bool, ApplicationError> {
        let mut transaction = self.pool.begin().await.map_err(storage_error)?;
        sqlx::query("SELECT task_id FROM verification_tasks WHERE task_id = $1::uuid FOR UPDATE")
            .bind(task_id.to_string())
            .fetch_optional(&mut *transaction)
            .await
            .map_err(storage_error)?;
        let now: time::OffsetDateTime = sqlx::query_scalar("SELECT clock_timestamp()")
            .fetch_one(&mut *transaction)
            .await
            .map_err(storage_error)?;
        let updated = sqlx::query(
            "UPDATE verification_tasks
             SET entitlement_publication_claim_token = $3, updated_at = $4
             WHERE task_id = $1::uuid AND status = 'in_progress'
               AND claimed_by = $2 AND claim_token = $3 AND claim_expires_at > $4
               AND deletion_job_id IS NULL
               AND NOT EXISTS (
                   SELECT 1 FROM content_lock_deletion_task_snapshot AS snapshot
                   WHERE snapshot.verification_task_id = verification_tasks.task_id
               )",
        )
        .bind(task_id.to_string())
        .bind(worker_id)
        .bind(claim_token)
        .bind(now)
        .execute(&mut *transaction)
        .await
        .map_err(storage_error)?;
        transaction.commit().await.map_err(storage_error)?;
        Ok(updated.rows_affected() == 1)
    }

    async fn claim_next_verification_task(
        &self,
        worker_id: &str,
        claim_ttl: time::Duration,
    ) -> Result<Option<ClaimedVerificationTask>, ApplicationError> {
        let claim_token = uuid::Uuid::new_v4();
        let mut transaction = self.pool.begin().await.map_err(storage_error)?;
        let candidates: Vec<(
            uuid::Uuid,
            String,
            Option<time::OffsetDateTime>,
            Option<time::OffsetDateTime>,
        )> = sqlx::query_as(
            "SELECT task_id, status, next_attempt_at, claim_expires_at
                 FROM verification_tasks
                 WHERE status IN ('pending', 'in_progress')
                   AND deletion_job_id IS NULL
                   AND NOT EXISTS (
                       SELECT 1 FROM paykit_task_admissions
                       WHERE verification_task_id = verification_tasks.task_id
                         AND (ready = FALSE OR payment_in_hours IS NULL OR payment_in_hours <= 0
                              OR invoice_created_at IS NULL OR payment_deadline IS NULL
                              OR invoice_created_at > payment_deadline)
                   )
                   AND NOT EXISTS (
                       SELECT 1 FROM content_lock_deletion_task_snapshot AS snapshot
                       WHERE snapshot.verification_task_id = verification_tasks.task_id
                   )
                   AND creator = split_part(submitted_proof_bundle->>'pubky_lock_resource', '/', 1)
                   AND bundle_id = submitted_proof_bundle->>'bundle_id'
                 ORDER BY submitted_at
                 FOR UPDATE SKIP LOCKED",
        )
        .fetch_all(&mut *transaction)
        .await
        .map_err(storage_error)?;
        let now: time::OffsetDateTime = sqlx::query_scalar("SELECT clock_timestamp()")
            .fetch_one(&mut *transaction)
            .await
            .map_err(storage_error)?;
        let claim_expires_at =
            now.checked_add(claim_ttl)
                .ok_or_else(|| ApplicationError::Storage {
                    message: "verification task claim expiry overflow".to_owned(),
                })?;
        let Some((task_id, _, _, _)) = candidates.into_iter().find(|(_, status, next, expiry)| {
            (status == "pending" && next.is_none_or(|due| due <= now))
                || (status == "in_progress" && expiry.is_some_and(|deadline| deadline <= now))
        }) else {
            transaction.commit().await.map_err(storage_error)?;
            return Ok(None);
        };
        let sql = format!(
            "UPDATE verification_tasks
             SET status = 'in_progress', claimed_by = $2, claim_expires_at = $3,
                 claim_token = $4, next_attempt_at = NULL,
                 started_at = COALESCE(started_at, $5), attempt_count = attempt_count + 1,
                 updated_at = $5
             WHERE task_id = $1
             RETURNING {VERIFICATION_TASK_ROW_COLUMNS}"
        );
        let row = sqlx::query_as::<_, VerificationTaskRow>(&sql)
            .bind(task_id)
            .bind(worker_id)
            .bind(claim_expires_at)
            .bind(claim_token)
            .bind(now)
            .fetch_one(&mut *transaction)
            .await
            .map_err(storage_error)?;
        transaction.commit().await.map_err(storage_error)?;
        row_to_task(row).map(|task| Some(ClaimedVerificationTask { task, claim_token }))
    }

    async fn schedule_verification_task_retry(
        &self,
        task_id: &TaskId,
        worker_id: &str,
        claim_token: &uuid::Uuid,
        retry_after: time::Duration,
    ) -> Result<Option<VerificationTaskRecord>, ApplicationError> {
        let mut transaction = self.pool.begin().await.map_err(storage_error)?;
        sqlx::query("SELECT task_id FROM verification_tasks WHERE task_id = $1::uuid FOR UPDATE")
            .bind(task_id.to_string())
            .fetch_optional(&mut *transaction)
            .await
            .map_err(storage_error)?;
        let now: time::OffsetDateTime = sqlx::query_scalar("SELECT clock_timestamp()")
            .fetch_one(&mut *transaction)
            .await
            .map_err(storage_error)?;
        let next_attempt_at =
            now.checked_add(retry_after)
                .ok_or_else(|| ApplicationError::Storage {
                    message: "verification task retry time overflow".to_owned(),
                })?;
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
                next_attempt_at = $5,
                last_attempt_error = NULL,
                updated_at = $4
            WHERE task_id = $1::uuid
              AND status = 'in_progress'
              AND claimed_by = $2
              AND claim_token = $3
              AND claim_expires_at > $4
              AND deletion_job_id IS NULL
              AND NOT EXISTS (
                  SELECT 1
                  FROM content_lock_deletion_task_snapshot AS snapshot
                  WHERE snapshot.verification_task_id = verification_tasks.task_id
              )
            RETURNING {VERIFICATION_TASK_ROW_COLUMNS}"
        );
        let row = sqlx::query_as::<_, VerificationTaskRow>(&sql)
            .bind(task_id.to_string())
            .bind(worker_id)
            .bind(claim_token)
            .bind(now)
            .bind(next_attempt_at)
            .fetch_optional(&mut *transaction)
            .await
            .map_err(storage_error)?;
        let result = row.map(row_to_task).transpose()?;
        transaction.commit().await.map_err(storage_error)?;
        Ok(result)
    }

    async fn persist_claimed_verification_task_transition(
        &self,
        task: VerificationTaskRecord,
        worker_id: &str,
        claim_token: &uuid::Uuid,
    ) -> Result<Option<VerificationTaskRecord>, ApplicationError> {
        if !matches!(
            task.status,
            VerificationTaskStatus::Completed
                | VerificationTaskStatus::Failed
                | VerificationTaskStatus::Expired
        ) {
            return Err(ApplicationError::InvalidVerificationTaskState {
                message: "claimed task transition must be terminal".to_owned(),
            });
        }
        let mut transaction = self.pool.begin().await.map_err(storage_error)?;
        sqlx::query("SELECT task_id FROM verification_tasks WHERE task_id = $1::uuid FOR UPDATE")
            .bind(task.task_id.to_string())
            .fetch_optional(&mut *transaction)
            .await
            .map_err(storage_error)?;
        let now: time::OffsetDateTime = sqlx::query_scalar("SELECT clock_timestamp()")
            .fetch_one(&mut *transaction)
            .await
            .map_err(storage_error)?;
        let sql = format!(
            "UPDATE verification_tasks
             SET status = $5,
                 started_at = $6,
                 completed_at = $7,
                 failure_message = $8,
                 claimed_by = NULL,
                 claim_token = NULL,
                 claim_expires_at = NULL,
                 entitlement_publication_claim_token = NULL,
                 next_attempt_at = NULL,
                 last_attempt_error = NULL,
                 updated_at = $4
             WHERE task_id = $1::uuid
               AND status = 'in_progress'
               AND claimed_by = $2
               AND claim_token = $3
               AND claim_expires_at > $4
               AND deletion_job_id IS NULL
               AND NOT EXISTS (
                   SELECT 1
                   FROM content_lock_deletion_task_snapshot AS snapshot
                   WHERE snapshot.verification_task_id = verification_tasks.task_id
               )
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
            .fetch_optional(&mut *transaction)
            .await
            .map_err(storage_error)?;
        let result = row.map(row_to_task).transpose()?;
        transaction.commit().await.map_err(storage_error)?;
        Ok(result)
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
    use locks_core::verification::{Proof, SUBMITTED_PROOF_BUNDLE_VERSION, SubmittedProofBundle};

    use super::PostgresVerificationTaskClaimer;
    use crate::application::models::{VerificationTaskRecord, VerificationTaskStatus};
    use crate::application::ports::{VerificationTaskClaimer, VerificationTaskRepository};
    use crate::infrastructure::postgres::testing::TestDatabase;
    use crate::infrastructure::postgres::verification_tasks::PostgresVerificationTaskRepository;

    const LOCK_ID: &str = "000G40R40M30E209185GR38E1W8124GK2GAHC5RR34D1P70X3RFG";
    const BUNDLE_ID: &str = "000G40R40M30E209185GR38E1W";
    const BUNDLE_ID_2: &str = "000G40R40M30E209185GR38E1X";
    const BUNDLE_ID_3: &str = "000G40R40M30E209185GR38E1Y";
    const NOW: time::OffsetDateTime = datetime!(2026-05-29 12:10:00 UTC);
    const CLAIM_EXPIRES_AT: time::OffsetDateTime = datetime!(2026-05-29 12:15:00 UTC);

    #[tokio::test]
    async fn waiting_publication_update_rechecks_task_row_deletion_fence() {
        let database = TestDatabase::create().await;
        let repository = PostgresVerificationTaskRepository::new(database.pool().clone());
        let claimer = PostgresVerificationTaskClaimer::new(database.pool().clone());
        let pending = task(
            "018fc6ec-2f3d-4f7e-8b7d-6f5c4b3a2d13",
            VerificationTaskStatus::Pending,
            datetime!(2026-05-29 12:00:00 UTC),
        );
        repository
            .insert_verification_task(pending.clone())
            .await
            .unwrap();
        let claim = claimer
            .claim_next_verification_task("worker-a", (CLAIM_EXPIRES_AT) - (NOW))
            .await
            .unwrap()
            .unwrap();

        let mut transaction = database.pool().begin().await.unwrap();
        sqlx::query("SELECT task_id FROM verification_tasks WHERE task_id = $1 FOR UPDATE")
            .bind(pending.task_id.as_uuid())
            .fetch_one(&mut *transaction)
            .await
            .unwrap();

        let waiting_claimer = claimer.clone();
        let task_id = pending.task_id;
        let claim_token = claim.claim_token;
        let publication = tokio::spawn(async move {
            waiting_claimer
                .begin_claimed_entitlement_publication(&task_id, "worker-a", &claim_token)
                .await
        });
        tokio::time::sleep(std::time::Duration::from_millis(25)).await;
        assert!(!publication.is_finished());

        let deletion_job_id = uuid::Uuid::new_v4();
        sqlx::query(
            "INSERT INTO content_lock_deletion_jobs
             (job_id, creator, lock_id, frozen_content_lock, deletion_started_at, state, phase)
             VALUES ($1, $2, $3, '{}'::jsonb, $4, 'queued', 'withdraw')",
        )
        .bind(deletion_job_id)
        .bind(pending.creator.to_string())
        .bind(LOCK_ID)
        .bind(NOW)
        .execute(&mut *transaction)
        .await
        .unwrap();
        sqlx::query("UPDATE verification_tasks SET deletion_job_id = $1 WHERE task_id = $2")
            .bind(deletion_job_id)
            .bind(pending.task_id.as_uuid())
            .execute(&mut *transaction)
            .await
            .unwrap();
        transaction.commit().await.unwrap();

        assert!(!publication.await.unwrap().unwrap());
        database.cleanup().await;
    }

    #[tokio::test]
    async fn retry_retains_publication_marker_until_reconciled_terminal_transition() {
        let database = TestDatabase::create().await;
        let repository = PostgresVerificationTaskRepository::new(database.pool().clone());
        let claimer = PostgresVerificationTaskClaimer::new(database.pool().clone());
        let pending = task(
            "018fc6ec-2f3d-4f7e-8b7d-6f5c4b3a2d17",
            VerificationTaskStatus::Pending,
            datetime!(2026-05-29 12:00:00 UTC),
        );
        repository
            .insert_verification_task(pending.clone())
            .await
            .unwrap();
        let first = claimer
            .claim_next_verification_task("worker-a", (CLAIM_EXPIRES_AT) - (NOW))
            .await
            .unwrap()
            .unwrap();
        assert!(
            claimer
                .begin_claimed_entitlement_publication(
                    &pending.task_id,
                    "worker-a",
                    &first.claim_token,
                )
                .await
                .unwrap()
        );
        let retry_at = NOW + time::Duration::seconds(10);
        claimer
            .schedule_verification_task_retry(
                &pending.task_id,
                "worker-a",
                &first.claim_token,
                (retry_at) - (NOW),
            )
            .await
            .unwrap()
            .unwrap();

        let retained: Option<uuid::Uuid> = sqlx::query_scalar(
            "SELECT entitlement_publication_claim_token
             FROM verification_tasks
             WHERE task_id = $1",
        )
        .bind(pending.task_id.as_uuid())
        .fetch_one(database.pool())
        .await
        .unwrap();
        assert_eq!(retained, Some(first.claim_token));

        mark_retry_due(database.pool(), &pending.task_id).await;
        let second = claimer
            .claim_next_verification_task(
                "worker-b",
                (retry_at + time::Duration::minutes(5)) - (retry_at),
            )
            .await
            .unwrap()
            .unwrap();
        assert!(
            claimer
                .begin_claimed_entitlement_publication(
                    &pending.task_id,
                    "worker-b",
                    &second.claim_token,
                )
                .await
                .unwrap()
        );
        let completed = second
            .task
            .transition_to(VerificationTaskStatus::Completed, retry_at, None)
            .unwrap();
        claimer
            .persist_claimed_verification_task_transition(
                completed,
                "worker-b",
                &second.claim_token,
            )
            .await
            .unwrap()
            .unwrap();

        let cleared: Option<uuid::Uuid> = sqlx::query_scalar(
            "SELECT entitlement_publication_claim_token
             FROM verification_tasks
             WHERE task_id = $1",
        )
        .bind(pending.task_id.as_uuid())
        .fetch_one(database.pool())
        .await
        .unwrap();
        assert_eq!(cleared, None);

        database.cleanup().await;
    }

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

        let before_claim = database_time(database.pool()).await;
        let claimed = claimer
            .claim_next_verification_task("worker-a", (CLAIM_EXPIRES_AT) - (NOW))
            .await
            .unwrap()
            .expect("oldest pending task is claimed");

        assert_eq!(claimed.task.task_id, older.task_id);
        assert_eq!(claimed.task.status, VerificationTaskStatus::InProgress);
        let after_claim = database_time(database.pool()).await;
        assert!(
            claimed.task.started_at.is_some_and(|started_at| {
                before_claim <= started_at && started_at <= after_claim
            })
        );

        database.cleanup().await;
    }

    #[tokio::test]
    async fn paykit_reservation_is_not_claimable_until_marked_ready() {
        use crate::infrastructure::postgres::PostgresPaykitTaskAdmissionRepository;

        let database = TestDatabase::create().await;
        let claimer = PostgresVerificationTaskClaimer::new(database.pool().clone());
        let admissions = PostgresPaykitTaskAdmissionRepository::new(database.pool().clone());
        let pending = task(
            "018fc6ec-2f3d-4f7e-8b7d-6f5c4b3a2d15",
            VerificationTaskStatus::Pending,
            datetime!(2026-05-29 12:00:00 UTC),
        );

        let first = admissions.reserve(pending.clone(), 24).await.unwrap();
        assert!(first.requires_paykit);
        assert!(
            claimer
                .claim_next_verification_task("worker-a", (CLAIM_EXPIRES_AT) - (NOW))
                .await
                .unwrap()
                .is_none()
        );

        let replay = admissions.reserve(pending.clone(), 24).await.unwrap();
        assert!(replay.requires_paykit);
        assert_eq!(replay.task, pending);

        let invoice_window = crate::infrastructure::postgres::PaykitInvoiceWindow {
            invoice_created_at: datetime!(2026-05-29 12:00:00 UTC),
            payment_deadline: datetime!(2026-05-30 12:00:00 UTC),
        };
        admissions
            .mark_ready(&pending, invoice_window)
            .await
            .unwrap();
        let divergent_window = crate::infrastructure::postgres::PaykitInvoiceWindow {
            invoice_created_at: datetime!(2026-05-29 12:00:01 UTC),
            payment_deadline: datetime!(2026-05-30 12:00:01 UTC),
        };
        assert!(
            admissions
                .mark_ready(&pending, divergent_window)
                .await
                .is_err()
        );
        let ready_replay = admissions.reserve(pending.clone(), 24).await.unwrap();
        assert!(!ready_replay.requires_paykit);
        assert_eq!(ready_replay.task, pending);
        assert_eq!(ready_replay.payment_in, 24);
        assert_eq!(ready_replay.invoice_window, Some(invoice_window));
        assert_eq!(
            claimer
                .claim_next_verification_task("worker-a", (CLAIM_EXPIRES_AT) - (NOW))
                .await
                .unwrap()
                .unwrap()
                .task
                .task_id,
            pending.task_id
        );

        database.cleanup().await;
    }

    #[tokio::test]
    async fn legacy_paykit_admission_without_authoritative_window_fails_closed() {
        use crate::infrastructure::postgres::PostgresPaykitTaskAdmissionRepository;

        let database = TestDatabase::create().await;
        let tasks = PostgresVerificationTaskRepository::new(database.pool().clone());
        let admissions = PostgresPaykitTaskAdmissionRepository::new(database.pool().clone());
        let pending = task(
            "018fc6ec-2f3d-4f7e-8b7d-6f5c4b3a2d16",
            VerificationTaskStatus::Pending,
            datetime!(2026-05-29 12:00:00 UTC),
        );
        tasks
            .insert_verification_task(pending.clone())
            .await
            .unwrap();
        sqlx::query(
            "INSERT INTO paykit_task_admissions
                 (verification_task_id, ready, ready_at)
             VALUES ($1::uuid, TRUE, now())",
        )
        .bind(pending.task_id.to_string())
        .execute(database.pool())
        .await
        .unwrap();

        assert!(
            admissions
                .find_existing(&pending.submitted_proof_bundle)
                .await
                .is_err()
        );
        let claimer = PostgresVerificationTaskClaimer::new(database.pool().clone());
        assert!(
            claimer
                .claim_next_verification_task("worker-a", (CLAIM_EXPIRES_AT) - (NOW))
                .await
                .unwrap()
                .is_none()
        );

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
                .claim_next_verification_task("worker-a", (CLAIM_EXPIRES_AT) - (NOW))
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
            .claim_next_verification_task("worker-b", (CLAIM_EXPIRES_AT) - (NOW))
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
                .claim_next_verification_task("worker-b", (CLAIM_EXPIRES_AT) - (NOW))
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
            claimer_a.claim_next_verification_task("worker-a", (CLAIM_EXPIRES_AT) - (NOW)),
            claimer_b.claim_next_verification_task("worker-b", (CLAIM_EXPIRES_AT) - (NOW)),
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
            .claim_next_verification_task("worker-a", (CLAIM_EXPIRES_AT) - (NOW))
            .await
            .unwrap()
            .unwrap();
        expire_claim(database.pool(), &first.task.task_id).await;
        let reclaimed_at = database_time(database.pool()).await;
        let second = claimer
            .claim_next_verification_task(
                "worker-a",
                (reclaimed_at + time::Duration::minutes(5)) - (reclaimed_at),
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
                    (reclaimed_at + time::Duration::seconds(10)) - (reclaimed_at),
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
                    (reclaimed_at + time::Duration::seconds(10)) - (reclaimed_at),
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
        repository.insert_verification_task(pending).await.unwrap();
        let first = claimer
            .claim_next_verification_task("worker-a", (CLAIM_EXPIRES_AT) - (NOW))
            .await
            .unwrap()
            .unwrap();
        expire_claim(database.pool(), &first.task.task_id).await;
        let reclaimed_at = database_time(database.pool()).await;
        let second = claimer
            .claim_next_verification_task(
                "worker-a",
                (reclaimed_at + time::Duration::minutes(5)) - (reclaimed_at),
            )
            .await
            .unwrap()
            .unwrap();
        let completed = second
            .task
            .clone()
            .transition_to(
                VerificationTaskStatus::Completed,
                reclaimed_at + time::Duration::seconds(1),
                None,
            )
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

        for terminal in [completed.clone(), failed] {
            assert_eq!(
                claimer
                    .persist_claimed_verification_task_transition(
                        terminal,
                        "worker-a",
                        &first.claim_token,
                    )
                    .await
                    .unwrap(),
                None
            );
        }
        assert_eq!(
            claimer
                .persist_claimed_verification_task_transition(
                    completed.clone(),
                    "worker-a",
                    &second.claim_token,
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
            .claim_next_verification_task("worker-a", (CLAIM_EXPIRES_AT) - (NOW))
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
                    (next_attempt_at) - (NOW),
                )
                .await
                .unwrap(),
            None
        );
        let scheduled = claimer
            .schedule_verification_task_retry(
                &pending.task_id,
                "worker-a",
                &claim.claim_token,
                (next_attempt_at) - (NOW),
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
                    (CLAIM_EXPIRES_AT) - (next_attempt_at - time::Duration::milliseconds(1)),
                )
                .await
                .unwrap(),
            None
        );
        mark_retry_due(database.pool(), &pending.task_id).await;
        assert!(
            claimer
                .claim_next_verification_task(
                    "worker-b",
                    (CLAIM_EXPIRES_AT + time::Duration::seconds(10)) - (next_attempt_at),
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
            SET claimed_by = 'worker-a', claim_expires_at = clock_timestamp()
            WHERE task_id = $1::uuid",
        )
        .bind(task_id.to_string())
        .execute(pool)
        .await
        .expect("mark claim expired");
    }

    async fn mark_claim_active(pool: &sqlx::PgPool, task_id: &TaskId) {
        sqlx::query(
            "UPDATE verification_tasks
            SET claimed_by = 'worker-a',
                claim_expires_at = clock_timestamp() + INTERVAL '5 minutes'
            WHERE task_id = $1::uuid",
        )
        .bind(task_id.to_string())
        .execute(pool)
        .await
        .expect("mark claim active");
    }

    async fn expire_claim(pool: &sqlx::PgPool, task_id: &TaskId) {
        sqlx::query(
            "UPDATE verification_tasks
             SET claim_expires_at = clock_timestamp()
             WHERE task_id = $1::uuid",
        )
        .bind(task_id.to_string())
        .execute(pool)
        .await
        .expect("expire claim at the database clock boundary");
    }

    async fn mark_retry_due(pool: &sqlx::PgPool, task_id: &TaskId) {
        sqlx::query(
            "UPDATE verification_tasks
             SET next_attempt_at = clock_timestamp()
             WHERE task_id = $1::uuid",
        )
        .bind(task_id.to_string())
        .execute(pool)
        .await
        .expect("make retry due at the database clock boundary");
    }

    async fn database_time(pool: &sqlx::PgPool) -> time::OffsetDateTime {
        sqlx::query_scalar("SELECT clock_timestamp()")
            .fetch_one(pool)
            .await
            .expect("sample database clock")
    }

    fn terminal_task(task_id: &str, status: VerificationTaskStatus) -> VerificationTaskRecord {
        let started_at = datetime!(2026-05-29 12:00:00 UTC);
        let in_progress = task(task_id, VerificationTaskStatus::Pending, started_at)
            .transition_to(VerificationTaskStatus::InProgress, started_at, None)
            .unwrap();
        match status {
            VerificationTaskStatus::Completed => in_progress
                .transition_to(
                    VerificationTaskStatus::Completed,
                    datetime!(2026-05-29 12:01:00 UTC),
                    None,
                )
                .unwrap(),
            VerificationTaskStatus::Failed => in_progress
                .transition_to(
                    VerificationTaskStatus::Failed,
                    datetime!(2026-05-29 12:01:00 UTC),
                    Some("failed".to_owned()),
                )
                .unwrap(),
            VerificationTaskStatus::Expired => {
                task(task_id, VerificationTaskStatus::Pending, started_at)
                    .transition_to(
                        VerificationTaskStatus::Expired,
                        datetime!(2026-05-29 12:01:00 UTC),
                        None,
                    )
                    .unwrap()
            }
            VerificationTaskStatus::Pending | VerificationTaskStatus::InProgress => unreachable!(),
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
                    "{}/pub/locks.app/{LOCK_ID}.json",
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
