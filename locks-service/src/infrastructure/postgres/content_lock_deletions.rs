use std::str::FromStr;

use async_trait::async_trait;
use locks_core::{
    ids::{CreatorPubky, LockId},
    lock_policy::ContentLock,
};
use sqlx::{FromRow, PgPool, Postgres, Transaction};
use time::OffsetDateTime;
use uuid::Uuid;

use crate::application::{
    errors::ApplicationError,
    models::{
        ClaimedContentLockDeletionJob, ContentLockDeletionFailureCode, ContentLockDeletionJob,
        ContentLockDeletionPhase, ContentLockDeletionState, ForceDeletionReceipt,
        PrepareForceDeletionResult,
    },
    ports::{ContentLockDeletionRepository, publication_reconciliation_matches},
};
use crate::infrastructure::postgres::proof_admission::lock_proof_admission;

const ROW_COLUMNS: &str = "job_id, creator, lock_id, frozen_content_lock, deletion_started_at, state, phase, attempt_count, next_attempt_at, force_requested_at, failure_code, claimed_by, claim_token, claim_expires_at";

#[derive(Debug, FromRow)]
struct DeletionJobRow {
    job_id: Uuid,
    creator: String,
    lock_id: String,
    frozen_content_lock: serde_json::Value,
    deletion_started_at: OffsetDateTime,
    state: String,
    phase: String,
    attempt_count: i64,
    next_attempt_at: Option<OffsetDateTime>,
    force_requested_at: Option<OffsetDateTime>,
    failure_code: Option<String>,
    claimed_by: Option<String>,
    claim_token: Option<Uuid>,
    claim_expires_at: Option<OffsetDateTime>,
}

#[derive(Debug, FromRow)]
struct ForceDeletionReceiptRow {
    creator: String,
    lock_id: String,
    forced_at: OffsetDateTime,
    frozen_content_lock: Option<serde_json::Value>,
    completed_at: Option<OffsetDateTime>,
}

/// PostgreSQL-backed durable deletion job queue and permanent force receipt store.
#[derive(Debug, Clone)]
pub struct PostgresContentLockDeletionRepository {
    pool: PgPool,
}

impl PostgresContentLockDeletionRepository {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }
}

#[async_trait]
impl ContentLockDeletionRepository for PostgresContentLockDeletionRepository {
    async fn begin_publication(
        &self,
        creator: &CreatorPubky,
        lock_id: &LockId,
        publication_token: Uuid,
    ) -> Result<(), ApplicationError> {
        let mut transaction = self.pool.begin().await.map_err(storage_error)?;
        lock_proof_admission(&mut transaction, creator, lock_id).await?;
        let deletion_exists = sqlx::query_scalar::<_, bool>(
            "SELECT EXISTS (SELECT 1 FROM content_lock_force_deletion_receipts WHERE creator = $1 AND lock_id = $2)
             OR EXISTS (SELECT 1 FROM content_lock_deletion_jobs WHERE creator = $1 AND lock_id = $2)",
        )
        .bind(creator.to_string()).bind(lock_id.to_string())
        .fetch_one(&mut *transaction).await.map_err(storage_error)?;
        if deletion_exists {
            return Err(ApplicationError::ContentLockDeletionInProgress);
        }
        sqlx::query("INSERT INTO content_lock_publication_intents (creator, lock_id, publication_token) VALUES ($1, $2, $3)")
            .bind(creator.to_string()).bind(lock_id.to_string()).bind(publication_token)
            .execute(&mut *transaction).await.map_err(map_publication_insert_error)?;
        transaction.commit().await.map_err(storage_error)
    }

    async fn finish_publication(
        &self,
        creator: &CreatorPubky,
        lock_id: &LockId,
        publication_token: Uuid,
    ) -> Result<bool, ApplicationError> {
        delete_publication_intent(&self.pool, creator, lock_id, publication_token).await
    }

    async fn mark_publication_written(
        &self,
        creator: &CreatorPubky,
        lock_id: &LockId,
        publication_token: Uuid,
    ) -> Result<bool, ApplicationError> {
        let mut transaction = self.pool.begin().await.map_err(storage_error)?;
        lock_proof_admission(&mut transaction, creator, lock_id).await?;
        let result = sqlx::query(
            "UPDATE content_lock_publication_intents
             SET external_write_completed_at = COALESCE(external_write_completed_at, clock_timestamp())
             WHERE creator = $1 AND lock_id = $2 AND publication_token = $3",
        )
        .bind(creator.to_string())
        .bind(lock_id.to_string())
        .bind(publication_token)
        .execute(&mut *transaction)
        .await
        .map_err(storage_error)?;
        transaction.commit().await.map_err(storage_error)?;
        Ok(result.rows_affected() == 1)
    }

    async fn abandon_publication(
        &self,
        creator: &CreatorPubky,
        lock_id: &LockId,
        publication_token: Uuid,
    ) -> Result<bool, ApplicationError> {
        delete_publication_intent(&self.pool, creator, lock_id, publication_token).await
    }

    async fn reconcile_publication(
        &self,
        creator: &CreatorPubky,
        lock_id: &LockId,
        published_content_lock: &ContentLock,
        published_guarded_paths: &[String],
    ) -> Result<bool, ApplicationError> {
        if !publication_reconciliation_matches(
            creator,
            lock_id,
            published_content_lock,
            published_guarded_paths,
        ) {
            return Ok(false);
        }
        let mut transaction = self.pool.begin().await.map_err(storage_error)?;
        lock_proof_admission(&mut transaction, creator, lock_id).await?;
        let intent_exists = sqlx::query_scalar::<_, bool>(
            "SELECT EXISTS (
                SELECT 1 FROM content_lock_publication_intents
                WHERE creator = $1 AND lock_id = $2
                  AND external_write_completed_at IS NOT NULL
            )",
        )
        .bind(creator.to_string())
        .bind(lock_id.to_string())
        .fetch_one(&mut *transaction)
        .await
        .map_err(storage_error)?;
        if !intent_exists {
            transaction.commit().await.map_err(storage_error)?;
            return Ok(false);
        }
        let ownership = sqlx::query_as::<_, (String, String)>(
            "SELECT guarded_path, status
             FROM content_lock_ownership
             WHERE creator = $1 AND lock_id = $2",
        )
        .bind(creator.to_string())
        .bind(lock_id.to_string())
        .fetch_all(&mut *transaction)
        .await
        .map_err(storage_error)?;
        if ownership.len() != published_guarded_paths.len()
            || ownership.iter().any(|(guarded_path, status)| {
                status != "published" || !published_guarded_paths.contains(guarded_path)
            })
        {
            transaction.commit().await.map_err(storage_error)?;
            return Ok(false);
        }
        let result = sqlx::query(
            "DELETE FROM content_lock_publication_intents
             WHERE creator = $1 AND lock_id = $2
               AND external_write_completed_at IS NOT NULL",
        )
        .bind(creator.to_string())
        .bind(lock_id.to_string())
        .execute(&mut *transaction)
        .await
        .map_err(storage_error)?;
        transaction.commit().await.map_err(storage_error)?;
        Ok(result.rows_affected() == 1)
    }

    async fn publication_in_progress(
        &self,
        creator: &CreatorPubky,
        lock_id: &LockId,
    ) -> Result<bool, ApplicationError> {
        let mut transaction = self.pool.begin().await.map_err(storage_error)?;
        lock_proof_admission(&mut transaction, creator, lock_id).await?;
        let exists = sqlx::query_scalar::<_, bool>(
            "SELECT EXISTS (SELECT 1 FROM content_lock_publication_intents WHERE creator = $1 AND lock_id = $2)",
        )
        .bind(creator.to_string())
        .bind(lock_id.to_string())
        .fetch_one(&mut *transaction)
        .await
        .map_err(storage_error)?;
        transaction.commit().await.map_err(storage_error)?;
        Ok(exists)
    }

    async fn insert_job(&self, job: ContentLockDeletionJob) -> Result<(), ApplicationError> {
        job.validate_frozen_identity()?;
        job.validate_state(false)?;
        let frozen = serde_json::to_value(&job.frozen_content_lock).map_err(storage_display)?;
        let lock_resource = format!(
            "{}/pub/app.locks/{}.json",
            job.creator,
            job.lock_id.as_str()
        );
        let mut transaction = self.pool.begin().await.map_err(storage_error)?;
        lock_proof_admission(&mut transaction, &job.creator, &job.lock_id).await?;
        let entitlement_publication_in_progress = sqlx::query_scalar::<_, bool>(
            "SELECT EXISTS (
                SELECT 1 FROM verification_tasks
                WHERE creator = $1
                  AND submitted_proof_bundle->>'pubky_lock_resource' = $2
                  AND (status = 'publishing_entitlement'
                       OR entitlement_publication_claim_token IS NOT NULL)
            )",
        )
        .bind(job.creator.to_string())
        .bind(&lock_resource)
        .fetch_one(&mut *transaction)
        .await
        .map_err(storage_error)?;
        if entitlement_publication_in_progress {
            return Err(ApplicationError::ContentLockDeletionInProgress);
        }
        let publication_exists = sqlx::query_scalar::<_, bool>(
            "SELECT EXISTS (SELECT 1 FROM content_lock_publication_intents
             WHERE creator = $1 AND lock_id = $2)",
        )
        .bind(job.creator.to_string())
        .bind(job.lock_id.to_string())
        .fetch_one(&mut *transaction)
        .await
        .map_err(storage_error)?;
        if publication_exists {
            return Err(ApplicationError::ContentLockPublicationInProgress);
        }
        let receipt_exists = sqlx::query_scalar::<_, bool>(
            "SELECT EXISTS (SELECT 1 FROM content_lock_force_deletion_receipts
             WHERE creator = $1 AND lock_id = $2)",
        )
        .bind(job.creator.to_string())
        .bind(job.lock_id.to_string())
        .fetch_one(&mut *transaction)
        .await
        .map_err(storage_error)?;
        if receipt_exists {
            return Err(ApplicationError::ContentLockDeletionInProgress);
        }
        sqlx::query(
            "INSERT INTO content_lock_deletion_jobs
             (job_id, creator, lock_id, frozen_content_lock, deletion_started_at, state, phase,
              attempt_count, next_attempt_at, force_requested_at, failure_code)
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)",
        )
        .bind(job.job_id)
        .bind(job.creator.to_string())
        .bind(job.lock_id.to_string())
        .bind(frozen)
        .bind(job.deletion_started_at)
        .bind(state_to_database(job.state))
        .bind(phase_to_database(job.phase))
        .bind(i64::from(job.attempt_count))
        .bind(job.next_attempt_at)
        .bind(job.force_requested_at)
        .bind(job.failure_code.map(ContentLockDeletionFailureCode::as_str))
        .execute(&mut *transaction)
        .await
        .map_err(map_insert_error)?;
        sqlx::query(
            "WITH locked_tasks AS MATERIALIZED (
                 SELECT task_id, creator, bundle_id, submitted_proof_bundle, status
                 FROM verification_tasks
                 WHERE creator = $2
                   AND submitted_proof_bundle->>'pubky_lock_resource' = $3
                 ORDER BY task_id
                 FOR UPDATE
             )
             INSERT INTO content_lock_deletion_task_snapshot
                 (deletion_job_id, verification_task_id, creator, bundle_id,
                  pubky_lock_resource, criterion_id, status_at_cutoff,
                  paykit_admission_required)
             SELECT $1, task_id, creator, bundle_id,
                    submitted_proof_bundle->>'pubky_lock_resource',
                    (SELECT proof->>'criterion_id'
                     FROM jsonb_array_elements(submitted_proof_bundle->'proofs') AS proof
                     WHERE proof->>'verifier_type' = 'paykit-payment'
                     LIMIT 1),
                    status,
                    EXISTS (
                        SELECT 1
                        FROM jsonb_array_elements(submitted_proof_bundle->'proofs') AS proof
                        WHERE proof->>'verifier_type' = 'paykit-payment'
                    )
             FROM locked_tasks",
        )
        .bind(job.job_id)
        .bind(job.creator.to_string())
        .bind(lock_resource)
        .execute(&mut *transaction)
        .await
        .map_err(storage_error)?;
        sqlx::query(
            "UPDATE verification_tasks AS task
             SET status = CASE
                     WHEN task.status IN ('pending', 'in_progress') THEN 'pending'
                     ELSE task.status
                 END,
                 started_at = CASE
                     WHEN task.status IN ('pending', 'in_progress') THEN NULL
                     ELSE task.started_at
                 END,
                 claimed_by = CASE
                     WHEN task.invoice_admission_phase = 'invoice_pending' THEN task.claimed_by
                     ELSE NULL
                 END,
                 claim_token = CASE
                     WHEN task.invoice_admission_phase = 'invoice_pending' THEN task.claim_token
                     ELSE NULL
                 END,
                 claim_expires_at = CASE
                     WHEN task.invoice_admission_phase = 'invoice_pending' THEN task.claim_expires_at
                     ELSE NULL
                 END,
                 next_attempt_at = CASE
                     WHEN task.invoice_admission_phase = 'invoice_pending' THEN task.next_attempt_at
                     ELSE NULL
                 END,
                 last_attempt_error = CASE
                     WHEN task.invoice_admission_phase = 'invoice_pending' THEN task.last_attempt_error
                     ELSE NULL
                 END,
                 deletion_job_id = $1, updated_at = $2
             FROM content_lock_deletion_task_snapshot AS snapshot
             WHERE snapshot.deletion_job_id = $1
               AND snapshot.verification_task_id = task.task_id",
        )
        .bind(job.job_id)
        .bind(job.deletion_started_at)
        .execute(&mut *transaction)
        .await
        .map_err(storage_error)?;
        let publication_won_race = sqlx::query_scalar::<_, bool>(
            "SELECT EXISTS (
                SELECT 1 FROM verification_tasks
                WHERE deletion_job_id = $1
                  AND (status = 'publishing_entitlement'
                       OR entitlement_publication_claim_token IS NOT NULL)
            )",
        )
        .bind(job.job_id)
        .fetch_one(&mut *transaction)
        .await
        .map_err(storage_error)?;
        if publication_won_race {
            return Err(ApplicationError::ContentLockDeletionInProgress);
        }
        transaction.commit().await.map_err(storage_error)
    }

    async fn get_job(
        &self,
        creator: &CreatorPubky,
        lock_id: &LockId,
    ) -> Result<Option<ContentLockDeletionJob>, ApplicationError> {
        let sql = format!(
            "SELECT {ROW_COLUMNS} FROM content_lock_deletion_jobs WHERE creator = $1 AND lock_id = $2"
        );
        sqlx::query_as::<_, DeletionJobRow>(&sql)
            .bind(creator.to_string())
            .bind(lock_id.to_string())
            .fetch_optional(&self.pool)
            .await
            .map_err(storage_error)?
            .map(row_to_job)
            .transpose()
    }

    async fn claim_next(
        &self,
        worker_id: &str,
        now: OffsetDateTime,
        claim_expires_at: OffsetDateTime,
    ) -> Result<Option<ClaimedContentLockDeletionJob>, ApplicationError> {
        let claim_token = Uuid::new_v4();
        let sql = format!(
            "WITH candidate AS MATERIALIZED (
                 SELECT job_id AS candidate_job_id FROM content_lock_deletion_jobs
                 WHERE (state = 'queued'
                        AND (next_attempt_at IS NULL OR next_attempt_at <= clock_timestamp()))
                    OR (state = 'running' AND claim_expires_at <= clock_timestamp())
                 ORDER BY deletion_started_at
                 FOR UPDATE SKIP LOCKED LIMIT 1
             ), timing AS MATERIALIZED (
                 SELECT clock_timestamp() AS repository_now FROM candidate
             )
             UPDATE content_lock_deletion_jobs AS job
             SET state = 'running', claimed_by = $1, claim_token = $2,
                 claim_expires_at = timing.repository_now + ($3 - $4), next_attempt_at = NULL,
                 attempt_count = job.attempt_count + 1, updated_at = timing.repository_now
             FROM candidate, timing
             WHERE job.job_id = candidate.candidate_job_id
             RETURNING {ROW_COLUMNS}"
        );
        let row = sqlx::query_as::<_, DeletionJobRow>(&sql)
            .bind(worker_id)
            .bind(claim_token)
            .bind(claim_expires_at)
            .bind(now)
            .fetch_optional(&self.pool)
            .await
            .map_err(storage_error)?;
        row.map(row_to_job)
            .transpose()
            .map(|job| job.map(|job| ClaimedContentLockDeletionJob { job, claim_token }))
    }

    async fn schedule_retry(
        &self,
        job_id: Uuid,
        worker_id: &str,
        claim_token: Uuid,
        now: OffsetDateTime,
        next_attempt_at: OffsetDateTime,
    ) -> Result<Option<ContentLockDeletionJob>, ApplicationError> {
        let sql = format!(
            "WITH locked AS MATERIALIZED (
                 SELECT job_id AS locked_job_id FROM content_lock_deletion_jobs
                 WHERE job_id = $1 AND state = 'running' AND claimed_by = $2
                   AND claim_token = $3
                 FOR UPDATE
             ), timing AS MATERIALIZED (
                 SELECT clock_timestamp() AS repository_now FROM locked
             )
             UPDATE content_lock_deletion_jobs AS job
             SET state = 'queued',
                 next_attempt_at = timing.repository_now + ($5 - $4),
                 claimed_by = NULL, claim_token = NULL, claim_expires_at = NULL,
                 updated_at = timing.repository_now
             FROM locked, timing
             WHERE job.job_id = locked.locked_job_id
               AND job.claim_expires_at > timing.repository_now
             RETURNING {ROW_COLUMNS}"
        );
        fetch_optional_job(
            sqlx::query_as::<_, DeletionJobRow>(&sql)
                .bind(job_id)
                .bind(worker_id)
                .bind(claim_token)
                .bind(now)
                .bind(next_attempt_at)
                .fetch_optional(&self.pool)
                .await
                .map_err(storage_error)?,
        )
    }

    async fn advance_phase(
        &self,
        job_id: Uuid,
        worker_id: &str,
        claim_token: Uuid,
        now: OffsetDateTime,
        next_phase: ContentLockDeletionPhase,
    ) -> Result<Option<ContentLockDeletionJob>, ApplicationError> {
        let mut transaction = self.pool.begin().await.map_err(storage_error)?;
        let current = load_owned_claim(&mut transaction, job_id, worker_id, claim_token).await?;
        let Some(current) = current else {
            transaction.rollback().await.map_err(storage_error)?;
            return Ok(None);
        };
        if !current.phase.permits(next_phase) {
            return Err(ApplicationError::InvalidContentLockDeletionState {
                message: "deletion phase must advance to its immediate successor".to_owned(),
            });
        }
        if next_phase == ContentLockDeletionPhase::StartPaymentDrain {
            let has_unready_invoice_admission = sqlx::query_scalar::<_, bool>(
                "SELECT EXISTS (
                    SELECT 1
                    FROM content_lock_deletion_task_snapshot AS snapshot
                    JOIN verification_tasks AS task
                      ON task.task_id = snapshot.verification_task_id
                    WHERE snapshot.deletion_job_id = $1
                      AND snapshot.paykit_admission_required = TRUE
                      AND (task.invoice_admission_phase = 'invoice_pending'
                           OR (task.invoice_admission_phase = 'ready'
                               AND (task.invoice_created_at IS NULL
                                    OR task.payment_deadline IS NULL
                                    OR task.payment_deadline <= task.invoice_created_at)))
                )",
            )
            .bind(job_id)
            .fetch_one(&mut *transaction)
            .await
            .map_err(storage_error)?;
            if has_unready_invoice_admission {
                return Err(ApplicationError::InvalidContentLockDeletionState {
                    message:
                        "payment drain cannot start while snapshotted invoice admissions are unready"
                            .to_owned(),
                });
            }
            sqlx::query(
                "UPDATE content_lock_deletion_task_snapshot AS snapshot
                 SET invoice_created_at = task.invoice_created_at,
                     payment_deadline = task.payment_deadline,
                     resolved_status = CASE
                         WHEN task.status IN ('completed', 'failed', 'expired')
                         THEN task.status
                         WHEN snapshot.status_at_cutoff IN ('completed', 'failed', 'expired')
                         THEN snapshot.status_at_cutoff
                         ELSE NULL
                     END,
                     resolved_at = CASE
                         WHEN task.status IN ('completed', 'failed', 'expired')
                              OR snapshot.status_at_cutoff IN ('completed', 'failed', 'expired')
                         THEN $2
                         ELSE NULL
                     END
                 FROM verification_tasks AS task
                 WHERE snapshot.deletion_job_id = $1
                   AND snapshot.paykit_admission_required = TRUE
                   AND task.task_id = snapshot.verification_task_id",
            )
            .bind(job_id)
            .bind(now)
            .execute(&mut *transaction)
            .await
            .map_err(storage_error)?;
        }
        let sql = format!(
            "UPDATE content_lock_deletion_jobs
             SET phase = $2, state = 'queued', attempt_count = 0, next_attempt_at = NULL,
                 failure_code = NULL, claimed_by = NULL, claim_token = NULL,
                 claim_expires_at = NULL, updated_at = clock_timestamp()
             WHERE job_id = $1 AND state = 'running' AND claimed_by = $3
               AND claim_token = $4 AND claim_expires_at > clock_timestamp()
             RETURNING {ROW_COLUMNS}"
        );
        let updated = sqlx::query_as::<_, DeletionJobRow>(&sql)
            .bind(job_id)
            .bind(phase_to_database(next_phase))
            .bind(worker_id)
            .bind(claim_token)
            .fetch_optional(&mut *transaction)
            .await
            .map_err(storage_error)?;
        let Some(updated) = updated else {
            transaction.rollback().await.map_err(storage_error)?;
            return Ok(None);
        };
        transaction.commit().await.map_err(storage_error)?;
        Ok(Some(row_to_job(updated)?))
    }

    async fn finish(
        &self,
        job_id: Uuid,
        worker_id: &str,
        claim_token: Uuid,
        now: OffsetDateTime,
        failure_code: Option<ContentLockDeletionFailureCode>,
    ) -> Result<Option<ContentLockDeletionJob>, ApplicationError> {
        let state = if failure_code.is_some() {
            "failed"
        } else {
            "completed"
        };
        let sql = format!(
            "WITH locked AS MATERIALIZED (
                 SELECT job_id AS locked_job_id FROM content_lock_deletion_jobs
                 WHERE job_id = $1 AND state = 'running' AND claimed_by = $2
                   AND claim_token = $3
                 FOR UPDATE
             ), timing AS MATERIALIZED (
                 SELECT clock_timestamp() AS repository_now FROM locked
             )
             UPDATE content_lock_deletion_jobs AS job
             SET state = $5, failure_code = $6, next_attempt_at = NULL,
                 claimed_by = NULL, claim_token = NULL, claim_expires_at = NULL,
                 updated_at = timing.repository_now
             FROM locked, timing
             WHERE job.job_id = locked.locked_job_id
               AND job.claim_expires_at > timing.repository_now
             RETURNING {ROW_COLUMNS}"
        );
        fetch_optional_job(
            sqlx::query_as::<_, DeletionJobRow>(&sql)
                .bind(job_id)
                .bind(worker_id)
                .bind(claim_token)
                .bind(now)
                .bind(state)
                .bind(failure_code.map(ContentLockDeletionFailureCode::as_str))
                .fetch_optional(&self.pool)
                .await
                .map_err(storage_error)?,
        )
    }

    async fn resume_failed_job(
        &self,
        creator: &CreatorPubky,
        lock_id: &LockId,
        resumed_at: OffsetDateTime,
    ) -> Result<Option<ContentLockDeletionJob>, ApplicationError> {
        let mut transaction = self.pool.begin().await.map_err(storage_error)?;
        lock_proof_admission(&mut transaction, creator, lock_id).await?;
        let receipt_exists = sqlx::query_scalar::<_, bool>(
            "SELECT EXISTS (SELECT 1 FROM content_lock_force_deletion_receipts
             WHERE creator = $1 AND lock_id = $2)",
        )
        .bind(creator.to_string())
        .bind(lock_id.to_string())
        .fetch_one(&mut *transaction)
        .await
        .map_err(storage_error)?;
        if receipt_exists {
            transaction.commit().await.map_err(storage_error)?;
            return Ok(None);
        }
        let sql = format!(
            "UPDATE content_lock_deletion_jobs
             SET state = 'queued', attempt_count = 0, next_attempt_at = NULL,
                 failure_code = NULL, claimed_by = NULL, claim_token = NULL,
                 claim_expires_at = NULL, updated_at = $3
             WHERE creator = $1 AND lock_id = $2 AND state = 'failed'
             RETURNING {ROW_COLUMNS}"
        );
        let resumed = sqlx::query_as::<_, DeletionJobRow>(&sql)
            .bind(creator.to_string())
            .bind(lock_id.to_string())
            .bind(resumed_at)
            .fetch_optional(&mut *transaction)
            .await
            .map_err(storage_error)?;
        let current = if resumed.is_some() {
            resumed
        } else {
            let sql = format!(
                "SELECT {ROW_COLUMNS} FROM content_lock_deletion_jobs
                 WHERE creator = $1 AND lock_id = $2"
            );
            sqlx::query_as::<_, DeletionJobRow>(&sql)
                .bind(creator.to_string())
                .bind(lock_id.to_string())
                .fetch_optional(&mut *transaction)
                .await
                .map_err(storage_error)?
        };
        transaction.commit().await.map_err(storage_error)?;
        fetch_optional_job(current)
    }

    async fn prepare_force_deletion(
        &self,
        creator: &CreatorPubky,
        lock_id: &LockId,
        frozen_content_lock: Option<&ContentLock>,
        forced_at: OffsetDateTime,
    ) -> Result<PrepareForceDeletionResult, ApplicationError> {
        let mut transaction = self.pool.begin().await.map_err(storage_error)?;
        lock_proof_admission(&mut transaction, creator, lock_id).await?;
        let publication_in_progress = sqlx::query_scalar::<_, bool>(
            "SELECT EXISTS (SELECT 1 FROM content_lock_publication_intents WHERE creator = $1 AND lock_id = $2)",
        )
        .bind(creator.to_string()).bind(lock_id.to_string())
        .fetch_one(&mut *transaction).await.map_err(storage_error)?;
        if publication_in_progress {
            transaction.commit().await.map_err(storage_error)?;
            return Ok(PrepareForceDeletionResult::PublicationInProgress);
        }
        let sql = format!(
            "SELECT {ROW_COLUMNS} FROM content_lock_deletion_jobs
             WHERE creator = $1 AND lock_id = $2 FOR UPDATE"
        );
        let existing = sqlx::query_as::<_, DeletionJobRow>(&sql)
            .bind(creator.to_string())
            .bind(lock_id.to_string())
            .fetch_optional(&mut *transaction)
            .await
            .map_err(storage_error)?;
        if let Some(row) = &existing {
            let entitlement_publication_in_progress = sqlx::query_scalar::<_, bool>(
                "SELECT EXISTS (
                    SELECT 1 FROM verification_tasks
                    WHERE deletion_job_id = $1
                      AND entitlement_publication_claim_token IS NOT NULL
                )",
            )
            .bind(row.job_id)
            .fetch_one(&mut *transaction)
            .await
            .map_err(storage_error)?;
            if entitlement_publication_in_progress {
                transaction.commit().await.map_err(storage_error)?;
                return Ok(PrepareForceDeletionResult::PublicationInProgress);
            }
        }
        let receipt = sqlx::query_as::<_, ForceDeletionReceiptRow>(
            "SELECT creator, lock_id, forced_at, frozen_content_lock, completed_at
             FROM content_lock_force_deletion_receipts
             WHERE creator = $1 AND lock_id = $2 FOR UPDATE",
        )
        .bind(creator.to_string())
        .bind(lock_id.to_string())
        .fetch_optional(&mut *transaction)
        .await
        .map_err(storage_error)?;
        if let Some(receipt) = receipt {
            transaction.commit().await.map_err(storage_error)?;
            return Ok(PrepareForceDeletionResult::Synchronous(
                row_to_force_receipt(receipt)?,
            ));
        }
        if let Some(row) = existing {
            let job = row_to_job(row)?;
            if matches!(
                job.state,
                ContentLockDeletionState::Queued | ContentLockDeletionState::Running
            ) {
                let sql = format!(
                    "UPDATE content_lock_deletion_jobs
                     SET force_requested_at = COALESCE(force_requested_at, $3),
                         state = 'queued', next_attempt_at = NULL,
                         claimed_by = NULL, claim_token = NULL, claim_expires_at = NULL,
                         updated_at = $3
                     WHERE creator = $1 AND lock_id = $2 RETURNING {ROW_COLUMNS}"
                );
                let active = sqlx::query_as::<_, DeletionJobRow>(&sql)
                    .bind(creator.to_string())
                    .bind(lock_id.to_string())
                    .bind(forced_at)
                    .fetch_one(&mut *transaction)
                    .await
                    .map_err(storage_error)?;
                transaction.commit().await.map_err(storage_error)?;
                return Ok(PrepareForceDeletionResult::Active(row_to_job(active)?));
            }
            let frozen = serde_json::to_value(&job.frozen_content_lock).map_err(storage_display)?;
            let receipt = sqlx::query_as::<_, ForceDeletionReceiptRow>(
                "INSERT INTO content_lock_force_deletion_receipts
                     (creator, lock_id, forced_at, frozen_content_lock)
                 VALUES ($1, $2, $3, $4)
                 RETURNING creator, lock_id, forced_at, frozen_content_lock, completed_at",
            )
            .bind(creator.to_string())
            .bind(lock_id.to_string())
            .bind(forced_at)
            .bind(frozen)
            .fetch_one(&mut *transaction)
            .await
            .map_err(storage_error)?;
            sqlx::query("DELETE FROM content_lock_deletion_jobs WHERE job_id = $1")
                .bind(job.job_id)
                .execute(&mut *transaction)
                .await
                .map_err(storage_error)?;
            transaction.commit().await.map_err(storage_error)?;
            return Ok(PrepareForceDeletionResult::Synchronous(
                row_to_force_receipt(receipt)?,
            ));
        }
        let frozen = frozen_content_lock
            .map(serde_json::to_value)
            .transpose()
            .map_err(storage_display)?;
        let receipt = sqlx::query_as::<_, ForceDeletionReceiptRow>(
            "INSERT INTO content_lock_force_deletion_receipts
                 (creator, lock_id, forced_at, frozen_content_lock)
             VALUES ($1, $2, $3, $4)
             RETURNING creator, lock_id, forced_at, frozen_content_lock, completed_at",
        )
        .bind(creator.to_string())
        .bind(lock_id.to_string())
        .bind(forced_at)
        .bind(frozen)
        .fetch_one(&mut *transaction)
        .await
        .map_err(storage_error)?;
        transaction.commit().await.map_err(storage_error)?;
        Ok(PrepareForceDeletionResult::Synchronous(
            row_to_force_receipt(receipt)?,
        ))
    }

    async fn get_force_receipt(
        &self,
        creator: &CreatorPubky,
        lock_id: &LockId,
    ) -> Result<Option<ForceDeletionReceipt>, ApplicationError> {
        sqlx::query_as::<_, ForceDeletionReceiptRow>(
            "SELECT creator, lock_id, forced_at, frozen_content_lock, completed_at
             FROM content_lock_force_deletion_receipts WHERE creator = $1 AND lock_id = $2",
        )
        .bind(creator.to_string())
        .bind(lock_id.to_string())
        .fetch_optional(&self.pool)
        .await
        .map_err(storage_error)?
        .map(row_to_force_receipt)
        .transpose()
    }

    async fn complete_force_deletion(
        &self,
        creator: &CreatorPubky,
        lock_id: &LockId,
        completed_at: OffsetDateTime,
    ) -> Result<bool, ApplicationError> {
        let result = sqlx::query(
            "UPDATE content_lock_force_deletion_receipts
             SET completed_at = COALESCE(completed_at, $3)
             WHERE creator = $1 AND lock_id = $2",
        )
        .bind(creator.to_string())
        .bind(lock_id.to_string())
        .bind(completed_at)
        .execute(&self.pool)
        .await
        .map_err(storage_error)?;
        Ok(result.rows_affected() == 1)
    }

    async fn has_force_receipt(
        &self,
        creator: &CreatorPubky,
        lock_id: &LockId,
    ) -> Result<bool, ApplicationError> {
        sqlx::query_scalar(
            "SELECT EXISTS (SELECT 1 FROM content_lock_force_deletion_receipts
             WHERE creator = $1 AND lock_id = $2)",
        )
        .bind(creator.to_string())
        .bind(lock_id.to_string())
        .fetch_one(&self.pool)
        .await
        .map_err(storage_error)
    }
}

async fn delete_publication_intent(
    pool: &PgPool,
    creator: &CreatorPubky,
    lock_id: &LockId,
    publication_token: Uuid,
) -> Result<bool, ApplicationError> {
    let mut transaction = pool.begin().await.map_err(storage_error)?;
    lock_proof_admission(&mut transaction, creator, lock_id).await?;
    let result = sqlx::query("DELETE FROM content_lock_publication_intents WHERE creator = $1 AND lock_id = $2 AND publication_token = $3")
        .bind(creator.to_string()).bind(lock_id.to_string()).bind(publication_token)
        .execute(&mut *transaction).await.map_err(storage_error)?;
    transaction.commit().await.map_err(storage_error)?;
    Ok(result.rows_affected() == 1)
}

fn map_publication_insert_error(error: sqlx::Error) -> ApplicationError {
    if let sqlx::Error::Database(database_error) = &error
        && database_error.is_unique_violation()
    {
        return ApplicationError::ContentLockPathConflict {
            guarded_path: "content lock publication in progress".to_owned(),
        };
    }
    storage_error(error)
}

async fn load_owned_claim(
    transaction: &mut Transaction<'_, Postgres>,
    job_id: Uuid,
    worker_id: &str,
    claim_token: Uuid,
) -> Result<Option<ContentLockDeletionJob>, ApplicationError> {
    let sql = format!(
        "WITH locked AS MATERIALIZED (
             SELECT * FROM content_lock_deletion_jobs
             WHERE job_id = $1 AND state = 'running' AND claimed_by = $2
               AND claim_token = $3
             FOR UPDATE
         ), timing AS MATERIALIZED (
             SELECT clock_timestamp() AS repository_now FROM locked
         )
         SELECT {ROW_COLUMNS} FROM locked, timing
         WHERE locked.claim_expires_at > timing.repository_now"
    );
    sqlx::query_as::<_, DeletionJobRow>(&sql)
        .bind(job_id)
        .bind(worker_id)
        .bind(claim_token)
        .fetch_optional(&mut **transaction)
        .await
        .map_err(storage_error)?
        .map(row_to_job)
        .transpose()
}

fn fetch_optional_job(
    row: Option<DeletionJobRow>,
) -> Result<Option<ContentLockDeletionJob>, ApplicationError> {
    row.map(row_to_job).transpose()
}

fn row_to_force_receipt(
    row: ForceDeletionReceiptRow,
) -> Result<ForceDeletionReceipt, ApplicationError> {
    let creator = CreatorPubky::from_str(&row.creator).map_err(storage_display)?;
    let lock_id = LockId::from_str(&row.lock_id).map_err(storage_display)?;
    let frozen_content_lock = row
        .frozen_content_lock
        .map(serde_json::from_value::<ContentLock>)
        .transpose()
        .map_err(storage_display)?;
    if let Some(content_lock) = &frozen_content_lock {
        let actual_lock_id = content_lock.lock_id().map_err(storage_display)?;
        if content_lock.creator != creator || actual_lock_id != lock_id {
            return Err(invalid_state(
                "force receipt frozen manifest identity mismatch",
            ));
        }
    }
    Ok(ForceDeletionReceipt {
        creator,
        lock_id,
        frozen_content_lock,
        forced_at: row.forced_at,
        completed_at: row.completed_at,
    })
}

fn row_to_job(row: DeletionJobRow) -> Result<ContentLockDeletionJob, ApplicationError> {
    let has_active_lease = match (
        row.claimed_by.is_some(),
        row.claim_token.is_some(),
        row.claim_expires_at.is_some(),
    ) {
        (false, false, false) => false,
        (true, true, true) => true,
        _ => return Err(invalid_state("deletion lease fields are inconsistent")),
    };
    let job = ContentLockDeletionJob {
        job_id: row.job_id,
        creator: CreatorPubky::from_str(&row.creator).map_err(storage_display)?,
        lock_id: LockId::from_str(&row.lock_id).map_err(storage_display)?,
        frozen_content_lock: serde_json::from_value::<ContentLock>(row.frozen_content_lock)
            .map_err(storage_display)?,
        deletion_started_at: row.deletion_started_at,
        state: state_from_database(&row.state)?,
        phase: phase_from_database(&row.phase)?,
        attempt_count: u32::try_from(row.attempt_count).map_err(storage_display)?,
        next_attempt_at: row.next_attempt_at,
        force_requested_at: row.force_requested_at,
        failure_code: row
            .failure_code
            .map(|code| code.parse::<ContentLockDeletionFailureCode>())
            .transpose()?,
    };
    job.validate_frozen_identity()?;
    job.validate_state(has_active_lease)?;
    Ok(job)
}

fn state_to_database(state: ContentLockDeletionState) -> &'static str {
    match state {
        ContentLockDeletionState::Queued => "queued",
        ContentLockDeletionState::Running => "running",
        ContentLockDeletionState::Completed => "completed",
        ContentLockDeletionState::Failed => "failed",
    }
}

fn state_from_database(value: &str) -> Result<ContentLockDeletionState, ApplicationError> {
    match value {
        "queued" => Ok(ContentLockDeletionState::Queued),
        "running" => Ok(ContentLockDeletionState::Running),
        "completed" => Ok(ContentLockDeletionState::Completed),
        "failed" => Ok(ContentLockDeletionState::Failed),
        _ => Err(invalid_state("unknown deletion state")),
    }
}

fn phase_to_database(phase: ContentLockDeletionPhase) -> &'static str {
    match phase {
        ContentLockDeletionPhase::Withdraw => "withdraw",
        ContentLockDeletionPhase::StartPaymentDrain => "start_payment_drain",
        ContentLockDeletionPhase::DrainPayments => "drain_payments",
        ContentLockDeletionPhase::DrainExistingCredentials => "drain_existing_credentials",
        ContentLockDeletionPhase::IssueFinalCredentials => "issue_final_credentials",
        ContentLockDeletionPhase::DrainFinalReads => "drain_final_reads",
        ContentLockDeletionPhase::DeleteContent => "delete_content",
        ContentLockDeletionPhase::DeleteTombstone => "delete_tombstone",
        ContentLockDeletionPhase::PurgeOperationalState => "purge_operational_state",
    }
}

fn phase_from_database(value: &str) -> Result<ContentLockDeletionPhase, ApplicationError> {
    match value {
        "withdraw" => Ok(ContentLockDeletionPhase::Withdraw),
        "start_payment_drain" => Ok(ContentLockDeletionPhase::StartPaymentDrain),
        "drain_payments" => Ok(ContentLockDeletionPhase::DrainPayments),
        "drain_existing_credentials" => Ok(ContentLockDeletionPhase::DrainExistingCredentials),
        "issue_final_credentials" => Ok(ContentLockDeletionPhase::IssueFinalCredentials),
        "drain_final_reads" => Ok(ContentLockDeletionPhase::DrainFinalReads),
        "delete_content" => Ok(ContentLockDeletionPhase::DeleteContent),
        "delete_tombstone" => Ok(ContentLockDeletionPhase::DeleteTombstone),
        "purge_operational_state" => Ok(ContentLockDeletionPhase::PurgeOperationalState),
        _ => Err(invalid_state("unknown deletion phase")),
    }
}

fn invalid_state(message: &str) -> ApplicationError {
    ApplicationError::InvalidContentLockDeletionState {
        message: message.to_owned(),
    }
}

fn map_insert_error(error: sqlx::Error) -> ApplicationError {
    if error
        .as_database_error()
        .is_some_and(|error| error.is_unique_violation())
    {
        ApplicationError::DuplicateRecord {
            record: "content_lock_deletion_job",
        }
    } else {
        storage_error(error)
    }
}

fn storage_error(error: sqlx::Error) -> ApplicationError {
    storage_display(error)
}

fn storage_display(error: impl std::fmt::Display) -> ApplicationError {
    ApplicationError::Storage {
        message: error.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use std::{collections::BTreeMap, str::FromStr};

    use locks_core::{
        ids::{BundleId, CreatorPubky, GuardedResourceHash, PubkyLockResource, TaskId},
        lock_policy::{
            AccessPolicy, CONTENT_LOCK_VERSION, ContentLock, GuardedResource, LockLogic,
            LockServerConfig, VerifierType,
        },
        verification::{Proof, SUBMITTED_PROOF_BUNDLE_VERSION, SubmittedProofBundle},
    };
    use time::macros::datetime;
    use uuid::Uuid;

    use sqlx::{Postgres, Transaction};

    use super::PostgresContentLockDeletionRepository;
    use crate::{
        application::{
            errors::ApplicationError,
            models::{
                ContentLockDeletionFailureCode, ContentLockDeletionJob, ContentLockDeletionPhase,
                ContentLockDeletionState, INVOICE_ADMISSION_INTENT_VERSION,
                InvoiceAdmissionIntentV1, PrepareForceDeletionResult, VerificationTaskRecord,
                VerificationTaskStatus,
            },
            ports::{
                ContentLockDeletionRepository, ContentLockOwnershipRepository,
                InvoiceAdmissionRepository, PaymentDrainRepository, VerificationTaskRepository,
            },
        },
        infrastructure::postgres::{
            PostgresContentLockOwnershipRepository, PostgresPaymentDrainRepository,
            PostgresVerificationTaskRepository, proof_admission::lock_proof_admission,
            testing::TestDatabase,
        },
    };

    const CREATOR: &str = "pubkytkrq8zmwb8a3m9k15csu3q17qmfgqnp9dskbrg9uq1rydpyxp7qy";
    const NOW: time::OffsetDateTime = datetime!(2026-08-12 05:00:00 UTC);

    #[tokio::test]
    async fn deletion_commit_order_is_the_authoritative_proof_admission_cutoff() {
        let database = TestDatabase::create().await;
        let tasks = PostgresVerificationTaskRepository::new(database.pool().clone());
        let lock = content_lock();

        let admitted_before = verification_task(&lock, BundleId::from_bytes([1; 16]));
        tasks
            .insert_verification_task(admitted_before.clone())
            .await
            .unwrap();
        let job = ContentLockDeletionJob::new(Uuid::new_v4(), lock.clone(), NOW).unwrap();
        let mut cutoff = database.pool().begin().await.unwrap();
        lock_proof_admission(&mut cutoff, &job.creator, &job.lock_id)
            .await
            .unwrap();
        let replay_repository = tasks.clone();
        let replay_task = admitted_before.clone();
        let replay = tokio::spawn(async move {
            replay_repository
                .insert_verification_task(replay_task)
                .await
        });
        wait_for_advisory_waiter(&mut cutoff).await;
        insert_deletion_cutoff(&mut cutoff, &job).await;
        cutoff.commit().await.unwrap();
        assert_eq!(
            replay.await.unwrap(),
            Err(ApplicationError::DuplicateRecord {
                record: "verification_task",
            })
        );

        let snapshotted: Vec<Uuid> = sqlx::query_scalar(
            "SELECT verification_task_id
             FROM content_lock_deletion_task_snapshot
             WHERE deletion_job_id = $1",
        )
        .bind(job.job_id)
        .fetch_all(database.pool())
        .await
        .unwrap();
        assert_eq!(snapshotted, vec![admitted_before.task_id.as_uuid()]);
        assert_eq!(
            tasks
                .insert_verification_task(admitted_before.clone())
                .await,
            Err(ApplicationError::DuplicateRecord {
                record: "verification_task",
            })
        );

        let mut changed_replay = admitted_before.clone();
        changed_replay.submitted_proof_bundle.proofs.clear();
        assert_eq!(
            tasks.insert_verification_task(changed_replay).await,
            Err(ApplicationError::VerificationTaskConflict)
        );

        let admitted_after = verification_task(&lock, BundleId::from_bytes([2; 16]));
        assert_eq!(
            tasks.insert_verification_task(admitted_after).await,
            Err(ApplicationError::ContentLockDeletionInProgress)
        );

        database.cleanup().await;
    }

    #[tokio::test]
    async fn exact_invoice_replay_precedes_cutoff_but_changed_or_new_admission_is_rejected() {
        let database = TestDatabase::create().await;
        let tasks = PostgresVerificationTaskRepository::new(database.pool().clone());
        let lock = content_lock();
        let mut admitted_before = verification_task(&lock, BundleId::from_bytes([3; 16]));
        let reader =
            CreatorPubky::from_str("pubkyorhzqdiexwmi6iidktucgud63ufa5nwtsuzdxe176a8izd6jsqky")
                .unwrap();
        admitted_before.submitted_proof_bundle.reader_public_key = Some(reader.clone());
        let intent = invoice_intent(&admitted_before, reader);
        let original = tasks
            .insert_invoice_pending_task(admitted_before.clone(), intent.clone())
            .await
            .unwrap();
        let job = ContentLockDeletionJob::new(Uuid::new_v4(), lock.clone(), NOW).unwrap();
        let mut cutoff = database.pool().begin().await.unwrap();
        lock_proof_admission(&mut cutoff, &job.creator, &job.lock_id)
            .await
            .unwrap();
        let replay_repository = tasks.clone();
        let replay_task = admitted_before.clone();
        let replay_intent = intent.clone();
        let replay = tokio::spawn(async move {
            replay_repository
                .insert_invoice_pending_task(replay_task, replay_intent)
                .await
        });
        wait_for_advisory_waiter(&mut cutoff).await;
        insert_deletion_cutoff(&mut cutoff, &job).await;
        cutoff.commit().await.unwrap();
        assert_eq!(replay.await.unwrap().unwrap(), original);
        let changed_reader =
            CreatorPubky::from_str("pubky7ir1ttte48bcp4zjychjyscicrwi1j34mtt91ptsafdbjmr8g9eo")
                .unwrap();
        let mut changed_task = admitted_before;
        changed_task.submitted_proof_bundle.reader_public_key = Some(changed_reader.clone());
        let mut changed_intent = intent;
        changed_intent.reader = changed_reader;
        assert_eq!(
            tasks
                .insert_invoice_pending_task(changed_task, changed_intent)
                .await,
            Err(ApplicationError::VerificationTaskConflict)
        );
        let mut new_task = verification_task(&lock, BundleId::from_bytes([4; 16]));
        let new_reader =
            CreatorPubky::from_str("pubkyorhzqdiexwmi6iidktucgud63ufa5nwtsuzdxe176a8izd6jsqky")
                .unwrap();
        new_task.submitted_proof_bundle.reader_public_key = Some(new_reader.clone());
        let new_intent = invoice_intent(&new_task, new_reader);
        assert_eq!(
            tasks
                .insert_invoice_pending_task(new_task, new_intent)
                .await,
            Err(ApplicationError::ContentLockDeletionInProgress)
        );

        database.cleanup().await;
    }

    #[tokio::test]
    async fn tokenless_publication_reconciliation_is_exact_fenced_and_lock_scoped() {
        let database = TestDatabase::create().await;
        let publications = PostgresContentLockDeletionRepository::new(database.pool().clone());
        let ownership = PostgresContentLockOwnershipRepository::new(database.pool().clone());
        let lock = content_lock();
        let lock_id = lock.lock_id().unwrap();
        let paths = guarded_paths(&lock);
        ownership
            .reserve_paths(&lock.creator, &paths, &lock_id)
            .await
            .unwrap();
        let publication_token = Uuid::new_v4();
        publications
            .begin_publication(&lock.creator, &lock_id, publication_token)
            .await
            .unwrap();
        assert!(
            publications
                .mark_publication_written(&lock.creator, &lock_id, publication_token)
                .await
                .unwrap()
        );
        ownership
            .mark_paths_published(&lock.creator, &paths, &lock_id)
            .await
            .unwrap();

        let mut unrelated_lock = lock.clone();
        unrelated_lock.primary_resource.as_mut().unwrap().path =
            "/priv/app.locks/content/unrelated.json".to_owned();
        let unrelated_lock_id = unrelated_lock.lock_id().unwrap();
        let unrelated_paths = guarded_paths(&unrelated_lock);
        ownership
            .reserve_paths(
                &unrelated_lock.creator,
                &unrelated_paths,
                &unrelated_lock_id,
            )
            .await
            .unwrap();
        let unrelated_publication_token = Uuid::new_v4();
        publications
            .begin_publication(
                &unrelated_lock.creator,
                &unrelated_lock_id,
                unrelated_publication_token,
            )
            .await
            .unwrap();
        assert!(
            publications
                .mark_publication_written(
                    &unrelated_lock.creator,
                    &unrelated_lock_id,
                    unrelated_publication_token,
                )
                .await
                .unwrap()
        );
        ownership
            .mark_paths_published(
                &unrelated_lock.creator,
                &unrelated_paths,
                &unrelated_lock_id,
            )
            .await
            .unwrap();

        let mut mismatched_lock = lock.clone();
        mismatched_lock
            .access_policy
            .requested_credential_ttl_seconds += 1;
        assert!(
            !publications
                .reconcile_publication(&lock.creator, &lock_id, &mismatched_lock, &paths)
                .await
                .unwrap()
        );
        assert!(
            publications
                .publication_in_progress(&lock.creator, &lock_id)
                .await
                .unwrap()
        );

        let mut fence = database.pool().begin().await.unwrap();
        lock_proof_admission(&mut fence, &lock.creator, &lock_id)
            .await
            .unwrap();
        let reconcile_repository = publications.clone();
        let reconcile_creator = lock.creator.clone();
        let reconcile_lock_id = lock_id.clone();
        let reconcile_lock = lock.clone();
        let reconcile_paths = paths.clone();
        let reconcile = tokio::spawn(async move {
            reconcile_repository
                .reconcile_publication(
                    &reconcile_creator,
                    &reconcile_lock_id,
                    &reconcile_lock,
                    &reconcile_paths,
                )
                .await
        });
        wait_for_advisory_waiter(&mut fence).await;
        fence.commit().await.unwrap();
        assert!(reconcile.await.unwrap().unwrap());
        assert!(
            !publications
                .publication_in_progress(&lock.creator, &lock_id)
                .await
                .unwrap()
        );
        assert!(
            publications
                .publication_in_progress(&unrelated_lock.creator, &unrelated_lock_id)
                .await
                .unwrap()
        );

        let mut ownership_race_lock = lock.clone();
        ownership_race_lock.primary_resource.as_mut().unwrap().path =
            "/priv/app.locks/content/ownership-race.json".to_owned();
        let ownership_race_lock_id = ownership_race_lock.lock_id().unwrap();
        let ownership_race_paths = guarded_paths(&ownership_race_lock);
        ownership
            .reserve_paths(
                &ownership_race_lock.creator,
                &ownership_race_paths,
                &ownership_race_lock_id,
            )
            .await
            .unwrap();
        let ownership_race_publication_token = Uuid::new_v4();
        publications
            .begin_publication(
                &ownership_race_lock.creator,
                &ownership_race_lock_id,
                ownership_race_publication_token,
            )
            .await
            .unwrap();
        assert!(
            publications
                .mark_publication_written(
                    &ownership_race_lock.creator,
                    &ownership_race_lock_id,
                    ownership_race_publication_token,
                )
                .await
                .unwrap()
        );
        ownership
            .mark_paths_published(
                &ownership_race_lock.creator,
                &ownership_race_paths,
                &ownership_race_lock_id,
            )
            .await
            .unwrap();
        let mut fence = database.pool().begin().await.unwrap();
        lock_proof_admission(
            &mut fence,
            &ownership_race_lock.creator,
            &ownership_race_lock_id,
        )
        .await
        .unwrap();
        let reconcile_repository = publications.clone();
        let reconcile_creator = ownership_race_lock.creator.clone();
        let reconcile_lock_id = ownership_race_lock_id.clone();
        let reconcile_lock = ownership_race_lock.clone();
        let reconcile_paths = ownership_race_paths.clone();
        let reconcile = tokio::spawn(async move {
            reconcile_repository
                .reconcile_publication(
                    &reconcile_creator,
                    &reconcile_lock_id,
                    &reconcile_lock,
                    &reconcile_paths,
                )
                .await
        });
        wait_for_advisory_waiter(&mut fence).await;
        sqlx::query(
            "UPDATE content_lock_ownership
             SET status = 'reserved'
             WHERE creator = $1 AND lock_id = $2",
        )
        .bind(ownership_race_lock.creator.to_string())
        .bind(ownership_race_lock_id.to_string())
        .execute(&mut *fence)
        .await
        .unwrap();
        fence.commit().await.unwrap();
        assert!(!reconcile.await.unwrap().unwrap());
        assert!(
            publications
                .publication_in_progress(&ownership_race_lock.creator, &ownership_race_lock_id,)
                .await
                .unwrap()
        );

        database.cleanup().await;
    }

    #[tokio::test]
    async fn concurrent_deletion_and_new_bundle_have_one_serialized_cutoff_order() {
        for iteration in 0..20_u8 {
            let database = TestDatabase::create().await;
            let deletions = PostgresContentLockDeletionRepository::new(database.pool().clone());
            let tasks = PostgresVerificationTaskRepository::new(database.pool().clone());
            let lock = content_lock();
            let task = verification_task(&lock, BundleId::from_bytes([iteration; 16]));
            let job = ContentLockDeletionJob::new(Uuid::new_v4(), lock, NOW).unwrap();

            let (deletion_result, task_result) = tokio::join!(
                deletions.insert_job(job.clone()),
                tasks.insert_verification_task(task.clone())
            );
            deletion_result.unwrap();

            let snapshotted = sqlx::query_scalar::<_, bool>(
                "SELECT EXISTS (
                    SELECT 1 FROM content_lock_deletion_task_snapshot
                    WHERE deletion_job_id = $1 AND verification_task_id = $2
                )",
            )
            .bind(job.job_id)
            .bind(task.task_id.as_uuid())
            .fetch_one(database.pool())
            .await
            .unwrap();
            match task_result {
                Ok(()) => assert!(snapshotted),
                Err(ApplicationError::ContentLockDeletionInProgress) => assert!(!snapshotted),
                other => panic!("unexpected concurrent admission result: {other:?}"),
            }

            database.cleanup().await;
        }
    }

    #[tokio::test]
    async fn concurrent_graceful_start_and_force_prepare_leave_exactly_one_durable_mode() {
        for _ in 0..20 {
            let database = TestDatabase::create().await;
            let repository = PostgresContentLockDeletionRepository::new(database.pool().clone());
            let job = ContentLockDeletionJob::new(Uuid::new_v4(), content_lock(), NOW).unwrap();

            let (graceful, force) = tokio::join!(
                repository.insert_job(job.clone()),
                repository.prepare_force_deletion(&job.creator, &job.lock_id, None, NOW)
            );
            let persisted_job = repository
                .get_job(&job.creator, &job.lock_id)
                .await
                .unwrap();
            let receipt_exists = repository
                .has_force_receipt(&job.creator, &job.lock_id)
                .await
                .unwrap();

            match (graceful, force) {
                (Ok(()), Ok(PrepareForceDeletionResult::Active(active))) => {
                    assert_eq!(active.job_id, job.job_id);
                    assert!(active.force_requested_at.is_some());
                    assert_eq!(persisted_job, Some(active));
                    assert!(!receipt_exists);
                }
                (
                    Err(ApplicationError::ContentLockDeletionInProgress),
                    Ok(PrepareForceDeletionResult::Synchronous(receipt)),
                ) => {
                    assert!(persisted_job.is_none());
                    assert!(receipt_exists);
                    assert!(receipt.frozen_content_lock.is_none());
                    assert!(receipt.completed_at.is_none());
                }
                other => panic!("unexpected graceful/force race result: {other:?}"),
            }

            database.cleanup().await;
        }
    }

    #[tokio::test]
    async fn publication_intent_and_force_receipt_have_one_serialized_cutoff_order() {
        let database = TestDatabase::create().await;
        let repository = PostgresContentLockDeletionRepository::new(database.pool().clone());
        let lock = content_lock();
        let lock_id = lock.lock_id().unwrap();
        let token = Uuid::new_v4();

        repository
            .begin_publication(&lock.creator, &lock_id, token)
            .await
            .unwrap();
        assert_eq!(
            repository
                .prepare_force_deletion(&lock.creator, &lock_id, None, NOW)
                .await
                .unwrap(),
            PrepareForceDeletionResult::PublicationInProgress
        );
        assert!(
            !repository
                .has_force_receipt(&lock.creator, &lock_id)
                .await
                .unwrap()
        );
        assert!(
            repository
                .finish_publication(&lock.creator, &lock_id, token)
                .await
                .unwrap()
        );
        assert!(matches!(
            repository
                .prepare_force_deletion(&lock.creator, &lock_id, None, NOW)
                .await
                .unwrap(),
            PrepareForceDeletionResult::Synchronous(receipt)
                if receipt.frozen_content_lock.is_none() && receipt.completed_at.is_none()
        ));
        assert_eq!(
            repository
                .begin_publication(&lock.creator, &lock_id, Uuid::new_v4())
                .await,
            Err(ApplicationError::ContentLockDeletionInProgress)
        );

        database.cleanup().await;
    }

    #[tokio::test]
    async fn deletion_preserves_live_invoice_admission_claim_until_window_is_persisted() {
        let database = TestDatabase::create().await;
        let lock = content_lock();
        let task = paykit_verification_task(&lock, BundleId::from_bytes([3; 16]));
        let tasks = PostgresVerificationTaskRepository::new(database.pool().clone());
        tasks
            .insert_invoice_pending_task(
                task.clone(),
                invoice_intent(
                    &task,
                    task.submitted_proof_bundle
                        .reader_public_key
                        .clone()
                        .unwrap(),
                ),
            )
            .await
            .unwrap();
        let admission_claim = tasks
            .claim_next_invoice_admission("invoice-worker", NOW, time::Duration::minutes(1))
            .await
            .unwrap()
            .unwrap();

        let repository = PostgresContentLockDeletionRepository::new(database.pool().clone());
        let job = ContentLockDeletionJob::new(Uuid::new_v4(), lock, NOW).unwrap();
        repository.insert_job(job.clone()).await.unwrap();
        let claim = repository
            .claim_next("worker", NOW, NOW + time::Duration::seconds(60))
            .await
            .unwrap()
            .unwrap();
        assert!(
            repository
                .advance_phase(
                    claim.job.job_id,
                    "worker",
                    claim.claim_token,
                    NOW,
                    ContentLockDeletionPhase::StartPaymentDrain,
                )
                .await
                .is_err()
        );
        assert!(
            tasks
                .mark_invoice_admission_ready(
                    &task.task_id,
                    "invoice-worker",
                    &admission_claim.claim_token,
                    NOW,
                    NOW,
                    NOW,
                )
                .await
                .is_err()
        );
        let ready = tasks
            .mark_invoice_admission_ready(
                &task.task_id,
                "invoice-worker",
                &admission_claim.claim_token,
                NOW,
                NOW,
                NOW + time::Duration::hours(24),
            )
            .await
            .unwrap();
        assert!(ready.is_some());

        let advanced = repository
            .advance_phase(
                claim.job.job_id,
                "worker",
                claim.claim_token,
                NOW,
                ContentLockDeletionPhase::StartPaymentDrain,
            )
            .await
            .unwrap()
            .unwrap();
        assert_eq!(advanced.phase, ContentLockDeletionPhase::StartPaymentDrain);

        let window: (time::OffsetDateTime, time::OffsetDateTime) = sqlx::query_as(
            "SELECT invoice_created_at, payment_deadline
             FROM content_lock_deletion_task_snapshot
             WHERE deletion_job_id = $1 AND verification_task_id = $2",
        )
        .bind(job.job_id)
        .bind(task.task_id.as_uuid())
        .fetch_one(database.pool())
        .await
        .unwrap();
        assert_eq!(window, (NOW, NOW + time::Duration::hours(24)));

        database.cleanup().await;
    }

    #[tokio::test]
    async fn deletion_preserves_unclaimed_invoice_admission_retry_schedule() {
        let database = TestDatabase::create().await;
        let lock = content_lock();
        let task = paykit_verification_task(&lock, BundleId::from_bytes([4; 16]));
        let tasks = PostgresVerificationTaskRepository::new(database.pool().clone());
        tasks
            .insert_invoice_pending_task(
                task.clone(),
                invoice_intent(
                    &task,
                    task.submitted_proof_bundle
                        .reader_public_key
                        .clone()
                        .unwrap(),
                ),
            )
            .await
            .unwrap();
        PostgresContentLockDeletionRepository::new(database.pool().clone())
            .insert_job(ContentLockDeletionJob::new(Uuid::new_v4(), lock, NOW).unwrap())
            .await
            .unwrap();

        let claim = tasks
            .claim_next_invoice_admission("invoice-worker", NOW, time::Duration::minutes(1))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(claim.admission.task.task_id, task.task_id);

        database.cleanup().await;
    }

    #[tokio::test]
    async fn failed_invoice_admission_before_or_after_cutoff_does_not_block_payment_drain() {
        for fail_before_cutoff in [true, false] {
            let database = TestDatabase::create().await;
            let lock = content_lock();
            let task = paykit_verification_task(
                &lock,
                BundleId::from_bytes([if fail_before_cutoff { 5 } else { 6 }; 16]),
            );
            let tasks = PostgresVerificationTaskRepository::new(database.pool().clone());
            tasks
                .insert_invoice_pending_task(
                    task.clone(),
                    invoice_intent(
                        &task,
                        task.submitted_proof_bundle
                            .reader_public_key
                            .clone()
                            .unwrap(),
                    ),
                )
                .await
                .unwrap();
            let admission_claim = tasks
                .claim_next_invoice_admission("invoice-worker", NOW, time::Duration::minutes(1))
                .await
                .unwrap()
                .unwrap();
            let repository = PostgresContentLockDeletionRepository::new(database.pool().clone());
            let job = ContentLockDeletionJob::new(Uuid::new_v4(), lock, NOW).unwrap();

            if fail_before_cutoff {
                tasks
                    .mark_invoice_admission_failed(
                        &task.task_id,
                        "invoice-worker",
                        &admission_claim.claim_token,
                        NOW,
                        "invoice admission failed",
                    )
                    .await
                    .unwrap()
                    .unwrap();
            }
            repository.insert_job(job.clone()).await.unwrap();
            if !fail_before_cutoff {
                tasks
                    .mark_invoice_admission_failed(
                        &task.task_id,
                        "invoice-worker",
                        &admission_claim.claim_token,
                        NOW,
                        "invoice admission failed",
                    )
                    .await
                    .unwrap()
                    .unwrap();
            }

            let claim = repository
                .claim_next("worker", NOW, NOW + time::Duration::minutes(1))
                .await
                .unwrap()
                .unwrap();
            let advanced = repository
                .advance_phase(
                    claim.job.job_id,
                    "worker",
                    claim.claim_token,
                    NOW,
                    ContentLockDeletionPhase::StartPaymentDrain,
                )
                .await
                .unwrap()
                .unwrap();
            assert_eq!(advanced.phase, ContentLockDeletionPhase::StartPaymentDrain);
            let resolved_status: Option<String> = sqlx::query_scalar(
                "SELECT resolved_status FROM content_lock_deletion_task_snapshot
                 WHERE deletion_job_id = $1 AND verification_task_id = $2",
            )
            .bind(job.job_id)
            .bind(task.task_id.as_uuid())
            .fetch_one(database.pool())
            .await
            .unwrap();
            assert_eq!(resolved_status.as_deref(), Some("failed"));
            let drains = PostgresPaymentDrainRepository::new(database.pool().clone());
            assert!(
                drains
                    .list_obligations(job.job_id)
                    .await
                    .unwrap()
                    .is_empty()
            );
            assert!(drains.all_obligations_terminal(job.job_id).await.unwrap());

            database.cleanup().await;
        }
    }

    #[tokio::test]
    async fn persists_and_fences_the_full_job_lifecycle_across_repository_recreation() {
        let database = TestDatabase::create().await;
        let repository = PostgresContentLockDeletionRepository::new(database.pool().clone());
        let job = ContentLockDeletionJob::new(Uuid::new_v4(), content_lock(), NOW).unwrap();
        repository.insert_job(job.clone()).await.unwrap();

        let reopened = PostgresContentLockDeletionRepository::new(database.pool().clone());
        assert_eq!(
            reopened.get_job(&job.creator, &job.lock_id).await.unwrap(),
            Some(job.clone())
        );
        assert!(reopened.insert_job(job.clone()).await.is_err());
        let mut distinct_lock = content_lock();
        distinct_lock.access_policy.requested_credential_ttl_seconds = 901;
        let mut distinct_job =
            ContentLockDeletionJob::new(Uuid::new_v4(), distinct_lock, NOW).unwrap();
        distinct_job.job_id = job.job_id;
        assert!(reopened.insert_job(distinct_job).await.is_err());

        let first = reopened
            .claim_next("worker-a", NOW, datetime!(2026-08-12 05:05:00 UTC))
            .await
            .unwrap()
            .unwrap();
        assert!(
            reopened
                .claim_next("worker-b", NOW, datetime!(2026-08-12 05:05:00 UTC),)
                .await
                .unwrap()
                .is_none()
        );
        sqlx::query(
            "UPDATE content_lock_deletion_jobs
             SET claim_expires_at = clock_timestamp()
             WHERE job_id = $1",
        )
        .bind(job.job_id)
        .execute(database.pool())
        .await
        .unwrap();
        let reclaimed = reopened
            .claim_next(
                "worker-b",
                datetime!(2026-08-12 05:05:01 UTC),
                datetime!(2026-08-12 05:10:00 UTC),
            )
            .await
            .unwrap()
            .unwrap();
        assert_ne!(first.claim_token, reclaimed.claim_token);
        assert!(
            reopened
                .schedule_retry(
                    job.job_id,
                    "worker-a",
                    first.claim_token,
                    datetime!(2026-08-12 05:05:01 UTC),
                    datetime!(2026-08-12 05:06:00 UTC),
                )
                .await
                .unwrap()
                .is_none()
        );
        assert!(
            reopened
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
        assert!(
            reopened
                .finish(
                    job.job_id,
                    "worker-a",
                    first.claim_token,
                    datetime!(2026-08-12 05:05:01 UTC),
                    Some(ContentLockDeletionFailureCode::StateCorrupt),
                )
                .await
                .unwrap()
                .is_none()
        );

        let advanced = reopened
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
        assert_eq!(advanced.attempt_count, 0);

        let final_claim = reopened
            .claim_next(
                "worker-c",
                datetime!(2026-08-12 05:06:01 UTC),
                datetime!(2026-08-12 05:11:00 UTC),
            )
            .await
            .unwrap()
            .unwrap();
        let failed = reopened
            .finish(
                job.job_id,
                "worker-c",
                final_claim.claim_token,
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

        let prepared = reopened
            .prepare_force_deletion(&job.creator, &job.lock_id, None, NOW)
            .await
            .unwrap();
        assert!(matches!(
            prepared,
            PrepareForceDeletionResult::Synchronous(ref receipt)
                if receipt.frozen_content_lock == Some(job.frozen_content_lock.clone())
                    && receipt.completed_at.is_none()
        ));
        let reopened_after_force =
            PostgresContentLockDeletionRepository::new(database.pool().clone());
        assert!(matches!(
            reopened_after_force
                .prepare_force_deletion(&job.creator, &job.lock_id, None, NOW)
                .await
                .unwrap(),
            PrepareForceDeletionResult::Synchronous(receipt)
                if receipt.frozen_content_lock == Some(job.frozen_content_lock.clone())
                    && receipt.completed_at.is_none()
        ));
        assert!(
            reopened_after_force
                .has_force_receipt(&job.creator, &job.lock_id)
                .await
                .unwrap()
        );
        assert!(
            reopened_after_force
                .get_job(&job.creator, &job.lock_id)
                .await
                .unwrap()
                .is_none()
        );
        assert!(
            reopened_after_force
                .complete_force_deletion(&job.creator, &job.lock_id, NOW)
                .await
                .unwrap()
        );
        let reopened_after_completion =
            PostgresContentLockDeletionRepository::new(database.pool().clone());
        let completed = reopened_after_completion
            .get_force_receipt(&job.creator, &job.lock_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            completed.frozen_content_lock,
            Some(job.frozen_content_lock.clone())
        );
        assert_eq!(completed.completed_at, Some(NOW));

        database.cleanup().await;
    }

    #[tokio::test]
    async fn concurrent_claims_return_a_job_once() {
        let database = TestDatabase::create().await;
        let repository = PostgresContentLockDeletionRepository::new(database.pool().clone());
        let job = ContentLockDeletionJob::new(Uuid::new_v4(), content_lock(), NOW).unwrap();
        repository.insert_job(job).await.unwrap();

        let (left, right) = tokio::join!(
            repository.claim_next("worker-a", NOW, datetime!(2026-08-12 05:05:00 UTC)),
            repository.claim_next("worker-b", NOW, datetime!(2026-08-12 05:05:00 UTC)),
        );
        assert_eq!(
            usize::from(left.unwrap().is_some()) + usize::from(right.unwrap().is_some()),
            1
        );

        database.cleanup().await;
    }

    #[tokio::test]
    async fn force_escalation_invalidates_the_active_claim_and_requeues_for_force_processing() {
        let database = TestDatabase::create().await;
        let repository = PostgresContentLockDeletionRepository::new(database.pool().clone());
        let job = ContentLockDeletionJob::new(Uuid::new_v4(), content_lock(), NOW).unwrap();
        repository.insert_job(job.clone()).await.unwrap();
        let claimed = repository
            .claim_next("graceful-worker", NOW, NOW + time::Duration::minutes(1))
            .await
            .unwrap()
            .unwrap();

        let escalated = repository
            .prepare_force_deletion(&job.creator, &job.lock_id, None, NOW)
            .await
            .unwrap();
        let PrepareForceDeletionResult::Active(escalated) = escalated else {
            panic!("active job must be escalated asynchronously");
        };
        assert_eq!(escalated.state, ContentLockDeletionState::Queued);
        assert!(escalated.force_requested_at.is_some());

        assert_eq!(
            repository
                .schedule_retry(
                    job.job_id,
                    "graceful-worker",
                    claimed.claim_token,
                    NOW,
                    NOW + time::Duration::seconds(1),
                )
                .await
                .unwrap(),
            None
        );
        assert_eq!(
            repository
                .advance_phase(
                    job.job_id,
                    "graceful-worker",
                    claimed.claim_token,
                    NOW,
                    ContentLockDeletionPhase::StartPaymentDrain,
                )
                .await
                .unwrap(),
            None
        );
        assert_eq!(
            repository
                .finish(
                    job.job_id,
                    "graceful-worker",
                    claimed.claim_token,
                    NOW,
                    None,
                )
                .await
                .unwrap(),
            None
        );

        let force_claim = repository
            .claim_next("force-worker", NOW, NOW + time::Duration::minutes(1))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(force_claim.job.job_id, job.job_id);
        assert!(force_claim.job.force_requested_at.is_some());
        assert_ne!(force_claim.claim_token, claimed.claim_token);

        database.cleanup().await;
    }

    #[tokio::test]
    async fn claim_reclaims_at_equality_and_derives_expiry_from_repository_time() {
        let database = TestDatabase::create().await;
        let repository = PostgresContentLockDeletionRepository::new(database.pool().clone());
        let job = ContentLockDeletionJob::new(Uuid::new_v4(), content_lock(), NOW).unwrap();
        repository.insert_job(job.clone()).await.unwrap();
        let first = repository
            .claim_next("worker-a", NOW, NOW + time::Duration::minutes(5))
            .await
            .unwrap()
            .unwrap();
        sqlx::query(
            "UPDATE content_lock_deletion_jobs
             SET claim_expires_at = clock_timestamp()
             WHERE job_id = $1",
        )
        .bind(job.job_id)
        .execute(database.pool())
        .await
        .unwrap();

        let reclaimed = repository
            .claim_next("worker-b", NOW, NOW + time::Duration::minutes(5))
            .await
            .unwrap()
            .unwrap();

        assert_ne!(first.claim_token, reclaimed.claim_token);
        assert!(
            sqlx::query_scalar::<_, bool>(
                "SELECT claim_expires_at > clock_timestamp() + INTERVAL '4 minutes'
             FROM content_lock_deletion_jobs WHERE job_id = $1",
            )
            .bind(job.job_id)
            .fetch_one(database.pool())
            .await
            .unwrap()
        );
        database.cleanup().await;
    }

    #[tokio::test]
    async fn retry_advance_and_finish_are_fenced_when_row_lock_wait_crosses_expiry() {
        let database = TestDatabase::create().await;
        let repository = PostgresContentLockDeletionRepository::new(database.pool().clone());
        let job = ContentLockDeletionJob::new(Uuid::new_v4(), content_lock(), NOW).unwrap();
        repository.insert_job(job.clone()).await.unwrap();
        let claimed = repository
            .claim_next("worker-a", NOW, NOW + time::Duration::minutes(5))
            .await
            .unwrap()
            .unwrap();

        set_short_lease(database.pool(), job.job_id).await;
        let blocker = lock_job_row(database.pool(), job.job_id).await;
        let retry_repository = repository.clone();
        let mut retry = tokio::spawn(async move {
            retry_repository
                .schedule_retry(
                    job.job_id,
                    "worker-a",
                    claimed.claim_token,
                    NOW,
                    NOW + time::Duration::minutes(1),
                )
                .await
        });
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(50), &mut retry)
                .await
                .is_err(),
            "retry must remain blocked while the row lock is held"
        );
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
        blocker.commit().await.unwrap();
        assert!(retry.await.unwrap().unwrap().is_none());

        set_short_lease(database.pool(), job.job_id).await;
        let blocker = lock_job_row(database.pool(), job.job_id).await;
        let advance_repository = repository.clone();
        let mut advance = tokio::spawn(async move {
            advance_repository
                .advance_phase(
                    job.job_id,
                    "worker-a",
                    claimed.claim_token,
                    NOW,
                    ContentLockDeletionPhase::StartPaymentDrain,
                )
                .await
        });
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(50), &mut advance)
                .await
                .is_err(),
            "advance must remain blocked while the row lock is held"
        );
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
        blocker.commit().await.unwrap();
        assert!(advance.await.unwrap().unwrap().is_none());

        set_short_lease(database.pool(), job.job_id).await;
        let blocker = lock_job_row(database.pool(), job.job_id).await;
        let finish_repository = repository.clone();
        let mut finish = tokio::spawn(async move {
            finish_repository
                .finish(job.job_id, "worker-a", claimed.claim_token, NOW, None)
                .await
        });
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(50), &mut finish)
                .await
                .is_err(),
            "finish must remain blocked while the row lock is held"
        );
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
        blocker.commit().await.unwrap();
        assert!(finish.await.unwrap().unwrap().is_none());

        database.cleanup().await;
    }

    #[tokio::test]
    async fn snapshot_waits_for_terminal_worker_and_captures_one_consistent_task_version() {
        let database = TestDatabase::create().await;
        let tasks = PostgresVerificationTaskRepository::new(database.pool().clone());
        let lock = content_lock();
        let task = verification_task(&lock, BundleId::from_bytes([7; 16]));
        tasks.insert_verification_task(task.clone()).await.unwrap();
        let job = ContentLockDeletionJob::new(Uuid::new_v4(), lock, NOW).unwrap();

        let mut worker = database.pool().begin().await.unwrap();
        sqlx::query("SELECT task_id FROM verification_tasks WHERE task_id = $1 FOR UPDATE")
            .bind(task.task_id.as_uuid())
            .fetch_one(&mut *worker)
            .await
            .unwrap();
        let repository = PostgresContentLockDeletionRepository::new(database.pool().clone());
        let inserted_job = job.clone();
        let insertion = tokio::spawn(async move { repository.insert_job(inserted_job).await });
        wait_for_blocked_connection(&mut worker).await;
        sqlx::query(
            "UPDATE verification_tasks
             SET status = 'failed', started_at = $2, completed_at = $2,
                 failure_message = 'worker failure', updated_at = $2
             WHERE task_id = $1 AND deletion_job_id IS NULL",
        )
        .bind(task.task_id.as_uuid())
        .bind(NOW)
        .execute(&mut *worker)
        .await
        .unwrap();
        worker.commit().await.unwrap();
        insertion.await.unwrap().unwrap();

        let (status_at_cutoff, deletion_job_id): (String, Option<Uuid>) = sqlx::query_as(
            "SELECT snapshot.status_at_cutoff, task.deletion_job_id
             FROM content_lock_deletion_task_snapshot AS snapshot
             JOIN verification_tasks AS task
               ON task.task_id = snapshot.verification_task_id
             WHERE snapshot.deletion_job_id = $1 AND task.task_id = $2",
        )
        .bind(job.job_id)
        .bind(task.task_id.as_uuid())
        .fetch_one(database.pool())
        .await
        .unwrap();
        assert_eq!(status_at_cutoff, "failed");
        assert_eq!(deletion_job_id, Some(job.job_id));

        database.cleanup().await;
    }

    #[tokio::test]
    async fn concurrent_force_waits_for_publication_marker_then_retries_after_terminalization() {
        let database = TestDatabase::create().await;
        let tasks = PostgresVerificationTaskRepository::new(database.pool().clone());
        let lock = content_lock();
        let task = verification_task(&lock, BundleId::from_bytes([8; 16]));
        tasks.insert_verification_task(task.clone()).await.unwrap();
        let repository = PostgresContentLockDeletionRepository::new(database.pool().clone());
        let job = ContentLockDeletionJob::new(Uuid::new_v4(), lock, NOW).unwrap();
        repository.insert_job(job.clone()).await.unwrap();
        let mut publication = database.pool().begin().await.unwrap();
        sqlx::query("SELECT job_id FROM content_lock_deletion_jobs WHERE job_id = $1 FOR UPDATE")
            .bind(job.job_id)
            .fetch_one(&mut *publication)
            .await
            .unwrap();
        sqlx::query(
            "UPDATE verification_tasks SET status = 'publishing_entitlement',
             started_at = $3, entitlement_to_publish = '{}'::jsonb,
             entitlement_publication_claim_token = $2 WHERE task_id = $1",
        )
        .bind(task.task_id.as_uuid())
        .bind(Uuid::new_v4())
        .bind(NOW)
        .execute(&mut *publication)
        .await
        .unwrap();

        let force_repository = repository.clone();
        let force_creator = job.creator.clone();
        let force_lock_id = job.lock_id.clone();
        let force = tokio::spawn(async move {
            force_repository
                .prepare_force_deletion(&force_creator, &force_lock_id, None, NOW)
                .await
        });
        wait_for_blocked_connection(&mut publication).await;
        assert!(
            repository
                .get_force_receipt(&job.creator, &job.lock_id)
                .await
                .unwrap()
                .is_none()
        );
        publication.commit().await.unwrap();
        assert_eq!(
            force.await.unwrap().unwrap(),
            PrepareForceDeletionResult::PublicationInProgress
        );
        assert!(
            repository
                .get_force_receipt(&job.creator, &job.lock_id)
                .await
                .unwrap()
                .is_none()
        );
        sqlx::query(
            "UPDATE verification_tasks SET status = 'completed',
             completed_at = $2, entitlement_publication_claim_token = NULL WHERE task_id = $1",
        )
        .bind(task.task_id.as_uuid())
        .bind(NOW)
        .execute(database.pool())
        .await
        .unwrap();
        assert!(matches!(
            repository
                .prepare_force_deletion(&job.creator, &job.lock_id, None, NOW)
                .await
                .unwrap(),
            PrepareForceDeletionResult::Active(_)
        ));
        database.cleanup().await;
    }

    #[tokio::test]
    async fn force_of_terminal_job_deletes_job_and_clears_task_ownership() {
        let database = TestDatabase::create().await;
        let tasks = PostgresVerificationTaskRepository::new(database.pool().clone());
        let lock = content_lock();
        let task = verification_task(&lock, BundleId::from_bytes([9; 16]));
        tasks.insert_verification_task(task.clone()).await.unwrap();
        let repository = PostgresContentLockDeletionRepository::new(database.pool().clone());
        let job = ContentLockDeletionJob::new(Uuid::new_v4(), lock, NOW).unwrap();
        repository.insert_job(job.clone()).await.unwrap();
        let claim = repository
            .claim_next("worker", NOW, NOW + time::Duration::minutes(1))
            .await
            .unwrap()
            .unwrap();
        repository
            .finish(
                job.job_id,
                "worker",
                claim.claim_token,
                NOW,
                Some(ContentLockDeletionFailureCode::StateCorrupt),
            )
            .await
            .unwrap()
            .unwrap();

        assert!(matches!(
            repository
                .prepare_force_deletion(&job.creator, &job.lock_id, None, NOW)
                .await
                .unwrap(),
            PrepareForceDeletionResult::Synchronous(_)
        ));
        assert!(
            repository
                .get_job(&job.creator, &job.lock_id)
                .await
                .unwrap()
                .is_none()
        );
        let owner: Option<Uuid> =
            sqlx::query_scalar("SELECT deletion_job_id FROM verification_tasks WHERE task_id = $1")
                .bind(task.task_id.as_uuid())
                .fetch_one(database.pool())
                .await
                .unwrap();
        assert_eq!(owner, None);
        database.cleanup().await;
    }

    #[tokio::test]
    async fn read_rejects_corrupt_frozen_manifest_identity() {
        let database = TestDatabase::create().await;
        let repository = PostgresContentLockDeletionRepository::new(database.pool().clone());
        let job = ContentLockDeletionJob::new(Uuid::new_v4(), content_lock(), NOW).unwrap();
        repository.insert_job(job.clone()).await.unwrap();
        sqlx::query(
            "UPDATE content_lock_deletion_jobs
             SET frozen_content_lock = jsonb_set(
                 frozen_content_lock,
                 '{access_policy,requested_credential_ttl_seconds}',
                 '901'::jsonb
             )
             WHERE job_id = $1",
        )
        .bind(job.job_id)
        .execute(database.pool())
        .await
        .unwrap();

        assert!(
            repository
                .get_job(&job.creator, &job.lock_id)
                .await
                .is_err()
        );

        database.cleanup().await;
    }

    async fn wait_for_advisory_waiter(transaction: &mut Transaction<'_, Postgres>) {
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            loop {
                let waiter_exists = sqlx::query_scalar::<_, bool>(
                    "SELECT EXISTS (
                        SELECT 1
                        FROM pg_locks held
                        JOIN pg_locks waiting
                          ON waiting.locktype = held.locktype
                         AND waiting.database IS NOT DISTINCT FROM held.database
                         AND waiting.classid IS NOT DISTINCT FROM held.classid
                         AND waiting.objid IS NOT DISTINCT FROM held.objid
                         AND waiting.objsubid IS NOT DISTINCT FROM held.objsubid
                         AND waiting.pid <> held.pid
                        WHERE held.pid = pg_backend_pid()
                          AND held.locktype = 'advisory'
                          AND held.granted
                          AND NOT waiting.granted
                    )",
                )
                .fetch_one(&mut **transaction)
                .await
                .unwrap();
                if waiter_exists {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("second Postgres connection must wait on the canonical advisory fence");
    }

    async fn wait_for_blocked_connection(transaction: &mut Transaction<'_, Postgres>) {
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            loop {
                let waiter_exists = sqlx::query_scalar::<_, bool>(
                    "SELECT EXISTS (
                        SELECT 1 FROM pg_stat_activity
                        WHERE pg_backend_pid() = ANY(pg_blocking_pids(pid))
                    )",
                )
                .fetch_one(&mut **transaction)
                .await
                .unwrap();
                if waiter_exists {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("deletion admission must wait for the worker-owned task row");
    }

    async fn set_short_lease(pool: &sqlx::PgPool, job_id: Uuid) {
        sqlx::query(
            "UPDATE content_lock_deletion_jobs
             SET claim_expires_at = clock_timestamp() + INTERVAL '200 milliseconds'
             WHERE job_id = $1",
        )
        .bind(job_id)
        .execute(pool)
        .await
        .unwrap();
    }

    async fn lock_job_row(pool: &sqlx::PgPool, job_id: Uuid) -> Transaction<'_, Postgres> {
        let mut transaction = pool.begin().await.unwrap();
        sqlx::query("SELECT job_id FROM content_lock_deletion_jobs WHERE job_id = $1 FOR UPDATE")
            .bind(job_id)
            .fetch_one(&mut *transaction)
            .await
            .unwrap();
        transaction
    }

    async fn insert_deletion_cutoff(
        transaction: &mut Transaction<'_, Postgres>,
        job: &ContentLockDeletionJob,
    ) {
        sqlx::query(
            "INSERT INTO content_lock_deletion_jobs
             (job_id, creator, lock_id, frozen_content_lock, deletion_started_at)
             VALUES ($1, $2, $3, $4, $5)",
        )
        .bind(job.job_id)
        .bind(job.creator.to_string())
        .bind(job.lock_id.to_string())
        .bind(serde_json::to_value(&job.frozen_content_lock).unwrap())
        .bind(job.deletion_started_at)
        .execute(&mut **transaction)
        .await
        .unwrap();
        let lock_resource = format!(
            "{}/pub/app.locks/{}.json",
            job.creator,
            job.lock_id.as_str()
        );
        sqlx::query(
            "INSERT INTO content_lock_deletion_task_snapshot
                 (deletion_job_id, verification_task_id)
             SELECT $1, task_id
             FROM verification_tasks
             WHERE creator = $2
               AND submitted_proof_bundle->>'pubky_lock_resource' = $3",
        )
        .bind(job.job_id)
        .bind(job.creator.to_string())
        .bind(lock_resource)
        .execute(&mut **transaction)
        .await
        .unwrap();
    }

    fn guarded_paths(lock: &ContentLock) -> Vec<String> {
        lock.primary_resource
            .iter()
            .map(|resource| resource.path.clone())
            .chain(lock.secondary_resources.keys().cloned())
            .collect()
    }

    fn verification_task(lock: &ContentLock, bundle_id: BundleId) -> VerificationTaskRecord {
        let lock_resource = PubkyLockResource::from_str(&format!(
            "{}/pub/app.locks/{}.json",
            lock.creator,
            lock.lock_id().unwrap()
        ))
        .unwrap();
        VerificationTaskRecord {
            task_id: TaskId::from_str(&Uuid::new_v4().to_string()).unwrap(),
            creator: lock.creator.clone(),
            submitted_proof_bundle: SubmittedProofBundle {
                version: SUBMITTED_PROOF_BUNDLE_VERSION,
                bundle_id,
                pubky_lock_resource: lock_resource,
                reader_public_key: None,
                proofs: vec![Proof {
                    criterion_id: "criterion-1".to_owned(),
                    verifier_type: VerifierType::DevStatic,
                    payload: serde_json::json!({"satisfied": true}),
                }],
            },
            status: VerificationTaskStatus::Pending,
            submitted_at: NOW,
            started_at: None,
            completed_at: None,
            failure_message: None,
            terminal_reason: None,
            entitlement_to_publish: None,
        }
    }

    fn paykit_verification_task(lock: &ContentLock, bundle_id: BundleId) -> VerificationTaskRecord {
        let mut task = verification_task(lock, bundle_id);
        task.submitted_proof_bundle.reader_public_key = Some(lock.creator.clone());
        task.submitted_proof_bundle.proofs[0].verifier_type = VerifierType::PaykitPayment;
        task.submitted_proof_bundle.proofs[0].payload = serde_json::json!({});
        task
    }

    fn invoice_intent(
        task: &VerificationTaskRecord,
        reader: CreatorPubky,
    ) -> InvoiceAdmissionIntentV1 {
        InvoiceAdmissionIntentV1 {
            version: INVOICE_ADMISSION_INTENT_VERSION,
            creator: task.creator.clone(),
            bundle_id: task.submitted_proof_bundle.bundle_id.clone(),
            lock_resource: task.submitted_proof_bundle.pubky_lock_resource.clone(),
            reader,
            payment_in: 24,
        }
    }

    fn content_lock() -> ContentLock {
        ContentLock {
            version: CONTENT_LOCK_VERSION,
            creator: CreatorPubky::from_str(CREATOR).unwrap(),
            primary_resource: Some(
                GuardedResource::new(
                    "/priv/app.locks/content/post.json".to_owned(),
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
}
