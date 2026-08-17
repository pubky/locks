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
        ContentLockDeletionPhase, ContentLockDeletionState, PrepareForceDeletionResult,
    },
    ports::ContentLockDeletionRepository,
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

    async fn abandon_publication(
        &self,
        creator: &CreatorPubky,
        lock_id: &LockId,
        publication_token: Uuid,
    ) -> Result<bool, ApplicationError> {
        delete_publication_intent(&self.pool, creator, lock_id, publication_token).await
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
            "{}/pub/locks.app/{}.json",
            job.creator,
            job.lock_id.as_str()
        );
        let mut transaction = self.pool.begin().await.map_err(storage_error)?;
        lock_proof_admission(&mut transaction, &job.creator, &job.lock_id).await?;
        let deletion_cutoff_exists = sqlx::query_scalar::<_, bool>(
            "SELECT EXISTS (SELECT 1 FROM content_lock_force_deletion_receipts
             WHERE creator = $1 AND lock_id = $2)
             OR EXISTS (SELECT 1 FROM content_lock_publication_intents
             WHERE creator = $1 AND lock_id = $2)",
        )
        .bind(job.creator.to_string())
        .bind(job.lock_id.to_string())
        .fetch_one(&mut *transaction)
        .await
        .map_err(storage_error)?;
        if deletion_cutoff_exists {
            return Err(ApplicationError::ContentLockDeletionInProgress);
        }
        sqlx::query(
            "SELECT task_id FROM verification_tasks
             WHERE creator = $1
               AND submitted_proof_bundle->>'pubky_lock_resource' = $2
             FOR UPDATE",
        )
        .bind(job.creator.to_string())
        .bind(&lock_resource)
        .fetch_all(&mut *transaction)
        .await
        .map_err(storage_error)?;
        let publication_in_progress: bool = sqlx::query_scalar(
            "SELECT EXISTS (
                 SELECT 1 FROM verification_tasks
                 WHERE creator = $1
                   AND submitted_proof_bundle->>'pubky_lock_resource' = $2
                   AND entitlement_publication_claim_token IS NOT NULL
             )",
        )
        .bind(job.creator.to_string())
        .bind(&lock_resource)
        .fetch_one(&mut *transaction)
        .await
        .map_err(storage_error)?;
        if publication_in_progress {
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
            "INSERT INTO content_lock_deletion_task_snapshot
                 (deletion_job_id, verification_task_id, creator, bundle_id,
                  pubky_lock_resource, criterion_id, status_at_cutoff,
                  paykit_admission_required)
             SELECT $1, task_id, creator, bundle_id,
                    submitted_proof_bundle->>'pubky_lock_resource',
                    (
                        SELECT proof->>'criterion_id'
                        FROM jsonb_array_elements(submitted_proof_bundle->'proofs') AS proof
                        WHERE proof->>'verifier_type' = 'paykit-payment'
                        LIMIT 1
                    ),
                    status,
                    (
                        EXISTS (
                            SELECT 1
                            FROM jsonb_array_elements(submitted_proof_bundle->'proofs') AS proof
                            WHERE proof->>'verifier_type' = 'paykit-payment'
                        )
                        OR EXISTS (
                            SELECT 1 FROM paykit_task_admissions AS admission
                            WHERE admission.verification_task_id = verification_tasks.task_id
                        )
                    )
             FROM verification_tasks
             WHERE creator = $2
               AND submitted_proof_bundle->>'pubky_lock_resource' = $3",
        )
        .bind(job.job_id)
        .bind(job.creator.to_string())
        .bind(lock_resource)
        .execute(&mut *transaction)
        .await
        .map_err(storage_error)?;
        sqlx::query(
            "UPDATE verification_tasks AS task
             SET status = 'pending', started_at = NULL, claimed_by = NULL,
                 claim_token = NULL, claim_expires_at = NULL,
                 entitlement_publication_claim_token = NULL, deletion_job_id = $1,
                 next_attempt_at = NULL,
                 last_attempt_error = NULL, updated_at = $2
             FROM content_lock_deletion_task_snapshot AS snapshot
             WHERE snapshot.deletion_job_id = $1
               AND snapshot.verification_task_id = task.task_id
               AND task.status IN ('pending', 'in_progress')",
        )
        .bind(job.job_id)
        .bind(job.deletion_started_at)
        .execute(&mut *transaction)
        .await
        .map_err(storage_error)?;
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
            "UPDATE content_lock_deletion_jobs
             SET state = 'running', claimed_by = $1, claim_token = $2,
                 claim_expires_at = $3, next_attempt_at = NULL,
                 attempt_count = attempt_count + 1, updated_at = $4
             WHERE job_id = (
                 SELECT job_id FROM content_lock_deletion_jobs
                 WHERE (state = 'queued' AND (next_attempt_at IS NULL OR next_attempt_at <= $4))
                    OR (state = 'running' AND claim_expires_at < $4)
                 ORDER BY deletion_started_at
                 FOR UPDATE SKIP LOCKED LIMIT 1
             )
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
            "UPDATE content_lock_deletion_jobs
             SET state = 'queued', next_attempt_at = $5, claimed_by = NULL,
                 claim_token = NULL, claim_expires_at = NULL, updated_at = $4
             WHERE job_id = $1 AND state = 'running' AND claimed_by = $2
               AND claim_token = $3 AND claim_expires_at >= $4
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
        let current =
            load_owned_claim(&mut transaction, job_id, worker_id, claim_token, now).await?;
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
            let has_unready_paykit_admission = sqlx::query_scalar::<_, bool>(
                "SELECT EXISTS (
                    SELECT 1
                    FROM content_lock_deletion_task_snapshot AS snapshot
                    LEFT JOIN paykit_task_admissions AS admission
                      ON admission.verification_task_id = snapshot.verification_task_id
                    WHERE snapshot.deletion_job_id = $1
                      AND (
                          snapshot.paykit_admission_required IS NULL
                          OR (
                              snapshot.paykit_admission_required = TRUE
                              AND (
                                  snapshot.criterion_id IS NULL
                                  OR admission.verification_task_id IS NULL
                                  OR admission.ready = FALSE
                                  OR admission.payment_in_hours IS NULL
                                  OR admission.payment_in_hours <= 0
                                  OR admission.invoice_created_at IS NULL
                                  OR admission.payment_deadline IS NULL
                                  OR admission.invoice_created_at > admission.payment_deadline
                              )
                          )
                      )
                )",
            )
            .bind(job_id)
            .fetch_one(&mut *transaction)
            .await
            .map_err(storage_error)?;
            if has_unready_paykit_admission {
                return Err(ApplicationError::InvalidContentLockDeletionState {
                    message:
                        "payment drain cannot start before reserved Paykit admissions are ready"
                            .to_owned(),
                });
            }
            sqlx::query(
                "UPDATE content_lock_deletion_task_snapshot AS snapshot
                 SET payment_in_hours = admission.payment_in_hours,
                     invoice_created_at = admission.invoice_created_at,
                     payment_deadline = admission.payment_deadline,
                     resolved_status = CASE
                         WHEN snapshot.status_at_cutoff IN ('completed', 'failed', 'expired')
                         THEN snapshot.status_at_cutoff
                         ELSE NULL
                     END,
                     resolved_at = CASE
                         WHEN snapshot.status_at_cutoff IN ('completed', 'failed', 'expired')
                         THEN $2
                         ELSE NULL
                     END
                 FROM paykit_task_admissions AS admission
                 WHERE snapshot.deletion_job_id = $1
                   AND snapshot.paykit_admission_required = TRUE
                   AND admission.verification_task_id = snapshot.verification_task_id",
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
                 claim_expires_at = NULL, updated_at = $3
             WHERE job_id = $1 RETURNING {ROW_COLUMNS}"
        );
        let updated = sqlx::query_as::<_, DeletionJobRow>(&sql)
            .bind(job_id)
            .bind(phase_to_database(next_phase))
            .bind(now)
            .fetch_one(&mut *transaction)
            .await
            .map_err(storage_error)?;
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
            "UPDATE content_lock_deletion_jobs
             SET state = $5, failure_code = $6, next_attempt_at = NULL,
                 claimed_by = NULL, claim_token = NULL, claim_expires_at = NULL, updated_at = $4
             WHERE job_id = $1 AND state = 'running' AND claimed_by = $2
               AND claim_token = $3 AND claim_expires_at >= $4
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
        if let Some(row) = existing {
            let job = row_to_job(row)?;
            if matches!(
                job.state,
                ContentLockDeletionState::Queued | ContentLockDeletionState::Running
            ) {
                let entitlement_publication_in_progress: bool = sqlx::query_scalar(
                    "SELECT EXISTS (
                        SELECT 1 FROM verification_tasks
                        WHERE deletion_job_id = $1
                          AND entitlement_publication_claim_token IS NOT NULL
                    )",
                )
                .bind(job.job_id)
                .fetch_one(&mut *transaction)
                .await
                .map_err(storage_error)?;
                if entitlement_publication_in_progress {
                    transaction.commit().await.map_err(storage_error)?;
                    return Ok(PrepareForceDeletionResult::PublicationInProgress);
                }
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
            sqlx::query(
                "INSERT INTO content_lock_force_deletion_receipts (creator, lock_id, forced_at)
                 VALUES ($1, $2, $3) ON CONFLICT (creator, lock_id) DO NOTHING",
            )
            .bind(creator.to_string())
            .bind(lock_id.to_string())
            .bind(forced_at)
            .execute(&mut *transaction)
            .await
            .map_err(storage_error)?;
            sqlx::query("DELETE FROM content_lock_deletion_jobs WHERE job_id = $1")
                .bind(job.job_id)
                .execute(&mut *transaction)
                .await
                .map_err(storage_error)?;
            transaction.commit().await.map_err(storage_error)?;
            return Ok(PrepareForceDeletionResult::Synchronous(Some(job)));
        }
        sqlx::query(
            "INSERT INTO content_lock_force_deletion_receipts (creator, lock_id, forced_at)
             VALUES ($1, $2, $3) ON CONFLICT (creator, lock_id) DO NOTHING",
        )
        .bind(creator.to_string())
        .bind(lock_id.to_string())
        .bind(forced_at)
        .execute(&mut *transaction)
        .await
        .map_err(storage_error)?;
        transaction.commit().await.map_err(storage_error)?;
        Ok(PrepareForceDeletionResult::Synchronous(None))
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
    now: OffsetDateTime,
) -> Result<Option<ContentLockDeletionJob>, ApplicationError> {
    let sql = format!(
        "SELECT {ROW_COLUMNS} FROM content_lock_deletion_jobs
         WHERE job_id = $1 AND state = 'running' AND claimed_by = $2
           AND claim_token = $3 AND claim_expires_at >= $4 FOR UPDATE"
    );
    sqlx::query_as::<_, DeletionJobRow>(&sql)
        .bind(job_id)
        .bind(worker_id)
        .bind(claim_token)
        .bind(now)
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
    use std::{collections::BTreeMap, str::FromStr, sync::Mutex};

    use async_trait::async_trait;
    use locks_core::{
        ids::{
            BundleId, CreatorPubky, GuardedResourceHash, LockServerPubky, PubkyLockResource, TaskId,
        },
        lock_policy::{
            AccessPolicy, CONTENT_LOCK_VERSION, ContentLock, GuardedResource, LockLogic,
            LockServerConfig, VerifierType,
        },
        verification::{Proof, SUBMITTED_PROOF_BUNDLE_VERSION, SubmittedProofBundle},
    };
    use time::macros::datetime;
    use uuid::Uuid;

    use super::PostgresContentLockDeletionRepository;
    use crate::{
        application::{
            errors::ApplicationError,
            models::{
                ContentLockDeletionFailureCode, ContentLockDeletionJob, ContentLockDeletionPhase,
                ContentLockDeletionState, PrepareForceDeletionResult, VerificationTaskRecord,
                VerificationTaskStatus,
            },
            ports::{
                Clock, ContentLockDeletionRepository, EntitlementRepository,
                PaymentDrainCleanupToken, PaymentDrainClient, PaymentDrainClientError,
                PaymentDrainRepository, PaymentDrainStatus, PaymentDrainSummary,
                PaymentDrainTerminalTransition, PaymentRequestState, PaymentRequestStatus,
                PaymentState, VerificationTaskClaimer, VerificationTaskRepository,
            },
            use_cases::drain_lock_payments::DrainLockPaymentsUseCase,
        },
        infrastructure::memory::entitlements::InMemoryEntitlementRepository,
        infrastructure::postgres::{
            PostgresPaymentDrainRepository, PostgresVerificationTaskClaimer,
            PostgresVerificationTaskRepository, testing::TestDatabase,
        },
    };

    const CREATOR: &str = "pubkytkrq8zmwb8a3m9k15csu3q17qmfgqnp9dskbrg9uq1rydpyxp7qy";
    const NOW: time::OffsetDateTime = datetime!(2026-08-12 05:00:00 UTC);

    #[tokio::test]
    async fn deletion_commit_order_is_the_authoritative_proof_admission_cutoff() {
        let database = TestDatabase::create().await;
        let deletions = PostgresContentLockDeletionRepository::new(database.pool().clone());
        let tasks = PostgresVerificationTaskRepository::new(database.pool().clone());
        let lock = content_lock();

        let admitted_before = verification_task(&lock, BundleId::from_bytes([1; 16]));
        tasks
            .insert_verification_task(admitted_before.clone())
            .await
            .unwrap();
        let job = ContentLockDeletionJob::new(Uuid::new_v4(), lock.clone(), NOW).unwrap();
        deletions.insert_job(job.clone()).await.unwrap();

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

        let admitted_after = verification_task(&lock, BundleId::from_bytes([2; 16]));
        assert_eq!(
            tasks.insert_verification_task(admitted_after).await,
            Err(ApplicationError::ContentLockDeletionInProgress)
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
                repository.prepare_force_deletion(&job.creator, &job.lock_id, NOW)
            );
            let persisted_job = repository
                .get_job(&job.creator, &job.lock_id)
                .await
                .unwrap();
            let receipt = repository
                .has_force_receipt(&job.creator, &job.lock_id)
                .await
                .unwrap();

            match (graceful, force) {
                (Ok(()), Ok(PrepareForceDeletionResult::Active(active))) => {
                    assert_eq!(active.job_id, job.job_id);
                    assert!(active.force_requested_at.is_some());
                    assert_eq!(persisted_job, Some(active));
                    assert!(!receipt);
                }
                (
                    Err(ApplicationError::ContentLockDeletionInProgress),
                    Ok(PrepareForceDeletionResult::Synchronous(None)),
                ) => {
                    assert!(persisted_job.is_none());
                    assert!(receipt);
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
                .prepare_force_deletion(&lock.creator, &lock_id, NOW)
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
        assert_eq!(
            repository
                .prepare_force_deletion(&lock.creator, &lock_id, NOW)
                .await
                .unwrap(),
            PrepareForceDeletionResult::Synchronous(None)
        );
        assert_eq!(
            repository
                .begin_publication(&lock.creator, &lock_id, Uuid::new_v4())
                .await,
            Err(ApplicationError::ContentLockDeletionInProgress)
        );

        database.cleanup().await;
    }

    #[tokio::test]
    async fn durable_paykit_reservation_commits_before_deletion_and_is_snapshotted() {
        use crate::infrastructure::postgres::PostgresPaykitTaskAdmissionRepository;

        let database = TestDatabase::create().await;
        let lock = content_lock();
        let task = verification_task(&lock, BundleId::from_bytes([3; 16]));
        let admissions = PostgresPaykitTaskAdmissionRepository::new(database.pool().clone());
        let admission = admissions.reserve(task.clone(), 24).await.unwrap();
        assert!(admission.requires_paykit);
        assert_eq!(admission.payment_in, 24);
        assert_eq!(admission.invoice_window, None);

        let repository = PostgresContentLockDeletionRepository::new(database.pool().clone());
        repository
            .insert_job(ContentLockDeletionJob::new(Uuid::new_v4(), lock, NOW).unwrap())
            .await
            .unwrap();

        let snapshotted = sqlx::query_scalar::<_, bool>(
            "SELECT EXISTS (
                SELECT 1 FROM content_lock_deletion_task_snapshot
                WHERE verification_task_id = $1
            )",
        )
        .bind(task.task_id.as_uuid())
        .fetch_one(database.pool())
        .await
        .unwrap();
        assert!(snapshotted);

        database.cleanup().await;
    }

    #[tokio::test]
    async fn deletion_first_rejects_durable_paykit_reservation_before_external_work() {
        use crate::infrastructure::postgres::PostgresPaykitTaskAdmissionRepository;

        let database = TestDatabase::create().await;
        let lock = content_lock();
        let repository = PostgresContentLockDeletionRepository::new(database.pool().clone());
        repository
            .insert_job(ContentLockDeletionJob::new(Uuid::new_v4(), lock.clone(), NOW).unwrap())
            .await
            .unwrap();

        let admissions = PostgresPaykitTaskAdmissionRepository::new(database.pool().clone());
        let mut external_calls = 0;
        let result = admissions
            .reserve(verification_task(&lock, BundleId::from_bytes([4; 16])), 24)
            .await;
        if result.is_ok() {
            external_calls += 1;
        }
        assert!(matches!(
            result,
            Err(ApplicationError::ContentLockDeletionInProgress)
        ));
        assert_eq!(external_calls, 0);

        database.cleanup().await;
    }

    #[tokio::test]
    async fn payment_drain_phase_waits_for_every_snapshotted_paykit_reservation() {
        use crate::infrastructure::postgres::PostgresPaykitTaskAdmissionRepository;

        let database = TestDatabase::create().await;
        let lock = content_lock();
        let mut task = verification_task(&lock, BundleId::from_bytes([6; 16]));
        task.submitted_proof_bundle.proofs[0].verifier_type = VerifierType::PaykitPayment;
        let admissions = PostgresPaykitTaskAdmissionRepository::new(database.pool().clone());
        let admission = admissions.reserve(task.clone(), 24).await.unwrap();
        let repository = PostgresContentLockDeletionRepository::new(database.pool().clone());
        repository
            .insert_job(ContentLockDeletionJob::new(Uuid::new_v4(), lock, NOW).unwrap())
            .await
            .unwrap();
        let claim = repository
            .claim_next("worker", NOW, NOW + time::Duration::seconds(60))
            .await
            .unwrap()
            .unwrap();

        let blocked = repository
            .advance_phase(
                claim.job.job_id,
                "worker",
                claim.claim_token,
                NOW,
                ContentLockDeletionPhase::StartPaymentDrain,
            )
            .await;
        assert!(matches!(
            blocked,
            Err(ApplicationError::InvalidContentLockDeletionState { message })
                if message == "payment drain cannot start before reserved Paykit admissions are ready"
        ));
        let still_running = repository
            .get_job(&claim.job.creator, &claim.job.lock_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(still_running.state, ContentLockDeletionState::Running);
        assert_eq!(still_running.phase, ContentLockDeletionPhase::Withdraw);

        let invoice_window = crate::infrastructure::postgres::PaykitInvoiceWindow {
            invoice_created_at: datetime!(2026-08-12 05:01:00 UTC),
            payment_deadline: datetime!(2026-08-13 05:01:00 UTC),
        };
        admissions
            .mark_ready(&admission.task, invoice_window)
            .await
            .unwrap();
        let replay = admissions
            .find_existing(&task.submitted_proof_bundle)
            .await
            .unwrap()
            .unwrap();
        assert!(!replay.requires_paykit);
        assert_eq!(replay.payment_in, 24);
        assert_eq!(replay.invoice_window, Some(invoice_window));
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

        database.cleanup().await;
    }

    #[tokio::test]
    async fn payment_drain_phase_rejects_snapshotted_legacy_admission_without_invoice_window() {
        use crate::infrastructure::postgres::verification_tasks::PostgresVerificationTaskRepository;

        let database = TestDatabase::create().await;
        let lock = content_lock();
        let task = verification_task(&lock, BundleId::from_bytes([7; 16]));
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

        let repository = PostgresContentLockDeletionRepository::new(database.pool().clone());
        repository
            .insert_job(ContentLockDeletionJob::new(Uuid::new_v4(), lock, NOW).unwrap())
            .await
            .unwrap();
        let claim = repository
            .claim_next("worker", NOW, NOW + time::Duration::seconds(60))
            .await
            .unwrap()
            .unwrap();

        let result = repository
            .advance_phase(
                claim.job.job_id,
                "worker",
                claim.claim_token,
                NOW,
                ContentLockDeletionPhase::StartPaymentDrain,
            )
            .await;
        assert!(matches!(
            result,
            Err(ApplicationError::InvalidContentLockDeletionState { message })
                if message == "payment drain cannot start before reserved Paykit admissions are ready"
        ));

        database.cleanup().await;
    }

    #[tokio::test]
    async fn paykit_admission_insert_failure_rolls_back_verification_task() {
        use crate::infrastructure::postgres::PostgresPaykitTaskAdmissionRepository;

        let database = TestDatabase::create().await;
        sqlx::query(
            "CREATE FUNCTION reject_paykit_admission() RETURNS trigger AS $$
             BEGIN
                 RAISE EXCEPTION 'injected admission failure';
             END;
             $$ LANGUAGE plpgsql",
        )
        .execute(database.pool())
        .await
        .unwrap();
        sqlx::query(
            "CREATE TRIGGER reject_paykit_admission
             BEFORE INSERT ON paykit_task_admissions
             FOR EACH ROW EXECUTE FUNCTION reject_paykit_admission()",
        )
        .execute(database.pool())
        .await
        .unwrap();

        let admissions = PostgresPaykitTaskAdmissionRepository::new(database.pool().clone());
        let task = verification_task(&content_lock(), BundleId::from_bytes([5; 16]));
        assert!(admissions.reserve(task, 24).await.is_err());
        let task_count = sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM verification_tasks")
            .fetch_one(database.pool())
            .await
            .unwrap();
        assert_eq!(task_count, 0);

        database.cleanup().await;
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

        assert!(matches!(
            reopened
                .prepare_force_deletion(&job.creator, &job.lock_id, NOW)
                .await
                .unwrap(),
            PrepareForceDeletionResult::Synchronous(Some(_))
        ));
        assert!(matches!(
            reopened
                .prepare_force_deletion(&job.creator, &job.lock_id, NOW)
                .await
                .unwrap(),
            PrepareForceDeletionResult::Synchronous(None)
        ));
        assert!(
            reopened
                .has_force_receipt(&job.creator, &job.lock_id)
                .await
                .unwrap()
        );
        assert!(
            reopened
                .get_job(&job.creator, &job.lock_id)
                .await
                .unwrap()
                .is_none()
        );
        assert!(
            reopened
                .has_force_receipt(&job.creator, &job.lock_id)
                .await
                .unwrap()
        );

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
            .prepare_force_deletion(&job.creator, &job.lock_id, NOW)
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
    async fn deletion_lease_takes_exclusive_ownership_of_snapshotted_paykit_obligation() {
        use crate::infrastructure::postgres::{
            PaykitInvoiceWindow, PostgresPaykitTaskAdmissionRepository,
        };

        let database = TestDatabase::create().await;
        let deletions = PostgresContentLockDeletionRepository::new(database.pool().clone());
        let drains = PostgresPaymentDrainRepository::new(database.pool().clone());
        let admissions = PostgresPaykitTaskAdmissionRepository::new(database.pool().clone());
        let ordinary = PostgresVerificationTaskClaimer::new(database.pool().clone());
        let lock = content_lock();
        let mut task = verification_task(&lock, BundleId::from_bytes([9; 16]));
        task.submitted_proof_bundle.proofs[0].verifier_type = VerifierType::PaykitPayment;
        admissions.reserve(task.clone(), 24).await.unwrap();
        let window = PaykitInvoiceWindow {
            invoice_created_at: NOW,
            payment_deadline: NOW + time::Duration::hours(24),
        };
        admissions.mark_ready(&task, window).await.unwrap();

        let ordinary_claim = ordinary
            .claim_next_verification_task("ordinary", NOW, NOW + time::Duration::minutes(5))
            .await
            .unwrap()
            .unwrap();
        let job = ContentLockDeletionJob::new(Uuid::new_v4(), lock, NOW).unwrap();
        deletions.insert_job(job.clone()).await.unwrap();

        assert!(
            !ordinary
                .begin_claimed_entitlement_publication(
                    &ordinary_claim.task.task_id,
                    "ordinary",
                    &ordinary_claim.claim_token,
                    NOW,
                )
                .await
                .unwrap()
        );

        let ordinary_completed = ordinary_claim
            .task
            .clone()
            .transition_to(VerificationTaskStatus::Completed, NOW, None)
            .unwrap();
        assert!(
            ordinary
                .persist_claimed_verification_task_transition(
                    ordinary_completed,
                    "ordinary",
                    &ordinary_claim.claim_token,
                    NOW,
                )
                .await
                .unwrap()
                .is_none()
        );

        let withdraw_claim = deletions
            .claim_next("deletion", NOW, NOW + time::Duration::minutes(5))
            .await
            .unwrap()
            .unwrap();
        deletions
            .advance_phase(
                job.job_id,
                "deletion",
                withdraw_claim.claim_token,
                NOW,
                ContentLockDeletionPhase::StartPaymentDrain,
            )
            .await
            .unwrap()
            .unwrap();
        let start_claim = deletions
            .claim_next("deletion", NOW, NOW + time::Duration::minutes(5))
            .await
            .unwrap()
            .unwrap();
        let token =
            PaymentDrainCleanupToken::parse("AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA").unwrap();
        let summary = PaymentDrainSummary {
            status: PaymentDrainStatus::Active,
            accepted_count: 1,
            terminal_count: 0,
            cancellation_enqueued_count: 0,
            cleanup_token: token,
        };
        assert!(
            drains
                .store_payment_drain(
                    job.job_id,
                    "deletion",
                    start_claim.claim_token,
                    NOW,
                    &summary,
                )
                .await
                .unwrap()
        );
        assert!(
            drains
                .store_payment_drain(
                    job.job_id,
                    "deletion",
                    start_claim.claim_token,
                    NOW,
                    &summary,
                )
                .await
                .unwrap()
        );
        let divergent = PaymentDrainSummary {
            accepted_count: 2,
            ..summary.clone()
        };
        assert!(
            drains
                .store_payment_drain(
                    job.job_id,
                    "deletion",
                    start_claim.claim_token,
                    NOW,
                    &divergent,
                )
                .await
                .is_err()
        );
        assert_eq!(
            drains.get_payment_drain(job.job_id).await.unwrap(),
            Some(summary)
        );
        let obligations = drains.list_obligations(job.job_id).await.unwrap();
        assert_eq!(obligations.len(), 1);
        assert_eq!(obligations[0].task_id, task.task_id);
        assert_eq!(obligations[0].invoice_created_at, window.invoice_created_at);
        assert_eq!(obligations[0].payment_deadline, window.payment_deadline);
        assert!(!drains.all_obligations_terminal(job.job_id).await.unwrap());

        deletions
            .advance_phase(
                job.job_id,
                "deletion",
                start_claim.claim_token,
                NOW,
                ContentLockDeletionPhase::DrainPayments,
            )
            .await
            .unwrap()
            .unwrap();
        let drain_claim = deletions
            .claim_next("deletion", NOW, NOW + time::Duration::minutes(5))
            .await
            .unwrap()
            .unwrap();
        let publication_token = drains
            .begin_entitlement_publication(
                job.job_id,
                "deletion",
                drain_claim.claim_token,
                NOW,
                &task.task_id,
            )
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            drains
                .begin_entitlement_publication(
                    job.job_id,
                    "deletion",
                    drain_claim.claim_token,
                    NOW,
                    &task.task_id,
                )
                .await
                .unwrap(),
            Some(publication_token)
        );
        assert_eq!(
            deletions
                .prepare_force_deletion(&job.creator, &job.lock_id, NOW)
                .await
                .unwrap(),
            PrepareForceDeletionResult::PublicationInProgress
        );
        assert!(
            !drains
                .persist_terminal_obligation(
                    job.job_id,
                    "deletion",
                    drain_claim.claim_token,
                    NOW,
                    &task.task_id,
                    PaymentDrainTerminalTransition {
                        status: VerificationTaskStatus::Completed,
                        entitlement_publication_token: Some(Uuid::new_v4()),
                    },
                )
                .await
                .unwrap()
        );
        assert!(
            drains
                .persist_terminal_obligation(
                    job.job_id,
                    "deletion",
                    drain_claim.claim_token,
                    NOW,
                    &task.task_id,
                    PaymentDrainTerminalTransition {
                        status: VerificationTaskStatus::Completed,
                        entitlement_publication_token: Some(publication_token),
                    },
                )
                .await
                .unwrap()
        );
        assert!(drains.all_obligations_terminal(job.job_id).await.unwrap());
        let retained_marker: Option<Uuid> = sqlx::query_scalar(
            "SELECT entitlement_publication_claim_token FROM verification_tasks WHERE task_id = $1",
        )
        .bind(task.task_id.as_uuid())
        .fetch_one(database.pool())
        .await
        .unwrap();
        assert_eq!(retained_marker, None);

        database.cleanup().await;
    }

    #[tokio::test]
    async fn entitlement_publication_fence_committed_first_blocks_deletion_cutoff() {
        let database = TestDatabase::create().await;
        let deletions = PostgresContentLockDeletionRepository::new(database.pool().clone());
        let tasks = PostgresVerificationTaskRepository::new(database.pool().clone());
        let ordinary = PostgresVerificationTaskClaimer::new(database.pool().clone());
        let lock = content_lock();
        let task = verification_task(&lock, BundleId::from_bytes([11; 16]));
        tasks.insert_verification_task(task).await.unwrap();
        let claim = ordinary
            .claim_next_verification_task("ordinary", NOW, NOW + time::Duration::minutes(5))
            .await
            .unwrap()
            .unwrap();
        assert!(
            ordinary
                .begin_claimed_entitlement_publication(
                    &claim.task.task_id,
                    "ordinary",
                    &claim.claim_token,
                    NOW,
                )
                .await
                .unwrap()
        );

        let job = ContentLockDeletionJob::new(Uuid::new_v4(), lock, NOW).unwrap();
        assert_eq!(
            deletions.insert_job(job.clone()).await,
            Err(ApplicationError::ContentLockDeletionInProgress)
        );
        assert!(
            deletions
                .get_job(&job.creator, &job.lock_id)
                .await
                .unwrap()
                .is_none()
        );

        database.cleanup().await;
    }

    #[tokio::test]
    async fn completed_paykit_aggregate_waits_for_locks_confirmation_and_local_terminal_state() {
        use crate::infrastructure::postgres::{
            PaykitInvoiceWindow, PostgresPaykitTaskAdmissionRepository,
        };

        let database = TestDatabase::create().await;
        let deletions = PostgresContentLockDeletionRepository::new(database.pool().clone());
        let drains = PostgresPaymentDrainRepository::new(database.pool().clone());
        let admissions = PostgresPaykitTaskAdmissionRepository::new(database.pool().clone());
        let entitlements = InMemoryEntitlementRepository::new();
        let lock = content_lock();
        let mut task = verification_task(&lock, BundleId::from_bytes([10; 16]));
        task.submitted_proof_bundle.proofs[0].verifier_type = VerifierType::PaykitPayment;
        admissions.reserve(task.clone(), 24).await.unwrap();
        let window = PaykitInvoiceWindow {
            invoice_created_at: NOW,
            payment_deadline: NOW + time::Duration::hours(24),
        };
        admissions.mark_ready(&task, window).await.unwrap();
        let job = ContentLockDeletionJob::new(Uuid::new_v4(), lock, NOW).unwrap();
        deletions.insert_job(job.clone()).await.unwrap();
        let withdraw = deletions
            .claim_next("deletion", NOW, NOW + time::Duration::minutes(5))
            .await
            .unwrap()
            .unwrap();
        deletions
            .advance_phase(
                job.job_id,
                "deletion",
                withdraw.claim_token,
                NOW,
                ContentLockDeletionPhase::StartPaymentDrain,
            )
            .await
            .unwrap()
            .unwrap();
        let start = deletions
            .claim_next("deletion", NOW, NOW + time::Duration::minutes(5))
            .await
            .unwrap()
            .unwrap();
        let token =
            PaymentDrainCleanupToken::parse("AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA").unwrap();
        let initial_summary = PaymentDrainSummary {
            status: PaymentDrainStatus::Active,
            accepted_count: 1,
            terminal_count: 0,
            cancellation_enqueued_count: 0,
            cleanup_token: token.clone(),
        };
        drains
            .store_payment_drain(
                job.job_id,
                "deletion",
                start.claim_token,
                NOW,
                &initial_summary,
            )
            .await
            .unwrap();
        deletions
            .advance_phase(
                job.job_id,
                "deletion",
                start.claim_token,
                NOW,
                ContentLockDeletionPhase::DrainPayments,
            )
            .await
            .unwrap()
            .unwrap();
        let claim = deletions
            .claim_next("deletion", NOW, NOW + time::Duration::minutes(5))
            .await
            .unwrap()
            .unwrap();
        let paykit = MutablePaymentDrainClient {
            summary: PaymentDrainSummary {
                status: PaymentDrainStatus::Completed,
                accepted_count: 0,
                terminal_count: 1,
                cancellation_enqueued_count: 0,
                cleanup_token: token,
            },
            status: Mutex::new(PaymentRequestStatus {
                request_state: PaymentRequestState::Accepted,
                payment_state: PaymentState::Detected,
                invoice_created_at: window.invoice_created_at,
                payment_deadline: window.payment_deadline,
                confirmations: 0,
                amount_matched: true,
            }),
        };
        let clock = FixedClock(NOW);
        let use_case = DrainLockPaymentsUseCase::new(
            &deletions,
            &drains,
            &paykit,
            &entitlements,
            &clock,
            LockServerPubky::from_str("pubky7ir1ttte48bcp4zjychjyscicrwi1j34mtt91ptsafdbjmr8g9eo")
                .unwrap(),
            6,
        );
        assert!(
            !use_case
                .execute_claimed(claim.clone(), "deletion")
                .await
                .unwrap()
        );
        assert!(!drains.all_obligations_terminal(job.job_id).await.unwrap());
        assert!(
            entitlements
                .get_verified_proof_bundle(&task.creator, &task.submitted_proof_bundle.bundle_id,)
                .await
                .unwrap()
                .is_none()
        );

        {
            let mut status = paykit.status.lock().unwrap();
            status.payment_state = PaymentState::Confirmed;
            status.confirmations = 6;
        }
        assert!(use_case.execute_claimed(claim, "deletion").await.unwrap());
        assert!(drains.all_obligations_terminal(job.job_id).await.unwrap());
        assert!(
            entitlements
                .get_verified_proof_bundle(&task.creator, &task.submitted_proof_bundle.bundle_id,)
                .await
                .unwrap()
                .is_some()
        );
        assert_eq!(
            deletions
                .get_job(&job.creator, &job.lock_id)
                .await
                .unwrap()
                .unwrap()
                .phase,
            ContentLockDeletionPhase::DrainExistingCredentials
        );

        database.cleanup().await;
    }

    struct FixedClock(time::OffsetDateTime);

    impl Clock for FixedClock {
        fn now(&self) -> time::OffsetDateTime {
            self.0
        }
    }

    struct MutablePaymentDrainClient {
        summary: PaymentDrainSummary,
        status: Mutex<PaymentRequestStatus>,
    }

    #[async_trait]
    impl PaymentDrainClient for MutablePaymentDrainClient {
        async fn start_payment_drain(
            &self,
            _lock_resource: &PubkyLockResource,
        ) -> Result<PaymentDrainSummary, PaymentDrainClientError> {
            Ok(self.summary.clone())
        }

        async fn lookup_payment_drain(
            &self,
            _lock_resource: &PubkyLockResource,
        ) -> Result<Option<PaymentDrainSummary>, PaymentDrainClientError> {
            Ok(Some(self.summary.clone()))
        }

        async fn payment_request_status(
            &self,
            _creator: &CreatorPubky,
            _bundle_id: &BundleId,
        ) -> Result<Option<PaymentRequestStatus>, PaymentDrainClientError> {
            Ok(Some(*self.status.lock().unwrap()))
        }
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

    fn verification_task(lock: &ContentLock, bundle_id: BundleId) -> VerificationTaskRecord {
        let lock_resource = PubkyLockResource::from_str(&format!(
            "{}/pub/locks.app/{}.json",
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
}
