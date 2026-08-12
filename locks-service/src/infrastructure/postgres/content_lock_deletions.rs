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
        ContentLockDeletionPhase, ContentLockDeletionState,
    },
    ports::ContentLockDeletionRepository,
};

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
    async fn insert_job(&self, job: ContentLockDeletionJob) -> Result<(), ApplicationError> {
        job.validate_frozen_identity()?;
        job.validate_state(false)?;
        let frozen = serde_json::to_value(&job.frozen_content_lock).map_err(storage_display)?;
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
        .execute(&self.pool)
        .await
        .map_err(map_insert_error)?;
        Ok(())
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

    async fn request_force(
        &self,
        creator: &CreatorPubky,
        lock_id: &LockId,
        requested_at: OffsetDateTime,
    ) -> Result<bool, ApplicationError> {
        let result = sqlx::query(
            "UPDATE content_lock_deletion_jobs SET force_requested_at = $3, updated_at = $3
             WHERE creator = $1 AND lock_id = $2 AND force_requested_at IS NULL",
        )
        .bind(creator.to_string())
        .bind(lock_id.to_string())
        .bind(requested_at)
        .execute(&self.pool)
        .await
        .map_err(storage_error)?;
        Ok(result.rows_affected() == 1)
    }

    async fn record_force_receipt(
        &self,
        creator: &CreatorPubky,
        lock_id: &LockId,
        forced_at: OffsetDateTime,
    ) -> Result<(), ApplicationError> {
        sqlx::query(
            "INSERT INTO content_lock_force_deletion_receipts (creator, lock_id, forced_at)
             VALUES ($1, $2, $3) ON CONFLICT (creator, lock_id) DO NOTHING",
        )
        .bind(creator.to_string())
        .bind(lock_id.to_string())
        .bind(forced_at)
        .execute(&self.pool)
        .await
        .map_err(storage_error)?;
        Ok(())
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
    use std::{collections::BTreeMap, str::FromStr};

    use locks_core::{
        ids::{CreatorPubky, GuardedResourceHash},
        lock_policy::{
            AccessPolicy, CONTENT_LOCK_VERSION, ContentLock, GuardedResource, LockLogic,
            LockServerConfig,
        },
    };
    use time::macros::datetime;
    use uuid::Uuid;

    use super::PostgresContentLockDeletionRepository;
    use crate::{
        application::{
            models::{
                ContentLockDeletionFailureCode, ContentLockDeletionJob, ContentLockDeletionPhase,
                ContentLockDeletionState,
            },
            ports::ContentLockDeletionRepository,
        },
        infrastructure::postgres::testing::TestDatabase,
    };

    const CREATOR: &str = "pubkytkrq8zmwb8a3m9k15csu3q17qmfgqnp9dskbrg9uq1rydpyxp7qy";
    const NOW: time::OffsetDateTime = datetime!(2026-08-12 05:00:00 UTC);

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

        assert!(
            reopened
                .request_force(&job.creator, &job.lock_id, NOW)
                .await
                .unwrap()
        );
        assert!(
            !reopened
                .request_force(&job.creator, &job.lock_id, NOW)
                .await
                .unwrap()
        );
        reopened
            .record_force_receipt(&job.creator, &job.lock_id, NOW)
            .await
            .unwrap();
        reopened
            .record_force_receipt(&job.creator, &job.lock_id, NOW)
            .await
            .unwrap();
        assert!(
            reopened
                .has_force_receipt(&job.creator, &job.lock_id)
                .await
                .unwrap()
        );
        sqlx::query("DELETE FROM content_lock_deletion_jobs WHERE job_id = $1")
            .bind(job.job_id)
            .execute(database.pool())
            .await
            .unwrap();
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
