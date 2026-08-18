use std::str::FromStr;

use async_trait::async_trait;
use locks_core::ids::{BundleId, CreatorPubky, PubkyLockResource, TaskId};
use sqlx::{FromRow, PgPool};
use time::OffsetDateTime;
use uuid::Uuid;

use crate::application::errors::ApplicationError;
use crate::application::models::VerificationTaskStatus;
use crate::application::ports::{
    PaymentDrainCleanupToken, PaymentDrainObligation, PaymentDrainRepository, PaymentDrainStatus,
    PaymentDrainSummary, PaymentDrainTerminalTransition,
};

#[derive(Debug, Clone)]
pub struct PostgresPaymentDrainRepository {
    pool: PgPool,
}

impl PostgresPaymentDrainRepository {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }
}

#[derive(FromRow)]
struct ObligationRow {
    task_id: Uuid,
    creator: String,
    bundle_id: String,
    pubky_lock_resource: String,
    criterion_id: String,
    invoice_created_at: OffsetDateTime,
    payment_deadline: OffsetDateTime,
    status: String,
}

#[derive(FromRow)]
struct DrainRow {
    status: String,
    accepted_count: i64,
    terminal_count: i64,
    cancellation_enqueued_count: i64,
    cleanup_token: String,
}

#[derive(FromRow)]
struct DeletionOwnershipRow {
    state: String,
    phase: String,
    force_requested_at: Option<OffsetDateTime>,
    claimed_by: Option<String>,
    claim_token: Option<Uuid>,
    claim_expires_at: Option<OffsetDateTime>,
}

#[derive(FromRow)]
struct ObligationFenceRow {
    paykit_admission_required: Option<bool>,
    resolved_status: Option<String>,
}

#[async_trait]
impl PaymentDrainRepository for PostgresPaymentDrainRepository {
    async fn store_payment_drain(
        &self,
        deletion_job_id: Uuid,
        worker_id: &str,
        claim_token: Uuid,
        now: OffsetDateTime,
        summary: &PaymentDrainSummary,
    ) -> Result<bool, ApplicationError> {
        let counts = summary_counts(summary)?;
        let mut transaction = self.pool.begin().await.map_err(storage_error)?;
        let ownership = lock_deletion_ownership(&mut transaction, deletion_job_id).await?;
        if !owns_live_drain_claim(
            ownership.as_ref(),
            worker_id,
            claim_token,
            now,
            "start_payment_drain",
        ) {
            transaction.rollback().await.map_err(storage_error)?;
            return Ok(false);
        }
        let existing = sqlx::query_as::<_, DrainRow>(
            "SELECT status, accepted_count, terminal_count,
                    cancellation_enqueued_count, cleanup_token
             FROM content_lock_payment_drains
             WHERE deletion_job_id = $1 FOR UPDATE",
        )
        .bind(deletion_job_id)
        .fetch_optional(&mut *transaction)
        .await
        .map_err(storage_error)?;
        if let Some(existing) = existing {
            let existing = row_to_summary(existing)?;
            if !valid_aggregate_progress(&existing, summary) {
                return Err(invalid_deletion(
                    "Paykit payment drain aggregate changed for deletion job",
                ));
            }
            sqlx::query(
                "UPDATE content_lock_payment_drains
                 SET status = $2, accepted_count = $3, terminal_count = $4, updated_at = $5
                 WHERE deletion_job_id = $1",
            )
            .bind(deletion_job_id)
            .bind(drain_status_to_database(summary.status))
            .bind(counts.0)
            .bind(counts.1)
            .bind(now)
            .execute(&mut *transaction)
            .await
            .map_err(storage_error)?;
        } else {
            sqlx::query(
                "INSERT INTO content_lock_payment_drains
                     (deletion_job_id, status, accepted_count, terminal_count,
                      cancellation_enqueued_count, cleanup_token, created_at, updated_at)
                 VALUES ($1, $2, $3, $4, $5, $6, $7, $7)",
            )
            .bind(deletion_job_id)
            .bind(drain_status_to_database(summary.status))
            .bind(counts.0)
            .bind(counts.1)
            .bind(counts.2)
            .bind(summary.cleanup_token.as_str())
            .bind(now)
            .execute(&mut *transaction)
            .await
            .map_err(storage_error)?;
        }
        transaction.commit().await.map_err(storage_error)?;
        Ok(true)
    }

    async fn get_payment_drain(
        &self,
        deletion_job_id: Uuid,
    ) -> Result<Option<PaymentDrainSummary>, ApplicationError> {
        sqlx::query_as::<_, DrainRow>(
            "SELECT status, accepted_count, terminal_count,
                    cancellation_enqueued_count, cleanup_token
             FROM content_lock_payment_drains WHERE deletion_job_id = $1",
        )
        .bind(deletion_job_id)
        .fetch_optional(&self.pool)
        .await
        .map_err(storage_error)?
        .map(row_to_summary)
        .transpose()
    }

    async fn reconcile_payment_drain(
        &self,
        deletion_job_id: Uuid,
        worker_id: &str,
        claim_token: Uuid,
        now: OffsetDateTime,
        summary: &PaymentDrainSummary,
    ) -> Result<bool, ApplicationError> {
        let counts = summary_counts(summary)?;
        let mut transaction = self.pool.begin().await.map_err(storage_error)?;
        let ownership = lock_deletion_ownership(&mut transaction, deletion_job_id).await?;
        if !owns_live_drain_claim(
            ownership.as_ref(),
            worker_id,
            claim_token,
            now,
            "drain_payments",
        ) {
            transaction.rollback().await.map_err(storage_error)?;
            return Ok(false);
        }
        let updated = sqlx::query(
            "UPDATE content_lock_payment_drains
             SET status = $3, accepted_count = $4, terminal_count = $5, updated_at = $2
             WHERE deletion_job_id = $1
               AND cleanup_token = $6
               AND cancellation_enqueued_count = $7
               AND accepted_count >= $4
               AND terminal_count <= $5
               AND accepted_count - $4 = $5 - terminal_count
               AND NOT (status = 'completed' AND $3 <> 'completed')
               AND (($3 = 'completed' AND $4 = 0) OR ($3 = 'active' AND $4 > 0))",
        )
        .bind(deletion_job_id)
        .bind(now)
        .bind(drain_status_to_database(summary.status))
        .bind(counts.0)
        .bind(counts.1)
        .bind(summary.cleanup_token.as_str())
        .bind(counts.2)
        .execute(&mut *transaction)
        .await
        .map_err(storage_error)?;
        transaction.commit().await.map_err(storage_error)?;
        Ok(updated.rows_affected() == 1)
    }

    async fn list_obligations(
        &self,
        deletion_job_id: Uuid,
    ) -> Result<Vec<PaymentDrainObligation>, ApplicationError> {
        let rows = sqlx::query_as::<_, ObligationRow>(
            "SELECT snapshot.verification_task_id AS task_id, snapshot.creator,
                    snapshot.bundle_id, snapshot.pubky_lock_resource,
                    snapshot.criterion_id,
                    snapshot.invoice_created_at, snapshot.payment_deadline,
                    COALESCE(snapshot.resolved_status, snapshot.status_at_cutoff) AS status
             FROM content_lock_deletion_task_snapshot AS snapshot
             WHERE snapshot.deletion_job_id = $1
               AND snapshot.paykit_admission_required = TRUE
             ORDER BY snapshot.verification_task_id",
        )
        .bind(deletion_job_id)
        .fetch_all(&self.pool)
        .await
        .map_err(storage_error)?;
        rows.into_iter().map(row_to_obligation).collect()
    }

    async fn begin_entitlement_publication(
        &self,
        deletion_job_id: Uuid,
        worker_id: &str,
        claim_token: Uuid,
        now: OffsetDateTime,
        task_id: &TaskId,
    ) -> Result<Option<Uuid>, ApplicationError> {
        let publication_token = Uuid::new_v4();
        let mut transaction = self.pool.begin().await.map_err(storage_error)?;
        let owns_claim: bool = sqlx::query_scalar(
            "SELECT EXISTS (
                SELECT 1 FROM content_lock_deletion_jobs
                WHERE job_id = $1 AND state = 'running' AND claimed_by = $2
                  AND claim_token = $3 AND claim_expires_at >= $4
                  AND phase = 'drain_payments' AND force_requested_at IS NULL
                FOR UPDATE
            )",
        )
        .bind(deletion_job_id)
        .bind(worker_id)
        .bind(claim_token)
        .bind(now)
        .fetch_one(&mut *transaction)
        .await
        .map_err(storage_error)?;
        if !owns_claim {
            transaction.rollback().await.map_err(storage_error)?;
            return Ok(None);
        }
        let admitted = sqlx::query_scalar(
            "UPDATE verification_tasks AS task
             SET entitlement_publication_claim_token =
                     COALESCE(task.entitlement_publication_claim_token, $6),
                 updated_at = $4
             FROM content_lock_deletion_jobs AS deletion,
                  content_lock_deletion_task_snapshot AS snapshot
             WHERE deletion.job_id = $1 AND deletion.state = 'running'
               AND deletion.claimed_by = $2 AND deletion.claim_token = $3
               AND deletion.claim_expires_at >= $4 AND deletion.phase = 'drain_payments'
               AND deletion.force_requested_at IS NULL
               AND snapshot.deletion_job_id = deletion.job_id
               AND snapshot.verification_task_id = task.task_id
               AND snapshot.paykit_admission_required = TRUE
               AND snapshot.resolved_status IS NULL
               AND task.task_id = $5::uuid
               AND task.deletion_job_id = deletion.job_id
               AND task.status IN ('pending', 'in_progress')
             RETURNING task.entitlement_publication_claim_token",
        )
        .bind(deletion_job_id)
        .bind(worker_id)
        .bind(claim_token)
        .bind(now)
        .bind(task_id.to_string())
        .bind(publication_token)
        .fetch_optional(&mut *transaction)
        .await
        .map_err(storage_error)?;
        transaction.commit().await.map_err(storage_error)?;
        Ok(admitted)
    }

    async fn persist_terminal_obligation(
        &self,
        deletion_job_id: Uuid,
        worker_id: &str,
        claim_token: Uuid,
        now: OffsetDateTime,
        task_id: &TaskId,
        transition: PaymentDrainTerminalTransition,
    ) -> Result<bool, ApplicationError> {
        let PaymentDrainTerminalTransition {
            status,
            entitlement_publication_token,
        } = transition;
        if !matches!(
            status,
            VerificationTaskStatus::Completed | VerificationTaskStatus::Expired
        ) {
            return Err(ApplicationError::InvalidVerificationTaskState {
                message: "payment drain transition must be completed or expired".to_owned(),
            });
        }
        let mut transaction = self.pool.begin().await.map_err(storage_error)?;
        let ownership = lock_deletion_ownership(&mut transaction, deletion_job_id).await?;
        if !owns_live_drain_claim(
            ownership.as_ref(),
            worker_id,
            claim_token,
            now,
            "drain_payments",
        ) {
            transaction.rollback().await.map_err(storage_error)?;
            return Ok(false);
        }
        let snapshot = sqlx::query_as::<_, ObligationFenceRow>(
            "SELECT paykit_admission_required, resolved_status
             FROM content_lock_deletion_task_snapshot
             WHERE deletion_job_id = $1 AND verification_task_id = $2::uuid
             FOR UPDATE",
        )
        .bind(deletion_job_id)
        .bind(task_id.to_string())
        .fetch_optional(&mut *transaction)
        .await
        .map_err(storage_error)?;
        if !matches!(
            snapshot,
            Some(ObligationFenceRow {
                paykit_admission_required: Some(true),
                resolved_status: None,
            })
        ) {
            transaction.rollback().await.map_err(storage_error)?;
            return Ok(false);
        }
        let updated = sqlx::query(
            "UPDATE verification_tasks
             SET status = $3, started_at = COALESCE(started_at, $2), completed_at = $2,
                 failure_message = NULL, claimed_by = NULL, claim_token = NULL,
                 claim_expires_at = NULL, next_attempt_at = NULL,
                 last_attempt_error = NULL, entitlement_publication_claim_token = NULL,
                 updated_at = $2
             WHERE task_id = $1::uuid
               AND deletion_job_id = $4
               AND status IN ('pending', 'in_progress')
               AND entitlement_publication_claim_token IS NOT DISTINCT FROM $5",
        )
        .bind(task_id.to_string())
        .bind(now)
        .bind(status_to_database(status))
        .bind(deletion_job_id)
        .bind(entitlement_publication_token)
        .execute(&mut *transaction)
        .await
        .map_err(storage_error)?;
        if updated.rows_affected() != 1 {
            transaction.rollback().await.map_err(storage_error)?;
            return Ok(false);
        }
        let resolved = sqlx::query(
            "UPDATE content_lock_deletion_task_snapshot
             SET resolved_status = $3, resolved_at = $4,
                 final_credential_eligible_at = CASE
                     WHEN $3 = 'completed'
                      AND paykit_admission_required
                      AND NOT had_active_credential_at_cutoff
                     THEN $4
                     ELSE NULL
                 END
             WHERE deletion_job_id = $1 AND verification_task_id = $2::uuid
               AND resolved_status IS NULL",
        )
        .bind(deletion_job_id)
        .bind(task_id.to_string())
        .bind(status_to_database(status))
        .bind(now)
        .execute(&mut *transaction)
        .await
        .map_err(storage_error)?;
        if resolved.rows_affected() != 1 {
            return Err(invalid_deletion(
                "payment drain snapshot resolution lost its fence",
            ));
        }
        transaction.commit().await.map_err(storage_error)?;
        Ok(true)
    }

    async fn all_obligations_terminal(
        &self,
        deletion_job_id: Uuid,
    ) -> Result<bool, ApplicationError> {
        sqlx::query_scalar(
            "SELECT NOT EXISTS (
                SELECT 1 FROM content_lock_deletion_task_snapshot
                WHERE deletion_job_id = $1
                  AND paykit_admission_required = TRUE
                  AND COALESCE(resolved_status, status_at_cutoff)
                      NOT IN ('completed', 'failed', 'expired')
            )",
        )
        .bind(deletion_job_id)
        .fetch_one(&self.pool)
        .await
        .map_err(storage_error)
    }
}

async fn lock_deletion_ownership(
    transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    deletion_job_id: Uuid,
) -> Result<Option<DeletionOwnershipRow>, ApplicationError> {
    sqlx::query_as::<_, DeletionOwnershipRow>(
        "SELECT state, phase, force_requested_at, claimed_by, claim_token, claim_expires_at
         FROM content_lock_deletion_jobs
         WHERE job_id = $1
         FOR UPDATE",
    )
    .bind(deletion_job_id)
    .fetch_optional(&mut **transaction)
    .await
    .map_err(storage_error)
}

fn owns_live_drain_claim(
    ownership: Option<&DeletionOwnershipRow>,
    worker_id: &str,
    claim_token: Uuid,
    now: OffsetDateTime,
    phase: &str,
) -> bool {
    ownership.is_some_and(|ownership| {
        ownership.state == "running"
            && ownership.phase == phase
            && ownership.force_requested_at.is_none()
            && ownership.claimed_by.as_deref() == Some(worker_id)
            && ownership.claim_token == Some(claim_token)
            && ownership
                .claim_expires_at
                .is_some_and(|claim_expires_at| claim_expires_at >= now)
    })
}

fn row_to_obligation(row: ObligationRow) -> Result<PaymentDrainObligation, ApplicationError> {
    Ok(PaymentDrainObligation {
        task_id: TaskId::from_str(&row.task_id.to_string()).map_err(storage_display)?,
        creator: CreatorPubky::from_str(&row.creator).map_err(storage_display)?,
        bundle_id: BundleId::from_str(&row.bundle_id).map_err(storage_display)?,
        lock_resource: PubkyLockResource::from_str(&row.pubky_lock_resource)
            .map_err(storage_display)?,
        criterion_id: row.criterion_id,
        invoice_created_at: row.invoice_created_at,
        payment_deadline: row.payment_deadline,
        status: status_from_database(&row.status)?,
    })
}

fn row_to_summary(row: DrainRow) -> Result<PaymentDrainSummary, ApplicationError> {
    Ok(PaymentDrainSummary {
        status: match row.status.as_str() {
            "active" => PaymentDrainStatus::Active,
            "completed" => PaymentDrainStatus::Completed,
            _ => {
                return Err(invalid_deletion(
                    "persisted Paykit payment drain status is invalid",
                ));
            }
        },
        accepted_count: u64::try_from(row.accepted_count).map_err(storage_display)?,
        terminal_count: u64::try_from(row.terminal_count).map_err(storage_display)?,
        cancellation_enqueued_count: u64::try_from(row.cancellation_enqueued_count)
            .map_err(storage_display)?,
        cleanup_token: PaymentDrainCleanupToken::parse(&row.cleanup_token)
            .ok_or_else(|| invalid_deletion("persisted Paykit cleanup token is invalid"))?,
    })
}

fn summary_counts(summary: &PaymentDrainSummary) -> Result<(i64, i64, i64), ApplicationError> {
    Ok((
        i64::try_from(summary.accepted_count).map_err(storage_display)?,
        i64::try_from(summary.terminal_count).map_err(storage_display)?,
        i64::try_from(summary.cancellation_enqueued_count).map_err(storage_display)?,
    ))
}

fn valid_aggregate_progress(previous: &PaymentDrainSummary, current: &PaymentDrainSummary) -> bool {
    let accepted_delta = previous.accepted_count.checked_sub(current.accepted_count);
    let terminal_delta = current.terminal_count.checked_sub(previous.terminal_count);
    previous.cleanup_token == current.cleanup_token
        && previous.cancellation_enqueued_count == current.cancellation_enqueued_count
        && accepted_delta.is_some()
        && accepted_delta == terminal_delta
        && !(previous.status == PaymentDrainStatus::Completed
            && current.status != PaymentDrainStatus::Completed)
        && ((current.status == PaymentDrainStatus::Completed && current.accepted_count == 0)
            || (current.status == PaymentDrainStatus::Active && current.accepted_count > 0))
}

fn drain_status_to_database(status: PaymentDrainStatus) -> &'static str {
    match status {
        PaymentDrainStatus::Active => "active",
        PaymentDrainStatus::Completed => "completed",
    }
}

fn status_from_database(value: &str) -> Result<VerificationTaskStatus, ApplicationError> {
    match value {
        "pending" => Ok(VerificationTaskStatus::Pending),
        "in_progress" => Ok(VerificationTaskStatus::InProgress),
        "completed" => Ok(VerificationTaskStatus::Completed),
        "failed" => Ok(VerificationTaskStatus::Failed),
        "expired" => Ok(VerificationTaskStatus::Expired),
        _ => Err(ApplicationError::InvalidVerificationTaskState {
            message: "unknown snapshotted verification task status".to_owned(),
        }),
    }
}

fn status_to_database(status: VerificationTaskStatus) -> &'static str {
    match status {
        VerificationTaskStatus::Completed => "completed",
        VerificationTaskStatus::Expired => "expired",
        _ => unreachable!("validated terminal payment drain status"),
    }
}

fn invalid_deletion(message: &str) -> ApplicationError {
    ApplicationError::InvalidContentLockDeletionState {
        message: message.to_owned(),
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
    use std::str::FromStr;

    use base64::Engine;
    use base64::engine::general_purpose::URL_SAFE_NO_PAD;
    use locks_core::ids::TaskId;
    use time::OffsetDateTime;
    use uuid::Uuid;

    use crate::application::models::VerificationTaskStatus;
    use crate::application::ports::{
        PaymentDrainCleanupToken, PaymentDrainRepository, PaymentDrainStatus, PaymentDrainSummary,
        PaymentDrainTerminalTransition,
    };
    use crate::infrastructure::postgres::testing::TestDatabase;

    use super::PostgresPaymentDrainRepository;

    #[tokio::test]
    async fn payment_drain_migration_creates_durable_token_table() {
        let database = TestDatabase::create().await;
        let exists: bool = sqlx::query_scalar(
            "SELECT EXISTS (
                SELECT 1 FROM information_schema.tables
                WHERE table_schema = current_schema()
                  AND table_name = 'content_lock_payment_drains'
            )",
        )
        .fetch_one(database.pool())
        .await
        .unwrap();
        assert!(exists);
        database.cleanup().await;
    }

    #[tokio::test]
    async fn reconciliation_persists_only_monotonic_aggregate_progress_under_live_claim() {
        let database = TestDatabase::create().await;
        let repository = PostgresPaymentDrainRepository::new(database.pool().clone());
        let job_id = Uuid::new_v4();
        let claim_token = Uuid::new_v4();
        let now = OffsetDateTime::now_utc();
        sqlx::query(
            "INSERT INTO content_lock_deletion_jobs
                 (job_id, creator, lock_id, frozen_content_lock, deletion_started_at,
                  state, phase, claimed_by, claim_token, claim_expires_at)
             VALUES ($1, 'creator', 'lock', '{}'::jsonb, $2,
                     'running', 'drain_payments', 'worker', $3, $4)",
        )
        .bind(job_id)
        .bind(now)
        .bind(claim_token)
        .bind(now + time::Duration::minutes(5))
        .execute(database.pool())
        .await
        .unwrap();
        let token = PaymentDrainCleanupToken::parse(&URL_SAFE_NO_PAD.encode([7_u8; 32])).unwrap();
        sqlx::query(
            "INSERT INTO content_lock_payment_drains
                 (deletion_job_id, status, accepted_count, terminal_count,
                  cancellation_enqueued_count, cleanup_token, created_at, updated_at)
             VALUES ($1, 'active', 2, 3, 1, $2, $3, $3)",
        )
        .bind(job_id)
        .bind(token.as_str())
        .bind(now)
        .execute(database.pool())
        .await
        .unwrap();

        let completed = PaymentDrainSummary {
            status: PaymentDrainStatus::Completed,
            accepted_count: 0,
            terminal_count: 5,
            cancellation_enqueued_count: 1,
            cleanup_token: token.clone(),
        };
        assert!(
            repository
                .reconcile_payment_drain(job_id, "worker", claim_token, now, &completed)
                .await
                .unwrap()
        );
        assert_eq!(
            repository.get_payment_drain(job_id).await.unwrap(),
            Some(completed)
        );

        let divergent = PaymentDrainSummary {
            status: PaymentDrainStatus::Completed,
            accepted_count: 0,
            terminal_count: 6,
            cancellation_enqueued_count: 1,
            cleanup_token: token,
        };
        assert!(
            !repository
                .reconcile_payment_drain(job_id, "worker", claim_token, now, &divergent)
                .await
                .unwrap()
        );
        assert!(
            !repository
                .reconcile_payment_drain(job_id, "worker", Uuid::new_v4(), now, &divergent)
                .await
                .unwrap()
        );

        database.cleanup().await;
    }

    #[tokio::test]
    async fn start_phase_replay_persists_monotonic_progress_after_crash_before_phase_advance() {
        let database = TestDatabase::create().await;
        let repository = PostgresPaymentDrainRepository::new(database.pool().clone());
        let job_id = Uuid::new_v4();
        let claim_token = Uuid::new_v4();
        let now = OffsetDateTime::now_utc();
        sqlx::query(
            "INSERT INTO content_lock_deletion_jobs
                 (job_id, creator, lock_id, frozen_content_lock, deletion_started_at,
                  state, phase, claimed_by, claim_token, claim_expires_at)
             VALUES ($1, 'creator', 'lock', '{}'::jsonb, $2,
                     'running', 'start_payment_drain', 'worker', $3, $4)",
        )
        .bind(job_id)
        .bind(now)
        .bind(claim_token)
        .bind(now + time::Duration::minutes(5))
        .execute(database.pool())
        .await
        .unwrap();
        let token = PaymentDrainCleanupToken::parse(&URL_SAFE_NO_PAD.encode([8_u8; 32])).unwrap();
        let active = PaymentDrainSummary {
            status: PaymentDrainStatus::Active,
            accepted_count: 1,
            terminal_count: 0,
            cancellation_enqueued_count: 0,
            cleanup_token: token.clone(),
        };
        assert!(
            repository
                .store_payment_drain(job_id, "worker", claim_token, now, &active)
                .await
                .unwrap()
        );

        let completed = PaymentDrainSummary {
            status: PaymentDrainStatus::Completed,
            accepted_count: 0,
            terminal_count: 1,
            cancellation_enqueued_count: 0,
            cleanup_token: token,
        };
        assert!(
            repository
                .store_payment_drain(job_id, "worker", claim_token, now, &completed)
                .await
                .unwrap()
        );
        assert_eq!(
            repository.get_payment_drain(job_id).await.unwrap(),
            Some(completed)
        );

        database.cleanup().await;
    }

    #[tokio::test]
    async fn force_first_fences_stale_initial_payment_drain_store() {
        let database = TestDatabase::create().await;
        let repository = PostgresPaymentDrainRepository::new(database.pool().clone());
        let job_id = Uuid::new_v4();
        let stale_claim_token = Uuid::new_v4();
        let now = OffsetDateTime::now_utc();
        insert_start_drain_job(database.pool(), job_id, stale_claim_token, now).await;

        let mut force = database.pool().begin().await.unwrap();
        sqlx::query("SELECT job_id FROM content_lock_deletion_jobs WHERE job_id = $1 FOR UPDATE")
            .bind(job_id)
            .fetch_one(&mut *force)
            .await
            .unwrap();
        sqlx::query(
            "UPDATE content_lock_deletion_jobs
             SET force_requested_at = $2, state = 'queued', claimed_by = NULL,
                 claim_token = NULL, claim_expires_at = NULL
             WHERE job_id = $1",
        )
        .bind(job_id)
        .bind(now)
        .execute(&mut *force)
        .await
        .unwrap();

        let summary = PaymentDrainSummary {
            status: PaymentDrainStatus::Active,
            accepted_count: 1,
            terminal_count: 0,
            cancellation_enqueued_count: 0,
            cleanup_token: PaymentDrainCleanupToken::parse(&URL_SAFE_NO_PAD.encode([10_u8; 32]))
                .unwrap(),
        };
        let stale = tokio::spawn(async move {
            repository
                .store_payment_drain(job_id, "worker", stale_claim_token, now, &summary)
                .await
        });
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        assert!(
            !stale.is_finished(),
            "initial drain persistence must wait for the deletion-job ownership row"
        );

        force.commit().await.unwrap();
        assert!(!stale.await.unwrap().unwrap());
        assert_eq!(
            PostgresPaymentDrainRepository::new(database.pool().clone())
                .get_payment_drain(job_id)
                .await
                .unwrap(),
            None
        );

        database.cleanup().await;
    }

    #[tokio::test]
    async fn reclaim_first_fences_stale_initial_payment_drain_store() {
        let database = TestDatabase::create().await;
        let repository = PostgresPaymentDrainRepository::new(database.pool().clone());
        let job_id = Uuid::new_v4();
        let stale_claim_token = Uuid::new_v4();
        let replacement_claim_token = Uuid::new_v4();
        let now = OffsetDateTime::now_utc();
        insert_start_drain_job(database.pool(), job_id, stale_claim_token, now).await;

        let mut reclaim = database.pool().begin().await.unwrap();
        sqlx::query("SELECT job_id FROM content_lock_deletion_jobs WHERE job_id = $1 FOR UPDATE")
            .bind(job_id)
            .fetch_one(&mut *reclaim)
            .await
            .unwrap();
        sqlx::query(
            "UPDATE content_lock_deletion_jobs
             SET claimed_by = 'replacement-worker', claim_token = $2,
                 claim_expires_at = $3
             WHERE job_id = $1",
        )
        .bind(job_id)
        .bind(replacement_claim_token)
        .bind(now + time::Duration::minutes(10))
        .execute(&mut *reclaim)
        .await
        .unwrap();

        let summary = PaymentDrainSummary {
            status: PaymentDrainStatus::Active,
            accepted_count: 1,
            terminal_count: 0,
            cancellation_enqueued_count: 0,
            cleanup_token: PaymentDrainCleanupToken::parse(&URL_SAFE_NO_PAD.encode([11_u8; 32]))
                .unwrap(),
        };
        let stale = tokio::spawn(async move {
            repository
                .store_payment_drain(job_id, "worker", stale_claim_token, now, &summary)
                .await
        });
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        assert!(
            !stale.is_finished(),
            "initial drain persistence must wait for the deletion-job ownership row"
        );

        reclaim.commit().await.unwrap();
        assert!(!stale.await.unwrap().unwrap());
        assert_eq!(
            PostgresPaymentDrainRepository::new(database.pool().clone())
                .get_payment_drain(job_id)
                .await
                .unwrap(),
            None
        );

        database.cleanup().await;
    }

    #[tokio::test]
    async fn concurrent_force_winner_fences_stale_payment_drain_reconciliation() {
        let database = TestDatabase::create().await;
        let repository = PostgresPaymentDrainRepository::new(database.pool().clone());
        let job_id = Uuid::new_v4();
        let stale_claim_token = Uuid::new_v4();
        let now = OffsetDateTime::now_utc();
        let cleanup_token =
            PaymentDrainCleanupToken::parse(&URL_SAFE_NO_PAD.encode([9_u8; 32])).unwrap();
        insert_drain_job(
            database.pool(),
            job_id,
            stale_claim_token,
            now,
            cleanup_token.as_str(),
        )
        .await;

        let mut force = database.pool().begin().await.unwrap();
        sqlx::query("SELECT job_id FROM content_lock_deletion_jobs WHERE job_id = $1 FOR UPDATE")
            .bind(job_id)
            .fetch_one(&mut *force)
            .await
            .unwrap();
        sqlx::query(
            "UPDATE content_lock_deletion_jobs
             SET force_requested_at = $2, state = 'queued', claimed_by = NULL,
                 claim_token = NULL, claim_expires_at = NULL
             WHERE job_id = $1",
        )
        .bind(job_id)
        .bind(now)
        .execute(&mut *force)
        .await
        .unwrap();

        let completed = PaymentDrainSummary {
            status: PaymentDrainStatus::Completed,
            accepted_count: 0,
            terminal_count: 1,
            cancellation_enqueued_count: 0,
            cleanup_token: cleanup_token.clone(),
        };
        let stale = tokio::spawn(async move {
            repository
                .reconcile_payment_drain(job_id, "worker", stale_claim_token, now, &completed)
                .await
        });
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        assert!(
            !stale.is_finished(),
            "reconciliation must wait for the deletion-job ownership row"
        );

        force.commit().await.unwrap();
        assert!(!stale.await.unwrap().unwrap());
        assert_eq!(
            PostgresPaymentDrainRepository::new(database.pool().clone())
                .get_payment_drain(job_id)
                .await
                .unwrap(),
            Some(PaymentDrainSummary {
                status: PaymentDrainStatus::Active,
                accepted_count: 1,
                terminal_count: 0,
                cancellation_enqueued_count: 0,
                cleanup_token,
            })
        );

        database.cleanup().await;
    }

    #[tokio::test]
    async fn concurrent_reclaim_winner_fences_stale_terminal_obligation_persistence() {
        let database = TestDatabase::create().await;
        let repository = PostgresPaymentDrainRepository::new(database.pool().clone());
        let job_id = Uuid::new_v4();
        let task_uuid = Uuid::new_v4();
        let task_id = TaskId::from_str(&task_uuid.to_string()).unwrap();
        let stale_claim_token = Uuid::new_v4();
        let replacement_claim_token = Uuid::new_v4();
        let now = OffsetDateTime::now_utc();
        insert_terminal_obligation(database.pool(), job_id, task_uuid, stale_claim_token, now)
            .await;

        let mut reclaim = database.pool().begin().await.unwrap();
        sqlx::query("SELECT job_id FROM content_lock_deletion_jobs WHERE job_id = $1 FOR UPDATE")
            .bind(job_id)
            .fetch_one(&mut *reclaim)
            .await
            .unwrap();
        sqlx::query(
            "UPDATE content_lock_deletion_jobs
             SET claimed_by = 'replacement-worker', claim_token = $2,
                 claim_expires_at = $3
             WHERE job_id = $1",
        )
        .bind(job_id)
        .bind(replacement_claim_token)
        .bind(now + time::Duration::minutes(10))
        .execute(&mut *reclaim)
        .await
        .unwrap();

        let stale = tokio::spawn(async move {
            repository
                .persist_terminal_obligation(
                    job_id,
                    "worker",
                    stale_claim_token,
                    now,
                    &task_id,
                    PaymentDrainTerminalTransition {
                        status: VerificationTaskStatus::Completed,
                        entitlement_publication_token: None,
                    },
                )
                .await
        });
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        assert!(
            !stale.is_finished(),
            "terminal persistence must wait for the deletion-job ownership row"
        );

        reclaim.commit().await.unwrap();
        assert!(!stale.await.unwrap().unwrap());
        let task_status: String =
            sqlx::query_scalar("SELECT status FROM verification_tasks WHERE task_id = $1")
                .bind(task_uuid)
                .fetch_one(database.pool())
                .await
                .unwrap();
        let resolved_status: Option<String> = sqlx::query_scalar(
            "SELECT resolved_status FROM content_lock_deletion_task_snapshot
             WHERE deletion_job_id = $1 AND verification_task_id = $2",
        )
        .bind(job_id)
        .bind(task_uuid)
        .fetch_one(database.pool())
        .await
        .unwrap();
        assert_eq!(task_status, "pending");
        assert_eq!(resolved_status, None);
        database.cleanup().await;
    }

    async fn insert_start_drain_job(
        pool: &sqlx::PgPool,
        job_id: Uuid,
        claim_token: Uuid,
        now: OffsetDateTime,
    ) {
        sqlx::query(
            "INSERT INTO content_lock_deletion_jobs
                 (job_id, creator, lock_id, frozen_content_lock, deletion_started_at,
                  state, phase, claimed_by, claim_token, claim_expires_at)
             VALUES ($1, 'creator', 'lock', '{}'::jsonb, $2,
                     'running', 'start_payment_drain', 'worker', $3, $4)",
        )
        .bind(job_id)
        .bind(now)
        .bind(claim_token)
        .bind(now + time::Duration::minutes(5))
        .execute(pool)
        .await
        .unwrap();
    }

    async fn insert_drain_job(
        pool: &sqlx::PgPool,
        job_id: Uuid,
        claim_token: Uuid,
        now: OffsetDateTime,
        cleanup_token: &str,
    ) {
        sqlx::query(
            "INSERT INTO content_lock_deletion_jobs
                 (job_id, creator, lock_id, frozen_content_lock, deletion_started_at,
                  state, phase, claimed_by, claim_token, claim_expires_at)
             VALUES ($1, 'creator', 'lock', '{}'::jsonb, $2,
                     'running', 'drain_payments', 'worker', $3, $4)",
        )
        .bind(job_id)
        .bind(now)
        .bind(claim_token)
        .bind(now + time::Duration::minutes(5))
        .execute(pool)
        .await
        .unwrap();
        sqlx::query(
            "INSERT INTO content_lock_payment_drains
                 (deletion_job_id, status, accepted_count, terminal_count,
                  cancellation_enqueued_count, cleanup_token, created_at, updated_at)
             VALUES ($1, 'active', 1, 0, 0, $2, $3, $3)",
        )
        .bind(job_id)
        .bind(cleanup_token)
        .bind(now)
        .execute(pool)
        .await
        .unwrap();
    }

    async fn insert_terminal_obligation(
        pool: &sqlx::PgPool,
        job_id: Uuid,
        task_id: Uuid,
        claim_token: Uuid,
        now: OffsetDateTime,
    ) {
        sqlx::query(
            "INSERT INTO content_lock_deletion_jobs
                 (job_id, creator, lock_id, frozen_content_lock, deletion_started_at,
                  state, phase, claimed_by, claim_token, claim_expires_at)
             VALUES ($1, 'creator', 'lock', '{}'::jsonb, $2,
                     'running', 'drain_payments', 'worker', $3, $4)",
        )
        .bind(job_id)
        .bind(now)
        .bind(claim_token)
        .bind(now + time::Duration::minutes(5))
        .execute(pool)
        .await
        .unwrap();
        sqlx::query(
            "INSERT INTO verification_tasks
                 (task_id, status, submitted_proof_bundle, submitted_at, creator, bundle_id,
                  deletion_job_id)
             VALUES ($1, 'pending', '{}'::jsonb, $2, 'creator', 'bundle', $3)",
        )
        .bind(task_id)
        .bind(now)
        .bind(job_id)
        .execute(pool)
        .await
        .unwrap();
        sqlx::query(
            "INSERT INTO content_lock_deletion_task_snapshot
                 (deletion_job_id, verification_task_id, creator, bundle_id,
                  pubky_lock_resource, criterion_id, status_at_cutoff,
                  paykit_admission_required, payment_in_hours,
                  invoice_created_at, payment_deadline)
             VALUES ($1, $2, 'creator', 'bundle', 'pubkycreator/pub/locks.app/lock.json',
                     'payment', 'pending', TRUE, 1, $3, $4)",
        )
        .bind(job_id)
        .bind(task_id)
        .bind(now)
        .bind(now + time::Duration::hours(1))
        .execute(pool)
        .await
        .unwrap();
    }
}
