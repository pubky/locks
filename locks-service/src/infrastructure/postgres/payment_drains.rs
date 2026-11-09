use std::str::FromStr;

use async_trait::async_trait;
use locks_core::ids::{BundleId, CreatorPubky, PubkyLockResource, TaskId};
use locks_core::verification::VerifiedProofBundle;
use sqlx::{FromRow, PgPool};
use time::OffsetDateTime;
use uuid::Uuid;

use crate::application::errors::ApplicationError;
use crate::application::models::{
    PaymentTaskSnapshot, VerificationTaskStatus, VerificationTerminalReason,
};
use crate::application::ports::{
    PaykitAuthority, PaymentDrainCleanupPreparation, PaymentDrainCleanupToken,
    PaymentDrainObligation, PaymentDrainObligationKind, PaymentDrainRepository, PaymentDrainStatus,
    PaymentDrainSummary, PaymentDrainTerminalTransition, ProviderV2DeletionReconciliation,
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
    obligation_kind: String,
    criterion_id: Option<String>,
    invoice_created_at: Option<OffsetDateTime>,
    payment_deadline: Option<OffsetDateTime>,
    evaluator_revision_at_cutoff: Option<i64>,
    evaluator_snapshot_at_cutoff: Option<serde_json::Value>,
    provider_reconciliation_state: Option<String>,
    status: String,
    entitlement_to_publish: Option<serde_json::Value>,
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
struct CleanupRow {
    status: String,
    accepted_count: i64,
    cleanup_token: String,
    cleanup_state: String,
    pubky_lock_resource: Option<String>,
    paykit_authority_scheme: Option<String>,
    paykit_authority_host: Option<String>,
    paykit_authority_port: Option<i32>,
    paykit_authority_base_path: Option<String>,
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
        summary: &PaymentDrainSummary,
    ) -> Result<bool, ApplicationError> {
        let counts = summary_counts(summary)?;
        let mut transaction = self.pool.begin().await.map_err(storage_error)?;
        let (ownership, now) = lock_deletion_ownership(&mut transaction, deletion_job_id).await?;
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
        summary: &PaymentDrainSummary,
    ) -> Result<bool, ApplicationError> {
        let counts = summary_counts(summary)?;
        let mut transaction = self.pool.begin().await.map_err(storage_error)?;
        let (ownership, now) = lock_deletion_ownership(&mut transaction, deletion_job_id).await?;
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

    async fn prepare_provider_v2_fence(
        &self,
        deletion_job_id: Uuid,
        worker_id: &str,
        claim_token: Uuid,
        lock_resource: &PubkyLockResource,
        authority: &PaykitAuthority,
    ) -> Result<bool, ApplicationError> {
        let mut transaction = self.pool.begin().await.map_err(storage_error)?;
        let (ownership, now) = lock_deletion_ownership(&mut transaction, deletion_job_id).await?;
        if !owns_live_drain_claim(
            ownership.as_ref(),
            worker_id,
            claim_token,
            now,
            "start_payment_drain",
        ) && !owns_live_drain_claim(
            ownership.as_ref(),
            worker_id,
            claim_token,
            now,
            "drain_payments",
        ) {
            transaction.rollback().await.map_err(storage_error)?;
            return Ok(false);
        }
        let port = i32::from(authority.port);
        let existing: Option<(String, String, String, i32, String)> = sqlx::query_as(
            "SELECT pubky_lock_resource, paykit_authority_scheme, paykit_authority_host,
                    paykit_authority_port, paykit_authority_base_path
             FROM content_lock_provider_v2_drain_fences
             WHERE deletion_job_id = $1 FOR UPDATE",
        )
        .bind(deletion_job_id)
        .fetch_optional(&mut *transaction)
        .await
        .map_err(storage_error)?;
        if let Some(existing) = existing {
            if existing
                != (
                    lock_resource.to_string(),
                    authority.scheme.clone(),
                    authority.host.clone(),
                    port,
                    authority.base_path.clone(),
                )
            {
                return Err(invalid_deletion(
                    "Paykit authority or lock resource changed before provider-v2 drain replay",
                ));
            }
        } else {
            let inserted = sqlx::query(
                "INSERT INTO content_lock_provider_v2_drain_fences
                     (deletion_job_id, pubky_lock_resource, paykit_authority_scheme,
                      paykit_authority_host, paykit_authority_port,
                      paykit_authority_base_path, prepared_at)
                 SELECT $1, $2, $3, $4, $5, $6, $7
                 WHERE EXISTS (
                     SELECT 1 FROM content_lock_deletion_task_snapshot
                     WHERE deletion_job_id = $1 AND obligation_kind = 'provider_v2'
                 )",
            )
            .bind(deletion_job_id)
            .bind(lock_resource.to_string())
            .bind(&authority.scheme)
            .bind(&authority.host)
            .bind(port)
            .bind(&authority.base_path)
            .bind(now)
            .execute(&mut *transaction)
            .await
            .map_err(storage_error)?;
            if inserted.rows_affected() != 1 {
                return Err(invalid_deletion(
                    "provider-v2 drain preparation is missing its frozen obligation",
                ));
            }
        }
        transaction.commit().await.map_err(storage_error)?;
        Ok(true)
    }

    async fn store_provider_v2_fence(
        &self,
        deletion_job_id: Uuid,
        worker_id: &str,
        claim_token: Uuid,
        lock_resource: &PubkyLockResource,
        authority: &PaykitAuthority,
    ) -> Result<bool, ApplicationError> {
        let mut transaction = self.pool.begin().await.map_err(storage_error)?;
        let (ownership, now) = lock_deletion_ownership(&mut transaction, deletion_job_id).await?;
        if !owns_live_drain_claim(
            ownership.as_ref(),
            worker_id,
            claim_token,
            now,
            "start_payment_drain",
        ) && !owns_live_drain_claim(
            ownership.as_ref(),
            worker_id,
            claim_token,
            now,
            "drain_payments",
        ) {
            transaction.rollback().await.map_err(storage_error)?;
            return Ok(false);
        }
        let port = i32::from(authority.port);
        let updated = sqlx::query(
            "UPDATE content_lock_payment_drains AS drain
             SET pubky_lock_resource = COALESCE(drain.pubky_lock_resource, $2),
                 paykit_authority_scheme = COALESCE(drain.paykit_authority_scheme, $3),
                 paykit_authority_host = COALESCE(drain.paykit_authority_host, $4),
                 paykit_authority_port = COALESCE(drain.paykit_authority_port, $5),
                 paykit_authority_base_path = COALESCE(drain.paykit_authority_base_path, $6),
                 provider_v2_fence_acknowledged_at =
                     COALESCE(drain.provider_v2_fence_acknowledged_at, $7),
                 updated_at = $7
             WHERE drain.deletion_job_id = $1
               AND EXISTS (
                   SELECT 1 FROM content_lock_provider_v2_drain_fences AS prepared
                   WHERE prepared.deletion_job_id = drain.deletion_job_id
                     AND prepared.pubky_lock_resource = $2
                     AND prepared.paykit_authority_scheme = $3
                     AND prepared.paykit_authority_host = $4
                     AND prepared.paykit_authority_port = $5
                     AND prepared.paykit_authority_base_path = $6
               )
               AND EXISTS (
                   SELECT 1 FROM content_lock_deletion_task_snapshot AS snapshot
                   WHERE snapshot.deletion_job_id = drain.deletion_job_id
                     AND snapshot.obligation_kind = 'provider_v2'
               )
               AND (drain.pubky_lock_resource IS NULL OR drain.pubky_lock_resource = $2)
               AND (drain.paykit_authority_scheme IS NULL OR drain.paykit_authority_scheme = $3)
               AND (drain.paykit_authority_host IS NULL OR drain.paykit_authority_host = $4)
               AND (drain.paykit_authority_port IS NULL OR drain.paykit_authority_port = $5)
               AND (drain.paykit_authority_base_path IS NULL OR drain.paykit_authority_base_path = $6)",
        )
        .bind(deletion_job_id)
        .bind(lock_resource.to_string())
        .bind(&authority.scheme)
        .bind(&authority.host)
        .bind(port)
        .bind(&authority.base_path)
        .bind(now)
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
                    snapshot.obligation_kind, snapshot.criterion_id,
                    snapshot.invoice_created_at, snapshot.payment_deadline,
                    snapshot.evaluator_revision_at_cutoff,
                    snapshot.evaluator_snapshot_at_cutoff,
                    snapshot.provider_reconciliation_state,
                    task.status, task.entitlement_to_publish
             FROM content_lock_deletion_task_snapshot AS snapshot
             JOIN verification_tasks AS task
               ON task.task_id = snapshot.verification_task_id
             WHERE snapshot.deletion_job_id = $1
               AND snapshot.obligation_kind IN ('paykit_v1', 'provider_v2')
               AND (snapshot.obligation_kind = 'provider_v2'
                    OR task.invoice_admission_phase <> 'failed')
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
        task_id: &TaskId,
        entitlement: &VerifiedProofBundle,
    ) -> Result<Option<Uuid>, ApplicationError> {
        let publication_token = Uuid::new_v4();
        let entitlement = serde_json::to_value(entitlement).map_err(storage_display)?;
        let mut transaction = self.pool.begin().await.map_err(storage_error)?;
        let (ownership, now) = lock_deletion_ownership(&mut transaction, deletion_job_id).await?;
        if !owns_live_drain_claim(
            ownership.as_ref(),
            worker_id,
            claim_token,
            now,
            "drain_payments",
        ) {
            transaction.rollback().await.map_err(storage_error)?;
            return Ok(None);
        }
        let admitted = sqlx::query_scalar(
            "UPDATE verification_tasks AS task
             SET status = 'publishing_entitlement',
                 started_at = COALESCE(task.started_at, $4),
                 entitlement_to_publish = COALESCE(task.entitlement_to_publish, $7),
                 entitlement_publication_claim_token =
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
               AND (
                    (task.status IN ('pending', 'in_progress')
                     AND task.entitlement_to_publish IS NULL
                     AND task.entitlement_publication_claim_token IS NULL)
                    OR
                    (task.status = 'publishing_entitlement'
                     AND task.entitlement_to_publish = $7
                     AND task.entitlement_publication_claim_token IS NOT NULL)
               )
             RETURNING task.entitlement_publication_claim_token",
        )
        .bind(deletion_job_id)
        .bind(worker_id)
        .bind(claim_token)
        .bind(now)
        .bind(task_id.to_string())
        .bind(publication_token)
        .bind(entitlement)
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
        task_id: &TaskId,
        transition: PaymentDrainTerminalTransition,
    ) -> Result<bool, ApplicationError> {
        let PaymentDrainTerminalTransition {
            status,
            entitlement_publication_token,
            terminal_reason,
        } = transition;
        if !matches!(
            (status, entitlement_publication_token, terminal_reason),
            (VerificationTaskStatus::Completed, Some(_), None)
                | (VerificationTaskStatus::Expired, None, Some(_))
        ) {
            return Err(ApplicationError::InvalidVerificationTaskState {
                message: "payment drain terminal transition has an invalid canonical shape"
                    .to_owned(),
            });
        }
        let mut transaction = self.pool.begin().await.map_err(storage_error)?;
        let (ownership, now) = lock_deletion_ownership(&mut transaction, deletion_job_id).await?;
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
        let updated = if status == VerificationTaskStatus::Completed {
            sqlx::query(
                "UPDATE verification_tasks
                 SET status = 'completed', completed_at = $2, failure_message = NULL,
                     terminal_reason = NULL, claimed_by = NULL, claim_token = NULL,
                     claim_expires_at = NULL, next_attempt_at = NULL,
                     last_attempt_error = NULL, entitlement_publication_claim_token = NULL,
                     updated_at = $2
                 WHERE task_id = $1::uuid AND deletion_job_id = $3
                   AND status = 'publishing_entitlement'
                   AND entitlement_to_publish IS NOT NULL
                   AND entitlement_publication_claim_token = $4",
            )
            .bind(task_id.to_string())
            .bind(now)
            .bind(deletion_job_id)
            .bind(entitlement_publication_token)
            .execute(&mut *transaction)
            .await
        } else {
            sqlx::query(
                "UPDATE verification_tasks
                 SET status = 'expired', started_at = COALESCE(started_at, $2),
                     completed_at = $2, failure_message = NULL, terminal_reason = $4,
                     entitlement_to_publish = NULL, claimed_by = NULL, claim_token = NULL,
                     claim_expires_at = NULL, next_attempt_at = NULL,
                     last_attempt_error = NULL, entitlement_publication_claim_token = NULL,
                     updated_at = $2
                 WHERE task_id = $1::uuid AND deletion_job_id = $3
                   AND status IN ('pending', 'in_progress')
                   AND entitlement_to_publish IS NULL
                   AND entitlement_publication_claim_token IS NULL",
            )
            .bind(task_id.to_string())
            .bind(now)
            .bind(deletion_job_id)
            .bind(terminal_reason.map(VerificationTerminalReason::as_str))
            .execute(&mut *transaction)
            .await
        }
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

    async fn persist_provider_v2_terminal(
        &self,
        deletion_job_id: Uuid,
        worker_id: &str,
        claim_token: Uuid,
        task_id: &TaskId,
        reconciliation: &ProviderV2DeletionReconciliation,
    ) -> Result<bool, ApplicationError> {
        let valid_shape = (reconciliation.create_conflict
            && reconciliation.create_response.is_none()
            && reconciliation.status.is_none())
            || (!reconciliation.create_conflict
                && reconciliation.create_response.is_some()
                && reconciliation.status.is_some());
        if !valid_shape {
            return Err(invalid_deletion(
                "provider-v2 deletion reconciliation has an invalid closed shape",
            ));
        }
        let mut transaction = self.pool.begin().await.map_err(storage_error)?;
        let (ownership, now) = lock_deletion_ownership(&mut transaction, deletion_job_id).await?;
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
        let response = reconciliation.create_response;
        let status = reconciliation
            .status
            .as_ref()
            .map(serde_json::to_value)
            .transpose()
            .map_err(storage_display)?;
        let terminal_snapshot = reconciliation
            .terminal_snapshot
            .to_json()
            .map_err(storage_display)?;
        let task_updated = sqlx::query(
            "UPDATE verification_tasks AS task
             SET status = CASE WHEN task.status = 'completed' THEN task.status ELSE 'expired' END,
                 started_at = CASE WHEN task.status = 'completed' THEN task.started_at
                                   ELSE COALESCE(task.started_at, $3) END,
                 completed_at = CASE WHEN task.status = 'completed' THEN task.completed_at ELSE $3 END,
                 failure_message = CASE WHEN task.status = 'completed' THEN task.failure_message ELSE NULL END,
                 terminal_reason = CASE WHEN task.status = 'completed' THEN task.terminal_reason
                                        ELSE 'content_lock_deleted' END,
                 evaluator_snapshot = CASE WHEN task.status = 'completed' THEN task.evaluator_snapshot
                     ELSE $4 END,
                 evaluator_revision = CASE WHEN task.status = 'completed' THEN task.evaluator_revision
                                           ELSE snapshot.evaluator_revision_at_cutoff + 1 END,
                 entitlement_to_publish = CASE WHEN task.status = 'completed'
                     THEN task.entitlement_to_publish ELSE NULL END,
                 claimed_by = NULL, claim_token = NULL, claim_expires_at = NULL,
                 next_attempt_at = NULL, last_attempt_error = NULL,
                 entitlement_publication_claim_token = NULL, updated_at = $3
             FROM content_lock_deletion_task_snapshot AS snapshot
             WHERE snapshot.deletion_job_id = $1
               AND snapshot.verification_task_id = task.task_id
               AND snapshot.verification_task_id = $2::uuid
               AND snapshot.obligation_kind = 'provider_v2'
               AND snapshot.resolved_status IS NULL
               AND task.deletion_job_id = $1
               AND (
                    task.status = 'completed'
                    OR (task.status IN ('pending', 'in_progress')
                        AND task.evaluator_revision = snapshot.evaluator_revision_at_cutoff
                        AND task.evaluator_snapshot = snapshot.evaluator_snapshot_at_cutoff
                        AND task.entitlement_to_publish IS NOT DISTINCT FROM snapshot.entitlement_payload_at_cutoff
                        AND task.entitlement_publication_claim_token IS NOT DISTINCT FROM snapshot.publication_token_at_cutoff)
               )",
        )
        .bind(deletion_job_id)
        .bind(task_id.to_string())
        .bind(now)
        .bind(terminal_snapshot)
        .execute(&mut *transaction)
        .await
        .map_err(storage_error)?;
        if task_updated.rows_affected() != 1 {
            transaction.rollback().await.map_err(storage_error)?;
            return Ok(false);
        }
        let resolved = sqlx::query(
            "UPDATE content_lock_deletion_task_snapshot
             SET resolved_status = CASE WHEN status_at_cutoff = 'completed' THEN 'completed' ELSE 'expired' END,
                 resolved_at = $3,
                 provider_reconciliation_state = 'terminal',
                 provider_create_replay_result = $4,
                 provider_create_invoice_created_at = $5,
                 provider_create_proposal_expires_at = $6,
                 provider_create_deadline = $7,
                 provider_status_snapshot = $8,
                 provider_reconciled_at = $3,
                 final_credential_eligible_at = CASE
                     WHEN status_at_cutoff = 'completed'
                      AND NOT had_active_credential_at_cutoff
                     THEN $3
                     ELSE final_credential_eligible_at
                 END
             WHERE deletion_job_id = $1 AND verification_task_id = $2::uuid
               AND obligation_kind = 'provider_v2' AND resolved_status IS NULL
               AND provider_create_replay_result = $4
               AND provider_create_invoice_created_at IS NOT DISTINCT FROM $5
               AND provider_create_proposal_expires_at IS NOT DISTINCT FROM $6
               AND provider_create_deadline IS NOT DISTINCT FROM $7
               AND provider_status_snapshot IS NOT DISTINCT FROM $8
               AND ((provider_reconciliation_state = 'not_found_after_fence'
                     AND $4 = 'conflict' AND $8::jsonb IS NULL)
                    OR (provider_reconciliation_state = 'found'
                        AND $4 = 'exact_replay' AND $8::jsonb IS NOT NULL))",
        )
        .bind(deletion_job_id)
        .bind(task_id.to_string())
        .bind(now)
        .bind(if reconciliation.create_conflict {
            "conflict"
        } else {
            "exact_replay"
        })
        .bind(response.map(|value| value.invoice_created_at))
        .bind(response.map(|value| value.proposal_expires_at))
        .bind(response.map(|value| value.deadline))
        .bind(status)
        .execute(&mut *transaction)
        .await
        .map_err(storage_error)?;
        if resolved.rows_affected() != 1 {
            return Err(invalid_deletion(
                "provider-v2 deletion reconciliation lost its snapshot fence",
            ));
        }
        transaction.commit().await.map_err(storage_error)?;
        Ok(true)
    }

    async fn persist_provider_v2_binding(
        &self,
        deletion_job_id: Uuid,
        worker_id: &str,
        claim_token: Uuid,
        task_id: &TaskId,
        create_conflict: bool,
        response: Option<crate::application::ports::PaymentProviderCreateResponse>,
    ) -> Result<bool, ApplicationError> {
        if create_conflict == response.is_some() {
            return Err(invalid_deletion(
                "provider-v2 binding checkpoint has an invalid closed shape",
            ));
        }
        let mut transaction = self.pool.begin().await.map_err(storage_error)?;
        let (ownership, now) = lock_deletion_ownership(&mut transaction, deletion_job_id).await?;
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
        let replay_result = if create_conflict {
            "conflict"
        } else {
            "exact_replay"
        };
        let updated = sqlx::query(
            "UPDATE content_lock_deletion_task_snapshot
             SET provider_reconciliation_state = CASE
                     WHEN provider_create_replay_result IS NULL AND $3 = 'exact_replay'
                         THEN 'binding_replayed'
                     WHEN provider_create_replay_result IS NULL THEN 'pending'
                     ELSE provider_reconciliation_state END,
                 provider_create_replay_result = COALESCE(provider_create_replay_result, $3),
                 provider_create_invoice_created_at = COALESCE(provider_create_invoice_created_at, $4),
                 provider_create_proposal_expires_at = COALESCE(provider_create_proposal_expires_at, $5),
                 provider_create_deadline = COALESCE(provider_create_deadline, $6)
             WHERE deletion_job_id = $1 AND verification_task_id = $2::uuid
               AND obligation_kind = 'provider_v2' AND resolved_status IS NULL
               AND (provider_create_replay_result IS NULL
                    OR (provider_create_replay_result = $3
                        AND provider_create_invoice_created_at IS NOT DISTINCT FROM $4
                        AND provider_create_proposal_expires_at IS NOT DISTINCT FROM $5
                        AND provider_create_deadline IS NOT DISTINCT FROM $6))",
        )
        .bind(deletion_job_id)
        .bind(task_id.to_string())
        .bind(replay_result)
        .bind(response.map(|value| value.invoice_created_at))
        .bind(response.map(|value| value.proposal_expires_at))
        .bind(response.map(|value| value.deadline))
        .execute(&mut *transaction)
        .await
        .map_err(storage_error)?;
        transaction.commit().await.map_err(storage_error)?;
        Ok(updated.rows_affected() == 1)
    }

    async fn persist_provider_v2_status(
        &self,
        deletion_job_id: Uuid,
        worker_id: &str,
        claim_token: Uuid,
        task_id: &TaskId,
        status: Option<&crate::application::models::PaymentProviderStatus>,
    ) -> Result<bool, ApplicationError> {
        let mut transaction = self.pool.begin().await.map_err(storage_error)?;
        let (ownership, now) = lock_deletion_ownership(&mut transaction, deletion_job_id).await?;
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
        let status_json = status
            .map(serde_json::to_value)
            .transpose()
            .map_err(storage_display)?;
        let updated = sqlx::query(
            "UPDATE content_lock_deletion_task_snapshot
             SET provider_reconciliation_state = CASE
                     WHEN $4::jsonb IS NULL THEN 'not_found_after_fence'
                     ELSE 'found' END,
                 provider_status_snapshot = $4,
                 provider_reconciled_at = $3
             WHERE deletion_job_id = $1 AND verification_task_id = $2::uuid
               AND obligation_kind = 'provider_v2' AND resolved_status IS NULL
               AND provider_create_replay_result IS NOT NULL
               AND (($4::jsonb IS NULL AND provider_create_replay_result = 'conflict')
                    OR $4::jsonb IS NOT NULL)",
        )
        .bind(deletion_job_id)
        .bind(task_id.to_string())
        .bind(now)
        .bind(status_json)
        .execute(&mut *transaction)
        .await
        .map_err(storage_error)?;
        transaction.commit().await.map_err(storage_error)?;
        Ok(updated.rows_affected() == 1)
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
                  AND (
                      COALESCE(resolved_status, status_at_cutoff)
                          NOT IN ('completed', 'failed', 'expired')
                      OR (obligation_kind = 'provider_v2'
                          AND provider_reconciliation_state <> 'terminal')
                  )
            )",
        )
        .bind(deletion_job_id)
        .fetch_one(&self.pool)
        .await
        .map_err(storage_error)
    }

    async fn prepare_payment_drain_cleanup(
        &self,
        deletion_job_id: Uuid,
        worker_id: &str,
        claim_token: Uuid,
        lock_resource: &PubkyLockResource,
        authority: &PaykitAuthority,
    ) -> Result<PaymentDrainCleanupPreparation, ApplicationError> {
        let mut transaction = self.pool.begin().await.map_err(storage_error)?;
        let (ownership, now) = lock_deletion_ownership(&mut transaction, deletion_job_id).await?;
        if !owns_live_drain_claim(
            ownership.as_ref(),
            worker_id,
            claim_token,
            now,
            "purge_operational_state",
        ) {
            transaction.rollback().await.map_err(storage_error)?;
            return Ok(PaymentDrainCleanupPreparation::ClaimLost);
        }
        let row = sqlx::query_as::<_, CleanupRow>(
            "SELECT status, accepted_count, cleanup_token, cleanup_state,
                    pubky_lock_resource, paykit_authority_scheme, paykit_authority_host,
                    paykit_authority_port, paykit_authority_base_path
             FROM content_lock_payment_drains
             WHERE deletion_job_id = $1 FOR UPDATE",
        )
        .bind(deletion_job_id)
        .fetch_optional(&mut *transaction)
        .await
        .map_err(storage_error)?;
        let Some(row) = row else {
            transaction.commit().await.map_err(storage_error)?;
            return Ok(PaymentDrainCleanupPreparation::NotRequired);
        };
        if row.status != "completed" || row.accepted_count != 0 {
            return Err(invalid_deletion(
                "payment drain cleanup requires a completed zero-accepted aggregate",
            ));
        }
        let has_frozen_authority = row.paykit_authority_scheme.is_some()
            || row.paykit_authority_host.is_some()
            || row.paykit_authority_port.is_some()
            || row.paykit_authority_base_path.is_some()
            || row.pubky_lock_resource.is_some();
        if has_frozen_authority
            && (row.pubky_lock_resource.as_deref() != Some(&lock_resource.to_string())
                || row.paykit_authority_scheme.as_deref() != Some(authority.scheme.as_str())
                || row.paykit_authority_host.as_deref() != Some(authority.host.as_str())
                || row.paykit_authority_port != Some(i32::from(authority.port))
                || row.paykit_authority_base_path.as_deref() != Some(authority.base_path.as_str()))
        {
            return Err(invalid_deletion(
                "Paykit authority or lock resource changed before payment drain cleanup",
            ));
        }
        let result = match row.cleanup_state.as_str() {
            "completed" => PaymentDrainCleanupPreparation::Completed,
            "pending" => PaymentDrainCleanupPreparation::Pending(
                PaymentDrainCleanupToken::parse(&row.cleanup_token)
                    .ok_or_else(|| invalid_deletion("persisted Paykit cleanup token is invalid"))?,
            ),
            _ => {
                return Err(invalid_deletion(
                    "persisted payment drain cleanup state is invalid",
                ));
            }
        };
        transaction.commit().await.map_err(storage_error)?;
        Ok(result)
    }

    async fn persist_payment_drain_cleanup(
        &self,
        deletion_job_id: Uuid,
        worker_id: &str,
        claim_token: Uuid,
        cleanup_token: &PaymentDrainCleanupToken,
    ) -> Result<bool, ApplicationError> {
        let mut transaction = self.pool.begin().await.map_err(storage_error)?;
        let (ownership, now) = lock_deletion_ownership(&mut transaction, deletion_job_id).await?;
        if !owns_live_drain_claim(
            ownership.as_ref(),
            worker_id,
            claim_token,
            now,
            "purge_operational_state",
        ) {
            transaction.rollback().await.map_err(storage_error)?;
            return Ok(false);
        }
        let updated = sqlx::query(
            "UPDATE content_lock_payment_drains
             SET cleanup_state = 'completed', cleanup_completed_at = COALESCE(cleanup_completed_at, $2),
                 updated_at = $2
             WHERE deletion_job_id = $1 AND cleanup_token = $3
               AND status = 'completed' AND accepted_count = 0
               AND cleanup_state IN ('pending', 'completed')",
        )
        .bind(deletion_job_id)
        .bind(now)
        .bind(cleanup_token.as_str())
        .execute(&mut *transaction)
        .await
        .map_err(storage_error)?;
        transaction.commit().await.map_err(storage_error)?;
        Ok(updated.rows_affected() == 1)
    }
}

async fn lock_deletion_ownership(
    transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    deletion_job_id: Uuid,
) -> Result<(Option<DeletionOwnershipRow>, OffsetDateTime), ApplicationError> {
    let ownership = sqlx::query_as::<_, DeletionOwnershipRow>(
        "SELECT state, phase, force_requested_at, claimed_by, claim_token, claim_expires_at
         FROM content_lock_deletion_jobs
         WHERE job_id = $1
         FOR UPDATE",
    )
    .bind(deletion_job_id)
    .fetch_optional(&mut **transaction)
    .await
    .map_err(storage_error)?;
    let winner_time = sqlx::query_scalar("SELECT clock_timestamp()")
        .fetch_one(&mut **transaction)
        .await
        .map_err(storage_error)?;
    Ok((ownership, winner_time))
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
                .is_some_and(|claim_expires_at| now < claim_expires_at)
    })
}

fn row_to_obligation(row: ObligationRow) -> Result<PaymentDrainObligation, ApplicationError> {
    let creator = CreatorPubky::from_str(&row.creator).map_err(storage_display)?;
    let bundle_id = BundleId::from_str(&row.bundle_id).map_err(storage_display)?;
    let lock_resource =
        PubkyLockResource::from_str(&row.pubky_lock_resource).map_err(storage_display)?;
    let kind = match row.obligation_kind.as_str() {
        "paykit_v1" => PaymentDrainObligationKind::PaykitV1 {
            criterion_id: row
                .criterion_id
                .ok_or_else(|| invalid_deletion("paykit-v1 obligation is missing criterion_id"))?,
            invoice_created_at: row.invoice_created_at.ok_or_else(|| {
                invalid_deletion("paykit-v1 obligation is missing invoice_created_at")
            })?,
            payment_deadline: row.payment_deadline.ok_or_else(|| {
                invalid_deletion("paykit-v1 obligation is missing payment_deadline")
            })?,
        },
        "provider_v2" => {
            let evaluator_revision =
                u64::try_from(row.evaluator_revision_at_cutoff.ok_or_else(|| {
                    invalid_deletion("provider-v2 obligation is missing evaluator revision")
                })?)
                .map_err(storage_display)?;
            let snapshot =
                PaymentTaskSnapshot::from_json(row.evaluator_snapshot_at_cutoff.ok_or_else(
                    || invalid_deletion("provider-v2 obligation is missing evaluator snapshot"),
                )?)
                .map_err(storage_display)?;
            let frozen = snapshot.as_v1();
            if frozen.creator != creator
                || frozen.bundle_id != bundle_id
                || frozen.pubky_lock_resource != lock_resource
            {
                return Err(invalid_deletion(
                    "provider-v2 frozen evaluator identity diverges from deletion snapshot",
                ));
            }
            PaymentDrainObligationKind::ProviderV2 {
                evaluator_revision,
                snapshot: Box::new(snapshot),
                reconciliation_terminal: row.provider_reconciliation_state.as_deref()
                    == Some("terminal"),
            }
        }
        _ => return Err(invalid_deletion("unknown deletion payment obligation kind")),
    };
    Ok(PaymentDrainObligation {
        task_id: TaskId::from_str(&row.task_id.to_string()).map_err(storage_display)?,
        creator,
        bundle_id,
        lock_resource,
        kind,
        status: status_from_database(&row.status)?,
        entitlement_to_publish: row
            .entitlement_to_publish
            .map(serde_json::from_value)
            .transpose()
            .map_err(storage_display)?,
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
        "publishing_entitlement" => Ok(VerificationTaskStatus::PublishingEntitlement),
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
    use locks_core::ids::{BundleId, CreatorPubky, LockHash, LockId, PubkyLockResource, TaskId};
    use locks_core::lock_policy::PaymentPolicyV2;
    use locks_core::verification::{
        EntitlementLifetime, SUBMITTED_PROOF_BUNDLE_V2_VERSION, SubmittedProofBundleV2,
        VERIFIED_PROOF_BUNDLE_VERSION, VerificationResult, VerifiedProofBundle,
    };
    use time::OffsetDateTime;
    use uuid::Uuid;

    use crate::application::models::{
        PaymentRootState, PaymentTaskSnapshot, PaymentTaskSnapshotV1, VerificationTaskStatus,
        VerificationTerminalReason,
    };
    use crate::application::ports::{
        PaykitAuthority, PaymentDrainCleanupToken, PaymentDrainObligationKind,
        PaymentDrainRepository, PaymentDrainStatus, PaymentDrainSummary,
        PaymentDrainTerminalTransition, ProviderV2DeletionReconciliation,
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
    async fn provider_v2_absence_after_fence_terminalizes_frozen_task_without_entitlement() {
        let database = TestDatabase::create().await;
        let repository = PostgresPaymentDrainRepository::new(database.pool().clone());
        let job_id = Uuid::new_v4();
        let task_uuid = Uuid::new_v4();
        let task_id = TaskId::from_str(&task_uuid.to_string()).unwrap();
        let claim_token = Uuid::new_v4();
        let now = OffsetDateTime::now_utc().replace_nanosecond(0).unwrap();
        let creator =
            CreatorPubky::from_str("pubkytkrq8zmwb8a3m9k15csu3q17qmfgqnp9dskbrg9uq1rydpyxp7qy")
                .unwrap();
        let reader =
            CreatorPubky::from_str("pubkyorhzqdiexwmi6iidktucgud63ufa5nwtsuzdxe176a8izd6jsqky")
                .unwrap();
        let bundle_id = BundleId::from_str("000G40R40M30E209185GR38E1W").unwrap();
        let lock_hash = LockHash::from_bytes([7; 32]);
        let lock_resource = PubkyLockResource::from_str(&format!(
            "{creator}/pub/app.locks/{}.json",
            LockId::from_hash(lock_hash)
        ))
        .unwrap();
        let snapshot = PaymentTaskSnapshotV1::new(
            SubmittedProofBundleV2 {
                version: SUBMITTED_PROOF_BUNDLE_V2_VERSION,
                bundle_id: bundle_id.clone(),
                pubky_lock_resource: lock_resource.clone(),
                reader_pubky: Some(reader),
            },
            [7; 32],
            PaymentPolicyV2 {
                recipient_pubky: creator.clone(),
                amount: "1".to_owned(),
                asset: "BTC".to_owned(),
                minimum_confirmations: 0,
            },
            now,
            300,
        )
        .unwrap();
        let snapshot_json = PaymentTaskSnapshot::V1(snapshot.clone()).to_json().unwrap();
        insert_drain_job(
            database.pool(),
            job_id,
            claim_token,
            now,
            &URL_SAFE_NO_PAD.encode([11_u8; 32]),
        )
        .await;
        sqlx::query(
            "INSERT INTO verification_tasks
                 (task_id, creator, bundle_id, status, submitted_proof_bundle, submitted_at,
                  invoice_admission_phase, evaluator_mode, evaluator_snapshot,
                  evaluator_revision, deletion_job_id)
             VALUES ($1, $2, $3, 'pending', $4, $5, 'ready', 'payment', $6, 0, $7)",
        )
        .bind(task_uuid)
        .bind(creator.to_string())
        .bind(bundle_id.to_string())
        .bind(serde_json::to_value(&snapshot.submitted_proof_bundle).unwrap())
        .bind(now)
        .bind(&snapshot_json)
        .bind(job_id)
        .execute(database.pool())
        .await
        .unwrap();
        sqlx::query(
            "INSERT INTO content_lock_deletion_task_snapshot
                 (deletion_job_id, verification_task_id, creator, bundle_id,
                  pubky_lock_resource, status_at_cutoff, paykit_admission_required,
                  obligation_kind, evaluator_mode_at_cutoff, evaluator_revision_at_cutoff,
                  evaluator_snapshot_at_cutoff, provider_reconciliation_state)
             VALUES ($1, $2, $3, $4, $5, 'pending', TRUE, 'provider_v2',
                     'payment', 0, $6, 'pending')",
        )
        .bind(job_id)
        .bind(task_uuid)
        .bind(creator.to_string())
        .bind(bundle_id.to_string())
        .bind(lock_resource.to_string())
        .bind(&snapshot_json)
        .execute(database.pool())
        .await
        .unwrap();

        let obligations = repository.list_obligations(job_id).await.unwrap();
        assert!(matches!(
            obligations.as_slice(),
            [obligation]
                if matches!(obligation.kind, PaymentDrainObligationKind::ProviderV2 { .. })
        ));
        let authority = PaykitAuthority {
            scheme: "http".to_owned(),
            host: "127.0.0.1".to_owned(),
            port: 8080,
            base_path: "/".to_owned(),
        };
        assert!(
            repository
                .prepare_provider_v2_fence(
                    job_id,
                    "worker",
                    claim_token,
                    &lock_resource,
                    &authority,
                )
                .await
                .unwrap()
        );
        let mismatched_authority = PaykitAuthority {
            port: 8081,
            ..authority.clone()
        };
        assert!(
            repository
                .prepare_provider_v2_fence(
                    job_id,
                    "worker",
                    claim_token,
                    &lock_resource,
                    &mismatched_authority,
                )
                .await
                .is_err(),
            "restart authority mismatch must fail before another provider call"
        );
        assert!(
            repository
                .store_provider_v2_fence(job_id, "worker", claim_token, &lock_resource, &authority,)
                .await
                .unwrap()
        );
        assert!(
            repository
                .persist_provider_v2_binding(job_id, "worker", claim_token, &task_id, true, None,)
                .await
                .unwrap()
        );
        assert!(
            repository
                .persist_provider_v2_status(job_id, "worker", claim_token, &task_id, None,)
                .await
                .unwrap()
        );
        assert!(
            repository
                .persist_provider_v2_binding(job_id, "worker", claim_token, &task_id, true, None,)
                .await
                .unwrap(),
            "replay after durable status checkpoint must preserve reconciliation state"
        );
        let mut terminal_snapshot = PaymentTaskSnapshot::V1(snapshot.clone());
        terminal_snapshot.as_v1_mut().root =
            PaymentRootState::Expired(VerificationTerminalReason::ContentLockDeleted);
        assert!(
            repository
                .persist_provider_v2_terminal(
                    job_id,
                    "worker",
                    claim_token,
                    &task_id,
                    &ProviderV2DeletionReconciliation {
                        create_conflict: true,
                        create_response: None,
                        status: None,
                        terminal_snapshot,
                    },
                )
                .await
                .unwrap()
        );
        let (status, reason, evaluator): (String, Option<String>, serde_json::Value) =
            sqlx::query_as(
                "SELECT status, terminal_reason, evaluator_snapshot
                 FROM verification_tasks WHERE task_id = $1",
            )
            .bind(task_uuid)
            .fetch_one(database.pool())
            .await
            .unwrap();
        assert_eq!(status, "expired");
        assert_eq!(reason.as_deref(), Some("content_lock_deleted"));
        assert_eq!(
            evaluator.get("root"),
            Some(
                &serde_json::to_value(PaymentRootState::Expired(
                    VerificationTerminalReason::ContentLockDeleted,
                ))
                .unwrap()
            )
        );
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
                .reconcile_payment_drain(job_id, "worker", claim_token, &completed)
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
                .reconcile_payment_drain(job_id, "worker", claim_token, &divergent)
                .await
                .unwrap()
        );
        assert!(
            !repository
                .reconcile_payment_drain(job_id, "worker", Uuid::new_v4(), &divergent)
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
                .store_payment_drain(job_id, "worker", claim_token, &active)
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
                .store_payment_drain(job_id, "worker", claim_token, &completed)
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
    async fn publication_intent_replays_exactly_and_completed_row_satisfies_constraints() {
        let database = TestDatabase::create().await;
        let repository = PostgresPaymentDrainRepository::new(database.pool().clone());
        let job_id = Uuid::new_v4();
        let task_uuid = Uuid::new_v4();
        let task_id = TaskId::from_str(&task_uuid.to_string()).unwrap();
        let claim_token = Uuid::new_v4();
        let now = OffsetDateTime::now_utc();
        insert_terminal_obligation(database.pool(), job_id, task_uuid, claim_token, now).await;
        let entitlement = entitlement();

        let first_token = repository
            .begin_entitlement_publication(job_id, "worker", claim_token, &task_id, &entitlement)
            .await
            .unwrap()
            .unwrap();
        let replay_token = repository
            .begin_entitlement_publication(job_id, "worker", claim_token, &task_id, &entitlement)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(replay_token, first_token);
        let obligations = repository.list_obligations(job_id).await.unwrap();
        assert_eq!(obligations.len(), 1);
        assert_eq!(
            obligations[0].status,
            VerificationTaskStatus::PublishingEntitlement
        );
        assert_eq!(
            obligations[0].entitlement_to_publish.as_ref(),
            Some(&entitlement)
        );

        let (status, stored, stored_token): (String, Option<serde_json::Value>, Option<Uuid>) =
            sqlx::query_as(
                "SELECT status, entitlement_to_publish, entitlement_publication_claim_token
                 FROM verification_tasks WHERE task_id = $1",
            )
            .bind(task_uuid)
            .fetch_one(database.pool())
            .await
            .unwrap();
        assert_eq!(status, "publishing_entitlement");
        assert_eq!(stored, Some(serde_json::to_value(&entitlement).unwrap()));
        assert_eq!(stored_token, Some(first_token));

        assert!(
            repository
                .persist_terminal_obligation(
                    job_id,
                    "worker",
                    claim_token,
                    &task_id,
                    PaymentDrainTerminalTransition {
                        status: VerificationTaskStatus::Completed,
                        entitlement_publication_token: Some(first_token),
                        terminal_reason: None,
                    },
                )
                .await
                .unwrap()
        );
        let (status, stored, reason, stored_token): (
            String,
            Option<serde_json::Value>,
            Option<String>,
            Option<Uuid>,
        ) = sqlx::query_as(
            "SELECT status, entitlement_to_publish, terminal_reason,
                    entitlement_publication_claim_token
             FROM verification_tasks WHERE task_id = $1",
        )
        .bind(task_uuid)
        .fetch_one(database.pool())
        .await
        .unwrap();
        assert_eq!(status, "completed");
        assert_eq!(stored, Some(serde_json::to_value(entitlement).unwrap()));
        assert_eq!(reason, None);
        assert_eq!(stored_token, None);
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
                .store_payment_drain(job_id, "worker", stale_claim_token, &summary)
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
                .store_payment_drain(job_id, "worker", stale_claim_token, &summary)
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
                .reconcile_payment_drain(job_id, "worker", stale_claim_token, &completed)
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
                    &task_id,
                    PaymentDrainTerminalTransition {
                        status: VerificationTaskStatus::Completed,
                        entitlement_publication_token: Some(Uuid::new_v4()),
                        terminal_reason: None,
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

    #[tokio::test]
    async fn each_typed_payment_expiry_satisfies_canonical_task_constraints() {
        for reason in [
            VerificationTerminalReason::PaymentRequestRejected,
            VerificationTerminalReason::PaymentRequestCanceled,
            VerificationTerminalReason::ProposalExpired,
            VerificationTerminalReason::PaymentDeadlineExpired,
        ] {
            let database = TestDatabase::create().await;
            let repository = PostgresPaymentDrainRepository::new(database.pool().clone());
            let job_id = Uuid::new_v4();
            let task_uuid = Uuid::new_v4();
            let task_id = TaskId::from_str(&task_uuid.to_string()).unwrap();
            let claim_token = Uuid::new_v4();
            let now = OffsetDateTime::now_utc();
            insert_terminal_obligation(database.pool(), job_id, task_uuid, claim_token, now).await;
            assert!(
                repository
                    .persist_terminal_obligation(
                        job_id,
                        "worker",
                        claim_token,
                        &task_id,
                        PaymentDrainTerminalTransition {
                            status: VerificationTaskStatus::Expired,
                            entitlement_publication_token: None,
                            terminal_reason: Some(reason),
                        },
                    )
                    .await
                    .unwrap()
            );
            let (status, stored_reason): (String, Option<String>) = sqlx::query_as(
                "SELECT status, terminal_reason FROM verification_tasks WHERE task_id = $1",
            )
            .bind(task_uuid)
            .fetch_one(database.pool())
            .await
            .unwrap();
            assert_eq!(status, "expired");
            assert_eq!(stored_reason.as_deref(), Some(reason.as_str()));
            database.cleanup().await;
        }
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
             VALUES ($1, 'pending', '{}'::jsonb, $2,
                     'pubkytkrq8zmwb8a3m9k15csu3q17qmfgqnp9dskbrg9uq1rydpyxp7qy',
                     '000G40R40M30E209185GR38E1W', $3)",
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
                  paykit_admission_required, obligation_kind,
                  invoice_created_at, payment_deadline)
             VALUES ($1, $2,
                     'pubkytkrq8zmwb8a3m9k15csu3q17qmfgqnp9dskbrg9uq1rydpyxp7qy',
                     '000G40R40M30E209185GR38E1W',
                     'pubkytkrq8zmwb8a3m9k15csu3q17qmfgqnp9dskbrg9uq1rydpyxp7qy/pub/app.locks/000G40R40M30E209185GR38E1W8124GK2GAHC5RR34D1P70X3RFG.json',
                     'payment', 'pending', TRUE, 'paykit_v1', $3, $4)",
        )
        .bind(job_id)
        .bind(task_id)
        .bind(now)
        .bind(now + time::Duration::hours(1))
        .execute(pool)
        .await
        .unwrap();
    }

    fn entitlement() -> VerifiedProofBundle {
        let creator = "pubkytkrq8zmwb8a3m9k15csu3q17qmfgqnp9dskbrg9uq1rydpyxp7qy";
        VerifiedProofBundle {
            version: VERIFIED_PROOF_BUNDLE_VERSION,
            bundle_id: BundleId::from_str("000G40R40M30E209185GR38E1W").unwrap(),
            pubky_lock_resource: PubkyLockResource::from_str(&format!(
                "{creator}/pub/app.locks/000G40R40M30E209185GR38E1W8124GK2GAHC5RR34D1P70X3RFG.json"
            ))
            .unwrap(),
            verification_result: VerificationResult { criteria: vec![] },
            entitlement_lifetime: EntitlementLifetime::Unbounded,
        }
    }
}
