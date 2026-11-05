use std::str::FromStr;

use async_trait::async_trait;
use locks_core::ids::{BundleId, CreatorPubky, TaskId};
use locks_core::verification::{
    SatisfiedByV2, VERIFIED_PROOF_BUNDLE_V2_VERSION, VerifiedProofBundleV2,
};
use sqlx::{Connection, FromRow, PgConnection, PgPool};

use crate::application::errors::ApplicationError;
use crate::application::models::{
    ClaimedPaymentTask, PaymentRootState, PaymentTaskRecord, PaymentTaskSnapshot,
    PaymentTaskSnapshotV1, VerificationTerminalReason,
};
use crate::application::ports::{
    AdmitPaymentTask, PaymentPublicationActionGuard, PaymentTaskClaimer, PaymentTaskRepository,
};

use super::proof_admission::{lock_proof_admission, proof_admission_lock_key};
use super::verification_tasks::{reject_deletion_cutoff, status_from_database};

#[derive(Debug, Clone)]
pub struct PostgresPaymentTaskRepository {
    pool: PgPool,
}
impl PostgresPaymentTaskRepository {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }
}

#[derive(Debug, FromRow)]
struct PaymentTaskRow {
    task_id: String,
    creator: String,
    bundle_id: String,
    status: String,
    submitted_proof_bundle: serde_json::Value,
    submitted_at: time::OffsetDateTime,
    evaluator_mode: String,
    evaluator_snapshot: serde_json::Value,
    evaluator_revision: i64,
    completed_at: Option<time::OffsetDateTime>,
    terminal_reason: Option<String>,
    entitlement_to_publish: Option<serde_json::Value>,
    entitlement_publication_claim_token: Option<uuid::Uuid>,
}

const COLUMNS: &str = "task_id::text AS task_id, creator, bundle_id, status, submitted_proof_bundle, submitted_at, evaluator_mode, evaluator_snapshot, evaluator_revision, completed_at, terminal_reason, entitlement_to_publish, entitlement_publication_claim_token";

struct PostgresPaymentPublicationActionGuard {
    connection: Option<PgConnection>,
    task_id: String,
    worker_id: String,
    claim_token: uuid::Uuid,
    expected_revision: i64,
    publication_token: uuid::Uuid,
}

#[async_trait]
impl PaymentPublicationActionGuard for PostgresPaymentPublicationActionGuard {
    async fn complete(&mut self) -> Result<Option<PaymentTaskRecord>, ApplicationError> {
        let connection = self
            .connection
            .as_mut()
            .ok_or_else(|| storage_message("payment publication guard released"))?;
        let row = sqlx::query_as::<_, PaymentTaskRow>(&format!(
            "UPDATE verification_tasks SET status='completed', completed_at=clock_timestamp(), claimed_by=NULL, claim_token=NULL, claim_expires_at=NULL, next_attempt_at=NULL, entitlement_publication_claim_token=NULL, updated_at=clock_timestamp()
             WHERE task_id=$1::uuid AND evaluator_mode='payment' AND status='publishing_entitlement'
               AND claimed_by=$2 AND claim_token=$3 AND claim_expires_at>clock_timestamp()
               AND evaluator_revision=$4 AND entitlement_to_publish IS NOT NULL
               AND entitlement_publication_claim_token=$5 AND deletion_job_id IS NULL
               AND NOT EXISTS(SELECT 1 FROM content_lock_force_deletion_receipts r
                 WHERE r.creator=verification_tasks.creator
                   AND verification_tasks.submitted_proof_bundle->>'pubky_lock_resource'=r.creator||'/pub/app.locks/'||r.lock_id||'.json')
               AND NOT EXISTS (SELECT 1 FROM content_lock_deletion_task_snapshot s WHERE s.verification_task_id=verification_tasks.task_id)
             RETURNING {COLUMNS}"
        )).bind(&self.task_id).bind(&self.worker_id).bind(self.claim_token).bind(self.expected_revision).bind(self.publication_token)
          .fetch_optional(connection).await.map_err(storage_error)?;
        row.map(row_to_task).transpose()
    }
    async fn release(mut self: Box<Self>) -> Result<(), ApplicationError> {
        self.connection
            .take()
            .ok_or_else(|| storage_message("payment publication guard released"))?
            .close()
            .await
            .map_err(storage_error)
    }
}

#[async_trait]
impl PaymentTaskRepository for PostgresPaymentTaskRepository {
    async fn admit_payment_task(
        &self,
        request: AdmitPaymentTask,
    ) -> Result<PaymentTaskRecord, ApplicationError> {
        let creator = request
            .submitted_proof_bundle
            .pubky_lock_resource
            .creator()
            .clone();
        let bundle_id = request.submitted_proof_bundle.bundle_id.clone();
        let lock_id = request
            .submitted_proof_bundle
            .pubky_lock_resource
            .lock_id()
            .clone();
        let mut tx = self.pool.begin().await.map_err(storage_error)?;
        lock_proof_admission(&mut tx, &creator, &lock_id).await?;

        let existing = sqlx::query_as::<_, PaymentTaskRow>(&format!(
            "SELECT {COLUMNS} FROM verification_tasks WHERE creator=$1 AND bundle_id=$2 AND evaluator_mode='payment' FOR UPDATE"
        )).bind(creator.to_string()).bind(bundle_id.to_string()).fetch_optional(&mut *tx).await.map_err(storage_error)?;
        if let Some(existing) = existing {
            let existing = row_to_task(existing)?;
            let candidate = PaymentTaskSnapshotV1::new(
                request.submitted_proof_bundle,
                request.lock_hash,
                request.payment,
                existing.snapshot.as_v1().accepted_at,
                request.verification_window_seconds,
            )
            .map_err(invalid_state)?;
            if existing
                .snapshot
                .as_v1()
                .has_same_admission_binding(&candidate)
            {
                tx.commit().await.map_err(storage_error)?;
                return Ok(existing);
            }
            return Err(ApplicationError::VerificationTaskConflict);
        }
        let handle_exists: bool = sqlx::query_scalar(
            "SELECT EXISTS(SELECT 1 FROM verification_tasks WHERE creator=$1 AND bundle_id=$2)",
        )
        .bind(creator.to_string())
        .bind(bundle_id.to_string())
        .fetch_one(&mut *tx)
        .await
        .map_err(storage_error)?;
        if handle_exists {
            return Err(ApplicationError::VerificationTaskConflict);
        }
        reject_deletion_cutoff(&mut tx, &creator, &lock_id).await?;
        let accepted_at: time::OffsetDateTime = sqlx::query_scalar("SELECT clock_timestamp()")
            .fetch_one(&mut *tx)
            .await
            .map_err(storage_error)?;
        let snapshot = PaymentTaskSnapshotV1::new(
            request.submitted_proof_bundle,
            request.lock_hash,
            request.payment,
            accepted_at,
            request.verification_window_seconds,
        )
        .map_err(invalid_state)?;
        let submitted =
            serde_json::to_value(&snapshot.submitted_proof_bundle).map_err(snapshot_error)?;
        let snapshot_json = PaymentTaskSnapshot::V1(snapshot)
            .to_json()
            .map_err(snapshot_error)?;
        let row = sqlx::query_as::<_, PaymentTaskRow>(&format!(
            "INSERT INTO verification_tasks(task_id,creator,bundle_id,status,submitted_proof_bundle,submitted_at,invoice_admission_phase,evaluator_mode,evaluator_snapshot,evaluator_revision)
             VALUES($1::uuid,$2,$3,'pending',$4,$5,'ready','payment',$6,0)
             ON CONFLICT DO NOTHING RETURNING {COLUMNS}"
        )).bind(request.task_id.to_string()).bind(creator.to_string()).bind(bundle_id.to_string()).bind(submitted).bind(accepted_at).bind(snapshot_json)
          .fetch_optional(&mut *tx).await.map_err(storage_error)?;
        let Some(row) = row else {
            return Err(ApplicationError::VerificationTaskConflict);
        };
        tx.commit().await.map_err(storage_error)?;
        row_to_task(row)
    }

    async fn get_payment_task_by_handle(
        &self,
        creator: &CreatorPubky,
        bundle_id: &BundleId,
    ) -> Result<Option<PaymentTaskRecord>, ApplicationError> {
        sqlx::query_as::<_, PaymentTaskRow>(&format!("SELECT {COLUMNS} FROM verification_tasks WHERE creator=$1 AND bundle_id=$2 AND evaluator_mode='payment'"))
            .bind(creator.to_string()).bind(bundle_id.to_string()).fetch_optional(&self.pool).await.map_err(storage_error)?
            .map(row_to_task).transpose()
    }
}

#[async_trait]
impl PaymentTaskClaimer for PostgresPaymentTaskRepository {
    async fn claim_next_payment_task(
        &self,
        worker_id: &str,
        now: time::OffsetDateTime,
        claim_expires_at: time::OffsetDateTime,
    ) -> Result<Option<ClaimedPaymentTask>, ApplicationError> {
        if claim_expires_at <= now {
            return Err(invalid_state("payment claim expiry must follow claim time"));
        }
        let token = uuid::Uuid::new_v4();
        let row = sqlx::query_as::<_, PaymentTaskRow>(
            "WITH candidate AS MATERIALIZED (
               SELECT task_id FROM verification_tasks
               WHERE evaluator_mode='payment' AND evaluator_snapshot IS NOT NULL AND deletion_job_id IS NULL
               AND NOT EXISTS(SELECT 1 FROM content_lock_force_deletion_receipts r
                 WHERE r.creator=verification_tasks.creator
                   AND verification_tasks.submitted_proof_bundle->>'pubky_lock_resource'=r.creator||'/pub/app.locks/'||r.lock_id||'.json')
                 AND (
                   (evaluator_snapshot->'root'->>'status'='pending' AND ((status='pending' AND (next_attempt_at IS NULL OR next_attempt_at<=clock_timestamp())) OR (status='in_progress' AND claim_expires_at<=clock_timestamp())))
                   OR (evaluator_snapshot->'root'->>'status'='satisfied' AND status='publishing_entitlement' AND (claim_expires_at IS NULL OR claim_expires_at<=clock_timestamp()) AND (next_attempt_at IS NULL OR next_attempt_at<=clock_timestamp()))
                   OR (evaluator_snapshot->'root'->>'status' IN ('satisfied','expired','failed') AND status='in_progress' AND (claim_expires_at IS NULL OR claim_expires_at<=clock_timestamp()))
                 )
                 AND NOT EXISTS(SELECT 1 FROM content_lock_deletion_task_snapshot s WHERE s.verification_task_id=verification_tasks.task_id)
               ORDER BY submitted_at,task_id FOR UPDATE SKIP LOCKED LIMIT 1
             )
             UPDATE verification_tasks t SET status=CASE WHEN t.status='publishing_entitlement' THEN t.status ELSE 'in_progress' END,
               claimed_by=$1,claim_token=$2,claim_expires_at=clock_timestamp()+($3-$4),started_at=COALESCE(started_at,clock_timestamp()),next_attempt_at=NULL,attempt_count=attempt_count+1,updated_at=clock_timestamp()
             FROM candidate WHERE t.task_id=candidate.task_id
             RETURNING t.task_id::text AS task_id,t.creator,t.bundle_id,t.status,t.submitted_proof_bundle,t.submitted_at,t.evaluator_mode,t.evaluator_snapshot,t.evaluator_revision,t.completed_at,t.terminal_reason,t.entitlement_to_publish,t.entitlement_publication_claim_token"
        ).bind(worker_id).bind(token).bind(claim_expires_at).bind(now).fetch_optional(&self.pool).await.map_err(storage_error)?;
        row.map(row_to_task).transpose().map(|task| {
            task.map(|task| ClaimedPaymentTask {
                task,
                claim_token: token,
            })
        })
    }

    async fn persist_claimed_payment_snapshot(
        &self,
        task_id: &TaskId,
        worker_id: &str,
        claim_token: &uuid::Uuid,
        _now: time::OffsetDateTime,
        expected_revision: u64,
        snapshot: PaymentTaskSnapshot,
    ) -> Result<Option<PaymentTaskRecord>, ApplicationError> {
        let expected = i64::try_from(expected_revision)
            .map_err(|_| invalid_state("payment revision exceeds storage"))?;
        let mut tx = self.pool.begin().await.map_err(storage_error)?;
        let current = sqlx::query_as::<_, PaymentTaskRow>(&format!(
            "SELECT {COLUMNS} FROM verification_tasks WHERE task_id=$1::uuid AND evaluator_mode='payment' AND status='in_progress' AND claimed_by=$2 AND claim_token=$3 AND claim_expires_at>clock_timestamp() AND evaluator_revision=$4 AND deletion_job_id IS NULL
               AND NOT EXISTS(SELECT 1 FROM content_lock_force_deletion_receipts r
                 WHERE r.creator=verification_tasks.creator
                   AND verification_tasks.submitted_proof_bundle->>'pubky_lock_resource'=r.creator||'/pub/app.locks/'||r.lock_id||'.json') FOR UPDATE"
        )).bind(task_id.to_string()).bind(worker_id).bind(claim_token).bind(expected).fetch_optional(&mut *tx).await.map_err(storage_error)?;
        let Some(current) = current else {
            tx.rollback().await.map_err(storage_error)?;
            return Ok(None);
        };
        let current = row_to_task(current)?;
        let database_now: time::OffsetDateTime = sqlx::query_scalar("SELECT clock_timestamp()")
            .fetch_one(&mut *tx)
            .await
            .map_err(storage_error)?;
        snapshot
            .as_v1()
            .validate_progress_from(current.snapshot.as_v1())
            .map_err(invalid_state)?;
        let root = snapshot.as_v1().root;
        let (status, reason, entitlement, token, retain): (
            &str,
            Option<&str>,
            Option<serde_json::Value>,
            Option<uuid::Uuid>,
            bool,
        ) = match root {
            PaymentRootState::Pending => ("in_progress", None, None, None, true),
            PaymentRootState::Satisfied => {
                let s = snapshot.as_v1();
                let value = VerifiedProofBundleV2 {
                    version: VERIFIED_PROOF_BUNDLE_V2_VERSION,
                    creator: s.creator.clone(),
                    reader: s.reader.clone(),
                    bundle_id: s.bundle_id.clone(),
                    pubky_lock_resource: s.pubky_lock_resource.clone(),
                    verified_at: database_now,
                    satisfied_by: vec![SatisfiedByV2::Payment],
                };
                value
                    .validate()
                    .map_err(|e| invalid_state(format!("invalid payment entitlement: {e}")))?;
                (
                    "publishing_entitlement",
                    None,
                    Some(serde_json::to_value(value).map_err(snapshot_error)?),
                    Some(uuid::Uuid::new_v4()),
                    true,
                )
            }
            PaymentRootState::Expired(reason) => {
                ("expired", Some(reason.as_str()), None, None, false)
            }
            PaymentRootState::Failed(reason) => {
                ("failed", Some(reason.as_str()), None, None, false)
            }
        };
        let snapshot_json = snapshot.to_json().map_err(snapshot_error)?;
        let row=sqlx::query_as::<_,PaymentTaskRow>(&format!(
            "UPDATE verification_tasks SET evaluator_snapshot=$5,evaluator_revision=evaluator_revision+1,status=$6,terminal_reason=$7,failure_message=NULL,completed_at=CASE WHEN $8 THEN NULL ELSE $4 END,entitlement_to_publish=$9,entitlement_publication_claim_token=$10,claimed_by=CASE WHEN $8 THEN claimed_by ELSE NULL END,claim_token=CASE WHEN $8 THEN claim_token ELSE NULL END,claim_expires_at=CASE WHEN $8 THEN claim_expires_at ELSE NULL END,next_attempt_at=NULL,updated_at=$4
             WHERE task_id=$1::uuid AND evaluator_mode='payment' AND status='in_progress' AND claimed_by=$2 AND claim_token=$3 AND claim_expires_at>clock_timestamp() AND evaluator_revision=$11 AND deletion_job_id IS NULL
               AND NOT EXISTS(SELECT 1 FROM content_lock_force_deletion_receipts r
                 WHERE r.creator=verification_tasks.creator
                   AND verification_tasks.submitted_proof_bundle->>'pubky_lock_resource'=r.creator||'/pub/app.locks/'||r.lock_id||'.json')
             RETURNING {COLUMNS}"
        )).bind(task_id.to_string()).bind(worker_id).bind(claim_token).bind(database_now).bind(snapshot_json).bind(status).bind(reason).bind(retain).bind(entitlement).bind(token).bind(expected)
          .fetch_optional(&mut *tx).await.map_err(storage_error)?;
        tx.commit().await.map_err(storage_error)?;
        row.map(row_to_task).transpose()
    }

    async fn schedule_payment_task_retry(
        &self,
        task_id: &TaskId,
        worker_id: &str,
        claim_token: &uuid::Uuid,
        caller_now: time::OffsetDateTime,
        expected_revision: u64,
        snapshot: PaymentTaskSnapshot,
        next_attempt_at: time::OffsetDateTime,
    ) -> Result<Option<PaymentTaskRecord>, ApplicationError> {
        let delay = next_attempt_at - caller_now;
        if !delay.is_positive() {
            return Err(invalid_state("payment retry delay must be positive"));
        }
        let expected = i64::try_from(expected_revision)
            .map_err(|_| invalid_state("payment revision exceeds storage"))?;
        let mut tx = self.pool.begin().await.map_err(storage_error)?;
        let current=sqlx::query_as::<_,PaymentTaskRow>(&format!("SELECT {COLUMNS} FROM verification_tasks WHERE task_id=$1::uuid AND evaluator_mode='payment' AND status='in_progress' AND claimed_by=$2 AND claim_token=$3 AND claim_expires_at>clock_timestamp() AND evaluator_revision=$4 AND deletion_job_id IS NULL
               AND NOT EXISTS(SELECT 1 FROM content_lock_force_deletion_receipts r
                 WHERE r.creator=verification_tasks.creator
                   AND verification_tasks.submitted_proof_bundle->>'pubky_lock_resource'=r.creator||'/pub/app.locks/'||r.lock_id||'.json') FOR UPDATE"))
            .bind(task_id.to_string()).bind(worker_id).bind(claim_token).bind(expected).fetch_optional(&mut *tx).await.map_err(storage_error)?;
        let Some(current) = current else {
            tx.rollback().await.map_err(storage_error)?;
            return Ok(None);
        };
        let current = row_to_task(current)?;
        snapshot
            .as_v1()
            .validate_progress_from(current.snapshot.as_v1())
            .map_err(invalid_state)?;
        if snapshot.as_v1().root != PaymentRootState::Pending {
            return Err(invalid_state("payment retry requires pending root"));
        }
        let now: time::OffsetDateTime = sqlx::query_scalar("SELECT clock_timestamp()")
            .fetch_one(&mut *tx)
            .await
            .map_err(storage_error)?;
        let due = (now + delay).min(snapshot.as_v1().deadline_at);
        let value = snapshot.to_json().map_err(snapshot_error)?;
        let row=sqlx::query_as::<_,PaymentTaskRow>(&format!("UPDATE verification_tasks SET evaluator_snapshot=$5,evaluator_revision=evaluator_revision+1,status='pending',started_at=NULL,claimed_by=NULL,claim_token=NULL,claim_expires_at=NULL,next_attempt_at=$6,updated_at=$4 WHERE task_id=$1::uuid AND evaluator_mode='payment' AND status='in_progress' AND claimed_by=$2 AND claim_token=$3 AND claim_expires_at>clock_timestamp() AND evaluator_revision=$7 AND deletion_job_id IS NULL
               AND NOT EXISTS(SELECT 1 FROM content_lock_force_deletion_receipts r
                 WHERE r.creator=verification_tasks.creator
                   AND verification_tasks.submitted_proof_bundle->>'pubky_lock_resource'=r.creator||'/pub/app.locks/'||r.lock_id||'.json') RETURNING {COLUMNS}"))
            .bind(task_id.to_string()).bind(worker_id).bind(claim_token).bind(now).bind(value).bind(due).bind(expected).fetch_optional(&mut *tx).await.map_err(storage_error)?;
        tx.commit().await.map_err(storage_error)?;
        row.map(row_to_task).transpose()
    }

    async fn release_claimed_payment_task(
        &self,
        task_id: &TaskId,
        worker_id: &str,
        claim_token: &uuid::Uuid,
    ) -> Result<bool, ApplicationError> {
        let result=sqlx::query("UPDATE verification_tasks SET status=CASE WHEN status='in_progress' AND evaluator_snapshot->'root'->>'status'='pending' THEN 'pending' ELSE status END,started_at=CASE WHEN status='in_progress' AND evaluator_snapshot->'root'->>'status'='pending' THEN NULL ELSE started_at END,claimed_by=NULL,claim_token=NULL,claim_expires_at=NULL,updated_at=clock_timestamp() WHERE task_id=$1::uuid AND evaluator_mode='payment' AND claimed_by=$2 AND claim_token=$3 AND deletion_job_id IS NULL AND NOT EXISTS(SELECT 1 FROM content_lock_force_deletion_receipts r WHERE r.creator=verification_tasks.creator AND verification_tasks.submitted_proof_bundle->>'pubky_lock_resource'=r.creator||'/pub/app.locks/'||r.lock_id||'.json') AND NOT EXISTS(SELECT 1 FROM content_lock_deletion_task_snapshot s WHERE s.verification_task_id=verification_tasks.task_id)").bind(task_id.to_string()).bind(worker_id).bind(claim_token).execute(&self.pool).await.map_err(storage_error)?;
        Ok(result.rows_affected() == 1)
    }

    async fn acquire_payment_publication_action(
        &self,
        task_id: &TaskId,
        worker_id: &str,
        claim_token: &uuid::Uuid,
        expected_revision: u64,
        publication_token: uuid::Uuid,
    ) -> Result<Option<Box<dyn PaymentPublicationActionGuard>>, ApplicationError> {
        let expected = i64::try_from(expected_revision)
            .map_err(|_| invalid_state("payment revision exceeds storage"))?;
        let identity=sqlx::query_as::<_,PaymentTaskRow>(&format!("SELECT {COLUMNS} FROM verification_tasks WHERE task_id=$1::uuid AND evaluator_mode='payment'" )).bind(task_id.to_string()).fetch_optional(&self.pool).await.map_err(storage_error)?.map(row_to_task).transpose()?;
        let Some(identity) = identity else {
            return Ok(None);
        };
        let snapshot = identity.snapshot.as_v1();
        let key =
            proof_admission_lock_key(&snapshot.creator, snapshot.pubky_lock_resource.lock_id());
        let mut pooled = self.pool.acquire().await.map_err(storage_error)?;
        pooled.close_on_drop();
        sqlx::query("SELECT pg_advisory_lock(hashtextextended($1,0))")
            .bind(key)
            .execute(&mut *pooled)
            .await
            .map_err(storage_error)?;
        let mut connection = pooled.detach();
        let live:bool=sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM verification_tasks WHERE task_id=$1::uuid AND evaluator_mode='payment' AND status='publishing_entitlement' AND claimed_by=$2 AND claim_token=$3 AND claim_expires_at>clock_timestamp() AND evaluator_revision=$4 AND entitlement_publication_claim_token=$5 AND deletion_job_id IS NULL
               AND NOT EXISTS(SELECT 1 FROM content_lock_force_deletion_receipts r
                 WHERE r.creator=verification_tasks.creator
                   AND verification_tasks.submitted_proof_bundle->>'pubky_lock_resource'=r.creator||'/pub/app.locks/'||r.lock_id||'.json') AND NOT EXISTS(SELECT 1 FROM content_lock_deletion_task_snapshot s WHERE s.verification_task_id=verification_tasks.task_id))").bind(task_id.to_string()).bind(worker_id).bind(claim_token).bind(expected).bind(publication_token).fetch_one(&mut connection).await.map_err(storage_error)?;
        if !live {
            connection.close().await.map_err(storage_error)?;
            return Ok(None);
        }
        Ok(Some(Box::new(PostgresPaymentPublicationActionGuard {
            connection: Some(connection),
            task_id: task_id.to_string(),
            worker_id: worker_id.to_owned(),
            claim_token: *claim_token,
            expected_revision: expected,
            publication_token,
        })))
    }

    async fn complete_claimed_payment_publication(
        &self,
        task_id: &TaskId,
        worker_id: &str,
        claim_token: &uuid::Uuid,
        _now: time::OffsetDateTime,
        expected_revision: u64,
        publication_token: uuid::Uuid,
    ) -> Result<Option<PaymentTaskRecord>, ApplicationError> {
        let expected = i64::try_from(expected_revision)
            .map_err(|_| invalid_state("payment revision exceeds storage"))?;
        let row=sqlx::query_as::<_,PaymentTaskRow>(&format!("UPDATE verification_tasks SET status='completed',completed_at=clock_timestamp(),claimed_by=NULL,claim_token=NULL,claim_expires_at=NULL,next_attempt_at=NULL,entitlement_publication_claim_token=NULL,updated_at=clock_timestamp() WHERE task_id=$1::uuid AND evaluator_mode='payment' AND status='publishing_entitlement' AND claimed_by=$2 AND claim_token=$3 AND claim_expires_at>clock_timestamp() AND evaluator_revision=$4 AND entitlement_to_publish IS NOT NULL AND entitlement_publication_claim_token=$5 AND deletion_job_id IS NULL
               AND NOT EXISTS(SELECT 1 FROM content_lock_force_deletion_receipts r
                 WHERE r.creator=verification_tasks.creator
                   AND verification_tasks.submitted_proof_bundle->>'pubky_lock_resource'=r.creator||'/pub/app.locks/'||r.lock_id||'.json') AND NOT EXISTS(SELECT 1 FROM content_lock_deletion_task_snapshot s WHERE s.verification_task_id=verification_tasks.task_id) RETURNING {COLUMNS}" )).bind(task_id.to_string()).bind(worker_id).bind(claim_token).bind(expected).bind(publication_token).fetch_optional(&self.pool).await.map_err(storage_error)?;
        row.map(row_to_task).transpose()
    }
}

fn row_to_task(row: PaymentTaskRow) -> Result<PaymentTaskRecord, ApplicationError> {
    if row.evaluator_mode != "payment" {
        return Err(storage_message(
            "non-payment evaluator row reached payment decoder",
        ));
    }
    let task_id = TaskId::from_str(&row.task_id)
        .map_err(|e| storage_message(format!("invalid payment task id: {e}")))?;
    let snapshot = PaymentTaskSnapshot::from_json(row.evaluator_snapshot)
        .map_err(|e| storage_message(format!("invalid payment snapshot: {e}")))?;
    let identity = snapshot.as_v1();
    if row.creator != identity.creator.to_string()
        || row.bundle_id != identity.bundle_id.to_string()
        || row.submitted_at != identity.accepted_at
        || row.submitted_proof_bundle
            != serde_json::to_value(&identity.submitted_proof_bundle).map_err(snapshot_error)?
    {
        return Err(storage_message(
            "payment snapshot diverges from relational identity",
        ));
    }
    let revision = u64::try_from(row.evaluator_revision)
        .map_err(|_| storage_message("negative payment evaluator revision"))?;
    let entitlement = row
        .entitlement_to_publish
        .map(|value| {
            serde_json::from_value::<VerifiedProofBundleV2>(value)
                .map_err(|e| storage_message(format!("invalid payment entitlement: {e}")))
        })
        .transpose()?;
    if let Some(value) = entitlement.as_ref() {
        value
            .validate()
            .map_err(|e| storage_message(format!("invalid payment entitlement: {e}")))?;
        if value.creator != identity.creator
            || value.reader != identity.reader
            || value.bundle_id != identity.bundle_id
            || value.pubky_lock_resource != identity.pubky_lock_resource
            || value.satisfied_by != [SatisfiedByV2::Payment]
        {
            return Err(storage_message(
                "payment entitlement diverges from snapshot",
            ));
        }
    }
    let status = status_from_database(&row.status)?;
    let terminal = row
        .terminal_reason
        .as_deref()
        .and_then(VerificationTerminalReason::from_storage_value);
    if row.terminal_reason.is_some() && terminal.is_none() {
        return Err(storage_message("unknown payment terminal reason"));
    }
    if terminal != identity.root.terminal_reason() {
        return Err(storage_message(
            "payment terminal reason diverges from root",
        ));
    }
    Ok(PaymentTaskRecord {
        task_id,
        snapshot,
        evaluator_revision: revision,
        status,
        entitlement_to_publish: entitlement,
        entitlement_publication_token: row.entitlement_publication_claim_token,
        completed_at: row.completed_at,
    })
}
fn invalid_state(message: impl Into<String>) -> ApplicationError {
    ApplicationError::InvalidVerificationTaskState {
        message: message.into(),
    }
}
fn storage_message(message: impl Into<String>) -> ApplicationError {
    ApplicationError::Storage {
        message: message.into(),
    }
}
fn storage_error(error: sqlx::Error) -> ApplicationError {
    storage_message(format!("payment task persistence failed: {error}"))
}
fn snapshot_error(error: serde_json::Error) -> ApplicationError {
    storage_message(format!("payment task serialization failed: {error}"))
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use locks_core::ids::{BundleId, CreatorPubky, LockHash, LockId, PubkyLockResource, TaskId};
    use locks_core::lock_policy::PaymentPolicyV2;
    use locks_core::verification::{SUBMITTED_PROOF_BUNDLE_V2_VERSION, SubmittedProofBundleV2};

    use super::*;
    use crate::application::models::{
        BitcoinOutpointV2, PaymentAssetV2, PaymentProviderStatus, PaymentRequestStateV2,
        PaymentSampleStatusV2, PaymentSampleV2, PaymentStateV2, VerificationTaskStatus,
    };
    use crate::application::payment_v2_reducer::reduce_payment_snapshot;
    use crate::application::ports::{PaymentTaskClaimer, PaymentTaskRepository};
    use crate::infrastructure::postgres::testing::TestDatabase;

    const CREATOR: &str = "pubkytkrq8zmwb8a3m9k15csu3q17qmfgqnp9dskbrg9uq1rydpyxp7qy";
    const READER: &str = "pubkyorhzqdiexwmi6iidktucgud63ufa5nwtsuzdxe176a8izd6jsqky";
    const BUNDLE: &str = "000G40R40M30E209185GR38E1W";

    fn request(task: &str) -> AdmitPaymentTask {
        let creator = CreatorPubky::from_str(CREATOR).unwrap();
        let hash = LockHash::from_bytes([7; 32]);
        let resource = PubkyLockResource::from_str(&format!(
            "{CREATOR}/pub/app.locks/{}.json",
            LockId::from_hash(hash)
        ))
        .unwrap();
        AdmitPaymentTask {
            task_id: TaskId::from_str(task).unwrap(),
            submitted_proof_bundle: SubmittedProofBundleV2 {
                version: SUBMITTED_PROOF_BUNDLE_V2_VERSION,
                bundle_id: BundleId::from_str(BUNDLE).unwrap(),
                pubky_lock_resource: resource,
                reader_pubky: Some(CreatorPubky::from_str(READER).unwrap()),
            },
            lock_hash: [7; 32],
            payment: PaymentPolicyV2 {
                recipient_pubky: creator,
                amount: "0001".to_owned(),
                asset: "BTC".to_owned(),
                minimum_confirmations: 0,
            },
            verification_window_seconds: 300,
        }
    }

    #[tokio::test]
    async fn payment_admission_replay_and_claim_are_mode_isolated_and_durable() {
        let database = TestDatabase::create().await;
        let repository = PostgresPaymentTaskRepository::new(database.pool().clone());
        let first = repository
            .admit_payment_task(request("123e4567-e89b-42d3-a456-426614174001"))
            .await
            .unwrap();
        assert_eq!(first.snapshot.as_v1().payment.amount, "0001");
        let replay = repository
            .admit_payment_task(request("123e4567-e89b-42d3-a456-426614174002"))
            .await
            .unwrap();
        assert_eq!(replay.task_id, first.task_id);
        let now = time::OffsetDateTime::now_utc();
        let claimed = repository
            .claim_next_payment_task("worker", now, now + time::Duration::seconds(30))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(claimed.task.task_id, first.task_id);
        assert_eq!(claimed.task.status, VerificationTaskStatus::InProgress);

        let next = now + time::Duration::seconds(5);
        let mut progressed = claimed.task.snapshot;
        progressed.as_v1_mut().next_due_at = Some(next);
        repository
            .schedule_payment_task_retry(
                &first.task_id,
                "worker",
                &claimed.claim_token,
                now,
                0,
                progressed,
                next,
            )
            .await
            .unwrap()
            .unwrap();
        let progressed_replay = repository
            .admit_payment_task(request("123e4567-e89b-42d3-a456-426614174003"))
            .await
            .unwrap();
        assert_eq!(progressed_replay.task_id, first.task_id);
        assert_eq!(progressed_replay.evaluator_revision, 1);
        database.cleanup().await;
    }

    #[tokio::test]
    async fn permanent_force_receipt_fences_payment_claims_and_retry_transitions() {
        let database = TestDatabase::create().await;
        let repository = PostgresPaymentTaskRepository::new(database.pool().clone());
        let admitted = repository
            .admit_payment_task(request("123e4567-e89b-42d3-a456-426614174011"))
            .await
            .unwrap();
        let now = time::OffsetDateTime::now_utc();
        let claimed = repository
            .claim_next_payment_task("worker", now, now + time::Duration::seconds(30))
            .await
            .unwrap()
            .unwrap();
        let snapshot = claimed.task.snapshot.as_v1();
        sqlx::query(
            "INSERT INTO content_lock_force_deletion_receipts(creator,lock_id,forced_at,completed_at) VALUES($1,$2,clock_timestamp(),clock_timestamp())",
        )
        .bind(snapshot.creator.to_string())
        .bind(snapshot.pubky_lock_resource.lock_id().to_string())
        .execute(database.pool())
        .await
        .unwrap();

        let mut progressed = claimed.task.snapshot.clone();
        progressed.as_v1_mut().next_due_at = Some(now + time::Duration::seconds(5));
        assert!(
            repository
                .schedule_payment_task_retry(
                    &admitted.task_id,
                    "worker",
                    &claimed.claim_token,
                    now,
                    0,
                    progressed,
                    now + time::Duration::seconds(5),
                )
                .await
                .unwrap()
                .is_none()
        );
        repository
            .release_claimed_payment_task(&admitted.task_id, "worker", &claimed.claim_token)
            .await
            .unwrap();
        assert!(
            repository
                .claim_next_payment_task("next-worker", now, now + time::Duration::seconds(30),)
                .await
                .unwrap()
                .is_none()
        );
        database.cleanup().await;
    }

    #[tokio::test]
    async fn permanent_force_receipt_fences_payment_publication_acquire_and_completion() {
        let database = TestDatabase::create().await;
        let repository = PostgresPaymentTaskRepository::new(database.pool().clone());
        let admitted = repository
            .admit_payment_task(request("123e4567-e89b-42d3-a456-426614174012"))
            .await
            .unwrap();
        let caller_now = time::OffsetDateTime::now_utc();
        let claimed = repository
            .claim_next_payment_task(
                "worker",
                caller_now,
                caller_now + time::Duration::seconds(30),
            )
            .await
            .unwrap()
            .unwrap();
        let mut snapshot = claimed.task.snapshot;
        let state = snapshot.as_v1_mut();
        let accepted = state.accepted_at;
        let deadline = state.deadline_at;
        state
            .apply_create_response(
                accepted + time::Duration::seconds(1),
                accepted + time::Duration::seconds(60),
                deadline,
            )
            .unwrap();
        let sample = PaymentSampleV2 {
            observed_at: accepted + time::Duration::seconds(2),
            status: PaymentSampleStatusV2::Detected,
            outpoint: Some(BitcoinOutpointV2 {
                txid: "ab".repeat(32),
                vout: 1,
            }),
            confirmations: 0,
            amount_matched: true,
        };
        state
            .apply_status(PaymentProviderStatus {
                creator: state.creator.clone(),
                bundle_id: state.bundle_id.clone(),
                request_state: PaymentRequestStateV2::Accepted,
                payment_state: PaymentStateV2::Detected,
                invoice_created_at: accepted + time::Duration::seconds(1),
                proposal_expires_at: accepted + time::Duration::seconds(60),
                deadline,
                asset: PaymentAssetV2::Btc,
                minimum_confirmations: 0,
                current_sample: Some(sample.clone()),
                first_threshold_satisfied: Some(sample.clone()),
            })
            .unwrap();
        reduce_payment_snapshot(state, sample.observed_at).unwrap();
        let publishing = repository
            .persist_claimed_payment_snapshot(
                &admitted.task_id,
                "worker",
                &claimed.claim_token,
                caller_now,
                0,
                snapshot,
            )
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            publishing.status,
            VerificationTaskStatus::PublishingEntitlement
        );
        let publication_token = publishing.entitlement_publication_token.unwrap();
        let identity = publishing.snapshot.as_v1();
        sqlx::query(
            "INSERT INTO content_lock_force_deletion_receipts(creator,lock_id,forced_at,completed_at) VALUES($1,$2,clock_timestamp(),clock_timestamp())",
        )
        .bind(identity.creator.to_string())
        .bind(identity.pubky_lock_resource.lock_id().to_string())
        .execute(database.pool())
        .await
        .unwrap();

        assert!(
            repository
                .acquire_payment_publication_action(
                    &admitted.task_id,
                    "worker",
                    &claimed.claim_token,
                    1,
                    publication_token,
                )
                .await
                .unwrap()
                .is_none()
        );
        assert!(
            repository
                .complete_claimed_payment_publication(
                    &admitted.task_id,
                    "worker",
                    &claimed.claim_token,
                    caller_now,
                    1,
                    publication_token,
                )
                .await
                .unwrap()
                .is_none()
        );
        database.cleanup().await;
    }
}
