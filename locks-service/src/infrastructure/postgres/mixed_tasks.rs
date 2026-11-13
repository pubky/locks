use std::str::FromStr;

use async_trait::async_trait;
use locks_core::ids::{BundleId, CreatorPubky, TaskId};
use locks_core::verification::{SatisfiedByV2, VerifiedProofBundleV2};
use sqlx::{Connection, PgConnection, PgPool};

use crate::application::errors::ApplicationError;
use crate::application::models::{
    ClaimedMixedTask, MixedRootState, MixedTaskRecord, MixedTaskSnapshot, VerificationTaskStatus,
    VerificationTerminalReason,
};
use crate::application::ports::{
    EvaluatorMode, EvaluatorTaskKernel, MixedPublicationActionGuard, MixedTaskClaimer,
    MixedTaskRepository, build_evaluator_publication_with_path,
};

use super::evaluator_tasks::{
    EVALUATOR_TASK_COLUMNS as COLUMNS, EvaluatorSnapshotTransition, EvaluatorTaskRow, claim_next,
    complete_publication_connection, complete_publication_pool, persist_claimed_snapshot,
    release_claim, schedule_claimed_retry,
};
use super::proof_admission::{lock_proof_admission, proof_admission_lock_key};
use super::verification_tasks::{reject_deletion_cutoff, status_from_database};

#[derive(Debug, Clone)]
pub struct PostgresMixedTaskRepository {
    pool: PgPool,
}
impl PostgresMixedTaskRepository {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }
}

struct PostgresMixedPublicationActionGuard {
    connection: Option<PgConnection>,
    task_id: String,
    worker_id: String,
    claim_token: uuid::Uuid,
    expected_revision: i64,
    publication_token: uuid::Uuid,
}

#[async_trait]
impl MixedPublicationActionGuard for PostgresMixedPublicationActionGuard {
    async fn complete(&mut self) -> Result<Option<MixedTaskRecord>, ApplicationError> {
        let connection = self
            .connection
            .as_mut()
            .ok_or_else(|| storage_message("mixed publication guard released"))?;
        complete_publication_connection(
            connection,
            EvaluatorMode::Mixed,
            &self.task_id,
            &self.worker_id,
            self.claim_token,
            self.expected_revision,
            self.publication_token,
        )
        .await?
        .map(row_to_task)
        .transpose()
    }
    async fn release(mut self: Box<Self>) -> Result<(), ApplicationError> {
        self.connection
            .take()
            .ok_or_else(|| storage_message("mixed publication guard released"))?
            .close()
            .await
            .map_err(storage_error)
    }
}

#[async_trait]
impl MixedTaskRepository for PostgresMixedTaskRepository {
    async fn insert_mixed_task(
        &self,
        task: MixedTaskRecord,
    ) -> Result<MixedTaskRecord, ApplicationError> {
        task.snapshot
            .as_v1()
            .validate_pristine_admission()
            .map_err(invalid_state)?;
        if task.status != VerificationTaskStatus::Pending || task.evaluator_revision != 0 {
            return Err(invalid_state("new mixed task lifecycle is not pristine"));
        }
        let identity = task.snapshot.as_v1();
        let mut tx = self.pool.begin().await.map_err(storage_error)?;
        lock_proof_admission(
            &mut tx,
            &identity.creator,
            identity.pubky_lock_resource.lock_id(),
        )
        .await?;
        let existing = sqlx::query_as::<_, EvaluatorTaskRow>(&format!(
            "SELECT {COLUMNS} FROM verification_tasks WHERE creator=$1 AND bundle_id=$2 FOR UPDATE"
        ))
        .bind(identity.creator.to_string())
        .bind(identity.bundle_id.to_string())
        .fetch_optional(&mut *tx)
        .await
        .map_err(storage_error)?;
        if let Some(existing) = existing {
            if existing.evaluator_mode != "mixed" {
                return Err(ApplicationError::VerificationTaskConflict);
            }
            let existing = row_to_task(existing)?;
            if existing.snapshot.as_v1().submitted_proof_bundle == identity.submitted_proof_bundle
                && existing.snapshot.as_v1().lock_hash == identity.lock_hash
                && existing.snapshot.as_v1().reader == identity.reader
                && existing.snapshot.as_v1().graph.operator == identity.graph.operator
                && existing
                    .snapshot
                    .as_v1()
                    .graph
                    .predicates
                    .iter()
                    .map(|v| &v.predicate)
                    .eq(identity.graph.predicates.iter().map(|v| &v.predicate))
                && existing.snapshot.as_v1().payment.payment == identity.payment.payment
            {
                tx.commit().await.map_err(storage_error)?;
                return Ok(existing);
            }
            return Err(ApplicationError::VerificationTaskConflict);
        }
        reject_deletion_cutoff(
            &mut tx,
            &identity.creator,
            identity.pubky_lock_resource.lock_id(),
        )
        .await?;
        let submitted =
            serde_json::to_value(&identity.submitted_proof_bundle).map_err(snapshot_error)?;
        let snapshot = task.snapshot.to_json().map_err(snapshot_error)?;
        let row = sqlx::query_as::<_, EvaluatorTaskRow>(&format!(
            "INSERT INTO verification_tasks(task_id,creator,bundle_id,status,submitted_proof_bundle,submitted_at,invoice_admission_phase,evaluator_mode,evaluator_snapshot,evaluator_revision)
             VALUES($1::uuid,$2,$3,'pending',$4,$5,'ready','mixed',$6,0) RETURNING {COLUMNS}"
        )).bind(task.task_id.to_string()).bind(identity.creator.to_string()).bind(identity.bundle_id.to_string()).bind(submitted).bind(identity.accepted_at).bind(snapshot)
          .fetch_one(&mut *tx).await.map_err(storage_error)?;
        tx.commit().await.map_err(storage_error)?;
        row_to_task(row)
    }

    async fn get_mixed_task_by_handle(
        &self,
        creator: &CreatorPubky,
        bundle_id: &BundleId,
    ) -> Result<Option<MixedTaskRecord>, ApplicationError> {
        sqlx::query_as::<_, EvaluatorTaskRow>(&format!("SELECT {COLUMNS} FROM verification_tasks WHERE creator=$1 AND bundle_id=$2 AND evaluator_mode='mixed'"))
            .bind(creator.to_string()).bind(bundle_id.to_string()).fetch_optional(&self.pool).await.map_err(storage_error)?.map(row_to_task).transpose()
    }
}

#[async_trait]
impl MixedTaskClaimer for PostgresMixedTaskRepository {
    async fn claim_next_mixed_task(
        &self,
        worker_id: &str,
        now: time::OffsetDateTime,
        claim_expires_at: time::OffsetDateTime,
    ) -> Result<Option<ClaimedMixedTask>, ApplicationError> {
        EvaluatorTaskKernel::claim_ttl(now, claim_expires_at)?;
        claim_next(
            &self.pool,
            EvaluatorMode::Mixed,
            worker_id,
            now,
            claim_expires_at,
        )
        .await?
        .map(|(row, claim_token)| {
            row_to_task(row).map(|task| ClaimedMixedTask { task, claim_token })
        })
        .transpose()
    }

    async fn persist_claimed_mixed_snapshot(
        &self,
        task_id: &TaskId,
        worker_id: &str,
        claim_token: &uuid::Uuid,
        _now: time::OffsetDateTime,
        expected_revision: u64,
        snapshot: MixedTaskSnapshot,
    ) -> Result<Option<MixedTaskRecord>, ApplicationError> {
        EvaluatorTaskKernel::next_revision(expected_revision)?;
        let expected = i64::try_from(expected_revision)
            .map_err(|_| invalid_state("mixed revision exceeds storage"))?;
        persist_claimed_snapshot(
            &self.pool,
            EvaluatorMode::Mixed,
            &task_id.to_string(),
            worker_id,
            claim_token,
            expected,
            |current, database_now| {
                let current = row_to_task(current.clone())?;
                snapshot
                    .as_v1()
                    .validate_progress_from(current.snapshot.as_v1())
                    .map_err(invalid_state)?;
                let root = snapshot.as_v1().root;
                let (status, reason, entitlement, token, retain_claim) = match root {
                    MixedRootState::Pending => ("in_progress", None, None, None, true),
                    MixedRootState::Satisfied => {
                        let value = build_evaluator_publication_with_path(
                            &snapshot.as_v1().identity(),
                            snapshot.as_v1().satisfied_by.clone(),
                            database_now,
                        )?;
                        (
                            "publishing_entitlement",
                            None,
                            Some(serde_json::to_value(value).map_err(snapshot_error)?),
                            Some(uuid::Uuid::new_v4()),
                            true,
                        )
                    }
                    MixedRootState::Expired(reason) => (
                        "expired",
                        Some(reason.as_str().to_owned()),
                        None,
                        None,
                        false,
                    ),
                    MixedRootState::Failed(reason) => (
                        "failed",
                        Some(reason.as_str().to_owned()),
                        None,
                        None,
                        false,
                    ),
                };
                Ok((
                    snapshot.to_json().map_err(snapshot_error)?,
                    EvaluatorSnapshotTransition {
                        status,
                        terminal_reason: reason,
                        failure_message: None,
                        entitlement,
                        publication_token: token,
                        retain_claim,
                    },
                ))
            },
        )
        .await?
        .map(row_to_task)
        .transpose()
    }

    async fn schedule_mixed_task_retry(
        &self,
        task_id: &TaskId,
        worker_id: &str,
        claim_token: &uuid::Uuid,
        caller_now: time::OffsetDateTime,
        expected_revision: u64,
        snapshot: MixedTaskSnapshot,
        next_attempt_at: time::OffsetDateTime,
    ) -> Result<Option<MixedTaskRecord>, ApplicationError> {
        EvaluatorTaskKernel::next_revision(expected_revision)?;
        let expected = i64::try_from(expected_revision)
            .map_err(|_| invalid_state("mixed revision exceeds storage"))?;
        schedule_claimed_retry(
            &self.pool,
            EvaluatorMode::Mixed,
            &task_id.to_string(),
            worker_id,
            claim_token,
            caller_now,
            expected,
            next_attempt_at,
            |current, _| {
                let current = row_to_task(current.clone())?;
                snapshot
                    .as_v1()
                    .validate_progress_from(current.snapshot.as_v1())
                    .map_err(invalid_state)?;
                if snapshot.as_v1().root != MixedRootState::Pending {
                    return Err(invalid_state("mixed retry requires pending root"));
                }
                Ok((
                    snapshot.to_json().map_err(snapshot_error)?,
                    snapshot.as_v1().deadline_at,
                ))
            },
        )
        .await?
        .map(row_to_task)
        .transpose()
    }

    async fn release_claimed_mixed_task(
        &self,
        task_id: &TaskId,
        worker_id: &str,
        claim_token: &uuid::Uuid,
    ) -> Result<bool, ApplicationError> {
        release_claim(
            &self.pool,
            EvaluatorMode::Mixed,
            &task_id.to_string(),
            worker_id,
            claim_token,
        )
        .await
    }

    async fn acquire_mixed_publication_action(
        &self,
        task_id: &TaskId,
        worker_id: &str,
        claim_token: &uuid::Uuid,
        expected_revision: u64,
        publication_token: uuid::Uuid,
    ) -> Result<Option<Box<dyn MixedPublicationActionGuard>>, ApplicationError> {
        let expected = i64::try_from(expected_revision)
            .map_err(|_| invalid_state("mixed revision exceeds storage"))?;
        let identity = sqlx::query_as::<_, EvaluatorTaskRow>(&format!("SELECT {COLUMNS} FROM verification_tasks WHERE task_id=$1::uuid AND evaluator_mode='mixed'"))
            .bind(task_id.to_string()).fetch_optional(&self.pool).await.map_err(storage_error)?.map(row_to_task).transpose()?;
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
        let live: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM verification_tasks WHERE task_id=$1::uuid AND evaluator_mode='mixed' AND status='publishing_entitlement' AND claimed_by=$2 AND claim_token=$3 AND claim_expires_at>clock_timestamp() AND evaluator_revision=$4 AND entitlement_publication_claim_token=$5 AND deletion_job_id IS NULL
          AND NOT EXISTS(SELECT 1 FROM content_lock_force_deletion_receipts r WHERE r.creator=verification_tasks.creator AND verification_tasks.submitted_proof_bundle->>'pubky_lock_resource'=r.creator||'/pub/app.locks/'||r.lock_id||'.json')
          AND NOT EXISTS(SELECT 1 FROM content_lock_deletion_task_snapshot s WHERE s.verification_task_id=verification_tasks.task_id))")
          .bind(task_id.to_string()).bind(worker_id).bind(claim_token).bind(expected).bind(publication_token).fetch_one(&mut connection).await.map_err(storage_error)?;
        if !live {
            connection.close().await.map_err(storage_error)?;
            return Ok(None);
        }
        Ok(Some(Box::new(PostgresMixedPublicationActionGuard {
            connection: Some(connection),
            task_id: task_id.to_string(),
            worker_id: worker_id.to_owned(),
            claim_token: *claim_token,
            expected_revision: expected,
            publication_token,
        })))
    }

    async fn complete_claimed_mixed_publication(
        &self,
        task_id: &TaskId,
        worker_id: &str,
        claim_token: &uuid::Uuid,
        _now: time::OffsetDateTime,
        expected_revision: u64,
        publication_token: uuid::Uuid,
    ) -> Result<Option<MixedTaskRecord>, ApplicationError> {
        let expected = i64::try_from(expected_revision)
            .map_err(|_| invalid_state("mixed revision exceeds storage"))?;
        complete_publication_pool(
            &self.pool,
            EvaluatorMode::Mixed,
            &task_id.to_string(),
            worker_id,
            *claim_token,
            expected,
            publication_token,
        )
        .await?
        .map(row_to_task)
        .transpose()
    }
}

fn row_to_task(row: EvaluatorTaskRow) -> Result<MixedTaskRecord, ApplicationError> {
    if row.evaluator_mode != "mixed" {
        return Err(storage_message(
            "non-mixed evaluator row reached mixed decoder",
        ));
    }
    let task_id = TaskId::from_str(&row.task_id)
        .map_err(|e| storage_message(format!("invalid mixed task id: {e}")))?;
    let snapshot = MixedTaskSnapshot::from_json(row.evaluator_snapshot)
        .map_err(|e| storage_message(format!("invalid mixed snapshot: {e}")))?;
    let identity = snapshot.as_v1();
    if row.creator != identity.creator.to_string()
        || row.bundle_id != identity.bundle_id.to_string()
        || row.submitted_at != identity.accepted_at
        || row.submitted_proof_bundle
            != serde_json::to_value(&identity.submitted_proof_bundle).map_err(snapshot_error)?
    {
        return Err(storage_message(
            "mixed snapshot diverges from relational identity",
        ));
    }
    let entitlement = row
        .entitlement_to_publish
        .map(|value| {
            serde_json::from_value::<VerifiedProofBundleV2>(value)
                .map_err(|e| storage_message(format!("invalid mixed entitlement: {e}")))
        })
        .transpose()?;
    if let Some(value) = entitlement.as_ref() {
        value
            .validate()
            .map_err(|e| storage_message(format!("invalid mixed entitlement: {e}")))?;
        if value.creator != identity.creator
            || value.reader != identity.reader
            || value.bundle_id != identity.bundle_id
            || value.pubky_lock_resource != identity.pubky_lock_resource
            || value.satisfied_by != identity.satisfied_by
        {
            return Err(storage_message("mixed entitlement diverges from snapshot"));
        }
        if !matches!(
            value.satisfied_by.as_slice(),
            [SatisfiedByV2::Payment]
                | [SatisfiedByV2::ZeroConfGraph]
                | [SatisfiedByV2::Payment, SatisfiedByV2::ZeroConfGraph]
        ) {
            return Err(storage_message("mixed entitlement path is not canonical"));
        }
    }
    let status = status_from_database(&row.status)?;
    let terminal_reason = row
        .terminal_reason
        .as_deref()
        .and_then(VerificationTerminalReason::from_storage_value);
    if row.terminal_reason.is_some() && terminal_reason.is_none() {
        return Err(storage_message("unknown mixed terminal reason"));
    }
    let deletion_projection = status == VerificationTaskStatus::Expired
        && terminal_reason == Some(VerificationTerminalReason::ContentLockDeleted)
        && entitlement.is_none()
        && row.deletion_projection_authorized;
    validate_mixed_record_lifecycle(status, &identity.root, terminal_reason, deletion_projection)?;
    Ok(MixedTaskRecord {
        task_id,
        snapshot,
        evaluator_revision: u64::try_from(row.evaluator_revision)
            .map_err(|_| storage_message("negative mixed evaluator revision"))?,
        status,
        terminal_reason,
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
    storage_message(format!("mixed task persistence failed: {error}"))
}
fn snapshot_error(error: serde_json::Error) -> ApplicationError {
    storage_message(format!("mixed task serialization failed: {error}"))
}

fn validate_mixed_record_lifecycle(
    status: VerificationTaskStatus,
    root: &MixedRootState,
    terminal_reason: Option<VerificationTerminalReason>,
    deletion_projection: bool,
) -> Result<(), ApplicationError> {
    if deletion_projection {
        return Ok(());
    }
    let status_matches = match root {
        MixedRootState::Pending => matches!(
            status,
            VerificationTaskStatus::Pending | VerificationTaskStatus::InProgress
        ),
        MixedRootState::Satisfied => matches!(
            status,
            VerificationTaskStatus::PublishingEntitlement | VerificationTaskStatus::Completed
        ),
        MixedRootState::Expired(_) => status == VerificationTaskStatus::Expired,
        MixedRootState::Failed(_) => status == VerificationTaskStatus::Failed,
    };
    if !status_matches {
        return Err(storage_message("mixed row status diverges from root"));
    }
    if terminal_reason != root.terminal_reason() {
        return Err(storage_message("mixed terminal reason diverges from root"));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ordinary_row_lifecycle_must_match_mixed_root_status() {
        assert!(
            validate_mixed_record_lifecycle(
                VerificationTaskStatus::Completed,
                &MixedRootState::Pending,
                None,
                false,
            )
            .is_err()
        );
    }
}
