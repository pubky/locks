use std::str::FromStr;

use async_trait::async_trait;
use locks_core::ids::{BundleId, CreatorPubky, TaskId};
use locks_core::verification::{SatisfiedByV2, VerifiedProofBundleV2};
use sqlx::{Connection, PgConnection, PgPool};

use crate::application::errors::ApplicationError;
use crate::application::models::{
    ClaimedPaymentTask, PaymentRootState, PaymentTaskRecord, PaymentTaskSnapshot,
    PaymentTaskSnapshotV1, VerificationTerminalReason,
};
use crate::application::ports::{
    AdmitPaymentTask, EvaluatorMode, EvaluatorTaskKernel, PaymentPublicationActionGuard,
    PaymentTaskClaimer, PaymentTaskRepository, build_evaluator_publication,
};

use super::evaluator_tasks::{
    EVALUATOR_TASK_COLUMNS as COLUMNS, EvaluatorSnapshotTransition,
    EvaluatorTaskRow as PaymentTaskRow, claim_next, complete_publication_connection,
    complete_publication_pool, persist_claimed_snapshot, release_claim, schedule_claimed_retry,
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
        let row = complete_publication_connection(
            connection,
            EvaluatorMode::Payment,
            &self.task_id,
            &self.worker_id,
            self.claim_token,
            self.expected_revision,
            self.publication_token,
        )
        .await?;
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
        EvaluatorTaskKernel::claim_ttl(now, claim_expires_at)
            .map_err(|_| invalid_state("payment claim expiry must follow claim time"))?;
        let row = claim_next(
            &self.pool,
            EvaluatorMode::Payment,
            worker_id,
            now,
            claim_expires_at,
        )
        .await?;
        row.map(|(row, claim_token)| {
            row_to_task(row).map(|task| ClaimedPaymentTask { task, claim_token })
        })
        .transpose()
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
        EvaluatorTaskKernel::next_revision(expected_revision)?;
        let expected = i64::try_from(expected_revision)
            .map_err(|_| invalid_state("payment revision exceeds storage"))?;
        let row = persist_claimed_snapshot(
            &self.pool,
            EvaluatorMode::Payment,
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
                    PaymentRootState::Pending => ("in_progress", None, None, None, true),
                    PaymentRootState::Satisfied => {
                        let value = build_evaluator_publication(
                            &snapshot.as_v1().identity(),
                            EvaluatorMode::Payment,
                            database_now,
                        )
                        .map_err(|error| {
                            invalid_state(format!("invalid payment entitlement: {error}"))
                        })?;
                        (
                            "publishing_entitlement",
                            None,
                            Some(serde_json::to_value(value).map_err(snapshot_error)?),
                            Some(uuid::Uuid::new_v4()),
                            true,
                        )
                    }
                    PaymentRootState::Expired(reason) => (
                        "expired",
                        Some(reason.as_str().to_owned()),
                        None,
                        None,
                        false,
                    ),
                    PaymentRootState::Failed(reason) => (
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
        .await?;
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
        EvaluatorTaskKernel::next_revision(expected_revision)?;
        let expected = i64::try_from(expected_revision)
            .map_err(|_| invalid_state("payment revision exceeds storage"))?;
        let row = schedule_claimed_retry(
            &self.pool,
            EvaluatorMode::Payment,
            &task_id.to_string(),
            worker_id,
            claim_token,
            caller_now,
            expected,
            next_attempt_at,
            |current, _database_now| {
                let current = row_to_task(current.clone())?;
                snapshot
                    .as_v1()
                    .validate_progress_from(current.snapshot.as_v1())
                    .map_err(invalid_state)?;
                if snapshot.as_v1().root != PaymentRootState::Pending {
                    return Err(invalid_state("payment retry requires pending root"));
                }
                Ok((
                    snapshot.to_json().map_err(snapshot_error)?,
                    snapshot.as_v1().deadline_at,
                ))
            },
        )
        .await?;
        row.map(row_to_task).transpose()
    }

    async fn release_claimed_payment_task(
        &self,
        task_id: &TaskId,
        worker_id: &str,
        claim_token: &uuid::Uuid,
    ) -> Result<bool, ApplicationError> {
        release_claim(
            &self.pool,
            EvaluatorMode::Payment,
            &task_id.to_string(),
            worker_id,
            claim_token,
        )
        .await
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
        let row = complete_publication_pool(
            &self.pool,
            EvaluatorMode::Payment,
            &task_id.to_string(),
            worker_id,
            *claim_token,
            expected,
            publication_token,
        )
        .await?;
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
