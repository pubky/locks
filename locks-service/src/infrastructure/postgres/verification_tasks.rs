use std::str::FromStr;

use async_trait::async_trait;
use sqlx::{FromRow, PgPool};

use locks_core::ids::{BundleId, CreatorPubky, TaskId};
use locks_core::verification::{SubmittedProofBundle, VerifiedProofBundle};

use crate::application::errors::ApplicationError;
use crate::application::models::{
    ClaimedInvoiceAdmission, INVOICE_ADMISSION_INTENT_VERSION, InvoiceAdmissionIntentV1,
    InvoiceAdmissionPhase, InvoiceAdmissionRecord, InvoiceAdmissionRetryReason,
    VerificationTaskRecord, VerificationTaskStatus, VerificationTerminalReason,
};
use crate::application::ports::{InvoiceAdmissionRepository, VerificationTaskRepository};

/// Postgres-backed repository for Lock Server private verification task state.
#[derive(Debug, Clone)]
pub struct PostgresVerificationTaskRepository {
    pool: PgPool,
}

#[derive(Debug, FromRow)]
pub(super) struct VerificationTaskRow {
    task_id: String,
    creator: String,
    bundle_id: String,
    status: String,
    submitted_proof_bundle: serde_json::Value,
    submitted_at: time::OffsetDateTime,
    started_at: Option<time::OffsetDateTime>,
    completed_at: Option<time::OffsetDateTime>,
    failure_message: Option<String>,
    terminal_reason: Option<String>,
    entitlement_to_publish: Option<serde_json::Value>,
}

struct VerificationTaskWriteRow {
    task_id: String,
    creator: String,
    bundle_id: String,
    status: &'static str,
    submitted_proof_bundle: serde_json::Value,
    submitted_at: time::OffsetDateTime,
    started_at: Option<time::OffsetDateTime>,
    completed_at: Option<time::OffsetDateTime>,
    failure_message: Option<String>,
    terminal_reason: Option<&'static str>,
    entitlement_to_publish: Option<serde_json::Value>,
}

#[derive(Debug, FromRow)]
struct InvoiceAdmissionMetadataRow {
    invoice_admission_phase: String,
    invoice_admission_intent: Option<serde_json::Value>,
    admission_deadline_at: Option<time::OffsetDateTime>,
    next_attempt_at: Option<time::OffsetDateTime>,
    attempt_count: i32,
    invoice_admission_retry_reason: Option<String>,
}

pub(super) const VERIFICATION_TASK_ROW_COLUMNS: &str = "
    task_id::text AS task_id,
    creator,
    bundle_id,
    status,
    submitted_proof_bundle,
    submitted_at,
    started_at,
    completed_at,
    failure_message,
    terminal_reason,
    entitlement_to_publish";

impl PostgresVerificationTaskRepository {
    /// Creates a repository backed by the provided migrated Postgres pool.
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }
}

#[async_trait]
impl VerificationTaskRepository for PostgresVerificationTaskRepository {
    async fn insert_verification_task(
        &self,
        task: VerificationTaskRecord,
    ) -> Result<(), ApplicationError> {
        let row = VerificationTaskWriteRow::try_from(&task)?;
        let result = sqlx::query(
            "INSERT INTO verification_tasks (
                task_id,
                creator,
                bundle_id,
                status,
                submitted_proof_bundle,
                submitted_at,
                started_at,
                completed_at,
                failure_message,
                terminal_reason,
                entitlement_to_publish
            )
            VALUES ($1::uuid, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
            ON CONFLICT DO NOTHING",
        )
        .bind(row.task_id)
        .bind(row.creator)
        .bind(row.bundle_id)
        .bind(row.status)
        .bind(row.submitted_proof_bundle)
        .bind(row.submitted_at)
        .bind(row.started_at)
        .bind(row.completed_at)
        .bind(row.failure_message)
        .bind(row.terminal_reason)
        .bind(row.entitlement_to_publish)
        .execute(&self.pool)
        .await
        .map_err(storage_error)?;

        if result.rows_affected() == 0 {
            return Err(ApplicationError::DuplicateRecord {
                record: "verification_task",
            });
        }

        Ok(())
    }

    async fn update_verification_task(
        &self,
        task: VerificationTaskRecord,
    ) -> Result<(), ApplicationError> {
        let row = VerificationTaskWriteRow::try_from(&task)?;
        let result = sqlx::query(
            "UPDATE verification_tasks
            SET creator = $2,
                bundle_id = $3,
                status = $4,
                submitted_proof_bundle = $5,
                submitted_at = $6,
                started_at = $7,
                completed_at = $8,
                failure_message = $9,
                terminal_reason = $10,
                entitlement_to_publish = $11,
                updated_at = now()
            WHERE task_id = $1::uuid",
        )
        .bind(row.task_id)
        .bind(row.creator)
        .bind(row.bundle_id)
        .bind(row.status)
        .bind(row.submitted_proof_bundle)
        .bind(row.submitted_at)
        .bind(row.started_at)
        .bind(row.completed_at)
        .bind(row.failure_message)
        .bind(row.terminal_reason)
        .bind(row.entitlement_to_publish)
        .execute(&self.pool)
        .await
        .map_err(storage_error)?;

        if result.rows_affected() == 0 {
            return Err(ApplicationError::MissingRecord {
                record: "verification_task",
            });
        }

        Ok(())
    }

    async fn get_verification_task(
        &self,
        task_id: &TaskId,
    ) -> Result<Option<VerificationTaskRecord>, ApplicationError> {
        let sql = format!(
            "SELECT {VERIFICATION_TASK_ROW_COLUMNS}
            FROM verification_tasks
            WHERE task_id = $1::uuid"
        );
        let row = sqlx::query_as::<_, VerificationTaskRow>(&sql)
            .bind(task_id.to_string())
            .fetch_optional(&self.pool)
            .await
            .map_err(storage_error)?;

        row.map(row_to_task).transpose()
    }

    async fn get_verification_task_by_handle(
        &self,
        creator: &CreatorPubky,
        bundle_id: &BundleId,
    ) -> Result<Option<VerificationTaskRecord>, ApplicationError> {
        let sql = format!(
            "SELECT {VERIFICATION_TASK_ROW_COLUMNS}
            FROM verification_tasks
            WHERE creator = $1 AND bundle_id = $2"
        );
        let row = sqlx::query_as::<_, VerificationTaskRow>(&sql)
            .bind(creator.to_string())
            .bind(bundle_id.to_string())
            .fetch_optional(&self.pool)
            .await
            .map_err(storage_error)?;

        row.map(row_to_task).transpose()
    }

    async fn delete_verification_task(&self, task_id: &TaskId) -> Result<(), ApplicationError> {
        sqlx::query("DELETE FROM verification_tasks WHERE task_id = $1::uuid")
            .bind(task_id.to_string())
            .execute(&self.pool)
            .await
            .map_err(storage_error)?;
        Ok(())
    }
}

#[async_trait]
impl InvoiceAdmissionRepository for PostgresVerificationTaskRepository {
    async fn insert_invoice_pending_task(
        &self,
        task: VerificationTaskRecord,
        intent: InvoiceAdmissionIntentV1,
    ) -> Result<InvoiceAdmissionRecord, ApplicationError> {
        validate_invoice_admission_inputs(&task, &intent)?;
        let row = VerificationTaskWriteRow::try_from(&task)?;
        let intent_json =
            serde_json::to_value(&intent).map_err(|error| ApplicationError::Storage {
                message: format!("serialize invoice admission intent for Postgres: {error}"),
            })?;
        let result = sqlx::query(
            "WITH timing AS (SELECT clock_timestamp() AS winner_time)
             INSERT INTO verification_tasks (
                task_id, creator, bundle_id, status, submitted_proof_bundle,
                submitted_at, started_at, completed_at, failure_message,
                terminal_reason, entitlement_to_publish, invoice_admission_phase,
                invoice_admission_intent, admission_deadline_at, next_attempt_at
             )
             SELECT $1::uuid, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11,
                    'invoice_pending', $12, winner_time + INTERVAL '10 minutes', winner_time
             FROM timing
             ON CONFLICT DO NOTHING",
        )
        .bind(row.task_id)
        .bind(row.creator)
        .bind(row.bundle_id)
        .bind(row.status)
        .bind(row.submitted_proof_bundle)
        .bind(row.submitted_at)
        .bind(row.started_at)
        .bind(row.completed_at)
        .bind(row.failure_message)
        .bind(row.terminal_reason)
        .bind(row.entitlement_to_publish)
        .bind(intent_json)
        .execute(&self.pool)
        .await
        .map_err(storage_error)?;

        let existing_task = self
            .get_verification_task_by_handle(&task.creator, &task.submitted_proof_bundle.bundle_id)
            .await?
            .ok_or(ApplicationError::DuplicateRecord {
                record: "verification_task",
            })?;
        let existing_admission = self
            .get_invoice_admission(&existing_task.task_id)
            .await?
            .ok_or(ApplicationError::VerificationTaskConflict)?;
        if result.rows_affected() == 0
            && (existing_task.submitted_proof_bundle != task.submitted_proof_bundle
                || existing_admission.intent != intent)
        {
            return Err(ApplicationError::VerificationTaskConflict);
        }
        Ok(existing_admission)
    }

    async fn get_invoice_admission(
        &self,
        task_id: &TaskId,
    ) -> Result<Option<InvoiceAdmissionRecord>, ApplicationError> {
        let metadata = sqlx::query_as::<_, InvoiceAdmissionMetadataRow>(
            "SELECT invoice_admission_phase, invoice_admission_intent,
                    admission_deadline_at, next_attempt_at, attempt_count,
                    invoice_admission_retry_reason
             FROM verification_tasks
             WHERE task_id = $1::uuid AND invoice_admission_intent IS NOT NULL",
        )
        .bind(task_id.to_string())
        .fetch_optional(&self.pool)
        .await
        .map_err(storage_error)?;
        let Some(metadata) = metadata else {
            return Ok(None);
        };
        let task =
            self.get_verification_task(task_id)
                .await?
                .ok_or(ApplicationError::MissingRecord {
                    record: "verification_task",
                })?;
        let phase = InvoiceAdmissionPhase::from_storage_value(&metadata.invoice_admission_phase)
            .ok_or_else(|| ApplicationError::Storage {
                message: format!(
                    "invalid invoice_admission_phase stored in Postgres: {}",
                    metadata.invoice_admission_phase
                ),
            })?;
        let intent = serde_json::from_value(
            metadata
                .invoice_admission_intent
                .expect("query requires invoice admission intent"),
        )
        .map_err(|error| ApplicationError::Storage {
            message: format!("deserialize invoice admission intent from Postgres: {error}"),
        })?;
        let admission_deadline_at =
            metadata
                .admission_deadline_at
                .ok_or_else(|| ApplicationError::Storage {
                    message: "invoice admission intent is missing its deadline".to_owned(),
                })?;
        let attempt_count =
            u32::try_from(metadata.attempt_count).map_err(|_| ApplicationError::Storage {
                message: "invoice admission attempt_count is negative".to_owned(),
            })?;
        let retry_reason = metadata
            .invoice_admission_retry_reason
            .map(|value| {
                InvoiceAdmissionRetryReason::from_storage_value(&value).ok_or_else(|| {
                    ApplicationError::Storage {
                        message: format!(
                            "invalid invoice_admission_retry_reason stored in Postgres: {value}"
                        ),
                    }
                })
            })
            .transpose()?;
        Ok(Some(InvoiceAdmissionRecord {
            task,
            phase,
            intent,
            admission_deadline_at,
            next_attempt_at: metadata.next_attempt_at,
            attempt_count,
            retry_reason,
        }))
    }

    async fn claim_next_invoice_admission(
        &self,
        worker_id: &str,
        _now: time::OffsetDateTime,
        claim_ttl: time::Duration,
    ) -> Result<Option<ClaimedInvoiceAdmission>, ApplicationError> {
        let claim_ttl_microseconds = duration_microseconds(claim_ttl, "claim TTL")?;
        let claim_token = uuid::Uuid::new_v4();
        let claimed: Option<(String, bool)> = sqlx::query_as(
            "WITH candidate AS MATERIALIZED (
                 SELECT task_id
                 FROM verification_tasks
                 WHERE invoice_admission_phase = 'invoice_pending'
                   AND status = 'pending'
                   AND COALESCE(next_attempt_at, claim_expires_at) <= clock_timestamp()
                   AND (claim_expires_at IS NULL OR claim_expires_at <= clock_timestamp())
                 ORDER BY COALESCE(next_attempt_at, claim_expires_at), submitted_at, task_id
                 FOR UPDATE SKIP LOCKED
                 LIMIT 1
             ),
             timing AS MATERIALIZED (
                 SELECT clock_timestamp() AS winner_time FROM candidate
             )
             UPDATE verification_tasks AS task
             SET claimed_by = $1,
                 claim_token = $2,
                 claim_expires_at = timing.winner_time + ($3 * INTERVAL '1 microsecond'),
                 next_attempt_at = NULL,
                 attempt_count = attempt_count + 1,
                 updated_at = timing.winner_time
             FROM candidate, timing
             WHERE task.task_id = candidate.task_id
               AND task.invoice_admission_phase = 'invoice_pending'
               AND task.status = 'pending'
               AND COALESCE(task.next_attempt_at, task.claim_expires_at) <= timing.winner_time
               AND (task.claim_expires_at IS NULL OR task.claim_expires_at <= timing.winner_time)
             RETURNING task.task_id::text,
                       task.admission_deadline_at <= timing.winner_time AS deadline_expired",
        )
        .bind(worker_id)
        .bind(claim_token)
        .bind(claim_ttl_microseconds)
        .fetch_optional(&self.pool)
        .await
        .map_err(storage_error)?;
        let Some((task_id, deadline_expired)) = claimed else {
            return Ok(None);
        };
        let task_id = TaskId::from_str(&task_id).map_err(|error| ApplicationError::Storage {
            message: format!("invalid claimed invoice admission task_id: {error}"),
        })?;
        let admission =
            self.get_invoice_admission(&task_id)
                .await?
                .ok_or(ApplicationError::MissingRecord {
                    record: "invoice_admission",
                })?;
        Ok(Some(ClaimedInvoiceAdmission {
            admission,
            claim_token,
            deadline_expired,
        }))
    }

    async fn mark_invoice_admission_ready(
        &self,
        task_id: &TaskId,
        worker_id: &str,
        claim_token: &uuid::Uuid,
        _now: time::OffsetDateTime,
    ) -> Result<Option<InvoiceAdmissionRecord>, ApplicationError> {
        let updated_task_id: Option<String> = sqlx::query_scalar(
            "WITH locked AS MATERIALIZED (
                 SELECT task_id FROM verification_tasks
                 WHERE task_id = $1::uuid
                 FOR UPDATE
             ),
             timing AS MATERIALIZED (
                 SELECT clock_timestamp() AS winner_time FROM locked
             )
             UPDATE verification_tasks AS task
             SET invoice_admission_phase = 'ready',
                 invoice_admission_retry_reason = NULL,
                 claimed_by = NULL,
                 claim_token = NULL,
                 claim_expires_at = NULL,
                 next_attempt_at = NULL,
                 updated_at = timing.winner_time
             FROM timing
             WHERE task.task_id = $1::uuid
               AND task.invoice_admission_phase = 'invoice_pending'
               AND task.status = 'pending'
               AND task.claimed_by = $2
               AND task.claim_token = $3
               AND task.claim_expires_at > timing.winner_time
               AND task.admission_deadline_at > timing.winner_time
             RETURNING task.task_id::text",
        )
        .bind(task_id.to_string())
        .bind(worker_id)
        .bind(claim_token)
        .fetch_optional(&self.pool)
        .await
        .map_err(storage_error)?;
        if updated_task_id.is_none() {
            return Ok(None);
        }
        self.get_invoice_admission(task_id).await
    }

    async fn schedule_invoice_admission_retry(
        &self,
        task_id: &TaskId,
        worker_id: &str,
        claim_token: &uuid::Uuid,
        _now: time::OffsetDateTime,
        retry_after: time::Duration,
        retry_reason: Option<InvoiceAdmissionRetryReason>,
    ) -> Result<Option<InvoiceAdmissionRecord>, ApplicationError> {
        let retry_after_microseconds = duration_microseconds(retry_after, "retry delay")?;
        let updated: Option<String> = sqlx::query_scalar(
            "WITH locked AS MATERIALIZED (
                 SELECT task_id FROM verification_tasks
                 WHERE task_id = $1::uuid
                 FOR UPDATE
             ),
             timing AS MATERIALIZED (
                 SELECT clock_timestamp() AS winner_time FROM locked
             )
             UPDATE verification_tasks AS task
             SET claimed_by = NULL, claim_token = NULL, claim_expires_at = NULL,
                 invoice_admission_retry_reason = COALESCE($5, task.invoice_admission_retry_reason),
                 next_attempt_at = LEAST(
                     timing.winner_time + ($4 * INTERVAL '1 microsecond'),
                     task.admission_deadline_at
                 ),
                 updated_at = timing.winner_time
             FROM timing
             WHERE task.task_id = $1::uuid
               AND task.invoice_admission_phase = 'invoice_pending'
               AND task.status = 'pending'
               AND task.claimed_by = $2 AND task.claim_token = $3
               AND task.claim_expires_at > timing.winner_time
               AND task.admission_deadline_at > timing.winner_time
             RETURNING task.task_id::text",
        )
        .bind(task_id.to_string())
        .bind(worker_id)
        .bind(claim_token)
        .bind(retry_after_microseconds)
        .bind(retry_reason.map(InvoiceAdmissionRetryReason::as_str))
        .fetch_optional(&self.pool)
        .await
        .map_err(storage_error)?;
        if updated.is_none() {
            return Ok(None);
        }
        self.get_invoice_admission(task_id).await
    }

    async fn mark_invoice_admission_failed(
        &self,
        task_id: &TaskId,
        worker_id: &str,
        claim_token: &uuid::Uuid,
        _now: time::OffsetDateTime,
        failure_message: &str,
    ) -> Result<Option<InvoiceAdmissionRecord>, ApplicationError> {
        let message = failure_message.trim();
        if message.is_empty() {
            return Err(ApplicationError::InvalidVerificationTaskFailureMessage);
        }
        let updated: Option<String> = sqlx::query_scalar(
            "WITH locked AS MATERIALIZED (
                 SELECT task_id FROM verification_tasks
                 WHERE task_id = $1::uuid
                 FOR UPDATE
             ),
             timing AS MATERIALIZED (
                 SELECT clock_timestamp() AS winner_time FROM locked
             )
             UPDATE verification_tasks AS task
             SET invoice_admission_phase = 'failed', status = 'failed',
                 invoice_admission_retry_reason = NULL,
                 started_at = COALESCE(task.started_at, timing.winner_time),
                 completed_at = timing.winner_time, failure_message = $4,
                 claimed_by = NULL, claim_token = NULL, claim_expires_at = NULL,
                 next_attempt_at = NULL, updated_at = timing.winner_time
             FROM timing
             WHERE task.task_id = $1::uuid
               AND task.invoice_admission_phase = 'invoice_pending'
               AND task.status = 'pending'
               AND task.claimed_by = $2 AND task.claim_token = $3
               AND task.claim_expires_at > timing.winner_time
             RETURNING task.task_id::text",
        )
        .bind(task_id.to_string())
        .bind(worker_id)
        .bind(claim_token)
        .bind(message)
        .fetch_optional(&self.pool)
        .await
        .map_err(storage_error)?;
        if updated.is_none() {
            return Ok(None);
        }
        self.get_invoice_admission(task_id).await
    }
}

fn validate_invoice_admission_inputs(
    task: &VerificationTaskRecord,
    intent: &InvoiceAdmissionIntentV1,
) -> Result<(), ApplicationError> {
    if intent.version != INVOICE_ADMISSION_INTENT_VERSION
        || task.status != VerificationTaskStatus::Pending
        || task.creator != intent.creator
        || task.submitted_proof_bundle.bundle_id != intent.bundle_id
        || task.submitted_proof_bundle.pubky_lock_resource != intent.lock_resource
        || task.submitted_proof_bundle.reader_public_key.as_ref() != Some(&intent.reader)
    {
        return Err(ApplicationError::InvalidVerificationTaskState {
            message: "invoice admission intent diverges from pending verification task".to_owned(),
        });
    }
    Ok(())
}

fn duration_microseconds(
    duration: time::Duration,
    field: &'static str,
) -> Result<i64, ApplicationError> {
    i64::try_from(duration.whole_microseconds()).map_err(|_| ApplicationError::Storage {
        message: format!("invoice admission {field} exceeds PostgreSQL interval range"),
    })
}

pub(super) fn row_to_task(
    row: VerificationTaskRow,
) -> Result<VerificationTaskRecord, ApplicationError> {
    row.try_into()
}

impl TryFrom<VerificationTaskRow> for VerificationTaskRecord {
    type Error = ApplicationError;

    fn try_from(row: VerificationTaskRow) -> Result<Self, Self::Error> {
        let task_id =
            TaskId::from_str(&row.task_id).map_err(|error| ApplicationError::Storage {
                message: format!("invalid verification task_id stored in Postgres: {error}"),
            })?;
        let submitted_proof_bundle = submitted_proof_bundle_from_json(row.submitted_proof_bundle)?;
        let stored_creator =
            CreatorPubky::from_str(&row.creator).map_err(|error| ApplicationError::Storage {
                message: format!("invalid verification task creator stored in Postgres: {error}"),
            })?;
        let stored_bundle_id =
            BundleId::from_str(&row.bundle_id).map_err(|error| ApplicationError::Storage {
                message: format!("invalid verification task bundle_id stored in Postgres: {error}"),
            })?;
        let bundle_creator = submitted_proof_bundle.pubky_lock_resource.creator().clone();
        if stored_creator != bundle_creator || stored_bundle_id != submitted_proof_bundle.bundle_id
        {
            return Err(ApplicationError::Storage {
                message: "verification task handle columns diverge from submitted proof bundle"
                    .to_owned(),
            });
        }

        Ok(VerificationTaskRecord {
            task_id,
            creator: stored_creator,
            submitted_proof_bundle,
            status: status_from_database(&row.status)?,
            submitted_at: row.submitted_at,
            started_at: row.started_at,
            completed_at: row.completed_at,
            failure_message: row.failure_message,
            terminal_reason: row
                .terminal_reason
                .as_deref()
                .map(terminal_reason_from_database)
                .transpose()?,
            entitlement_to_publish: row
                .entitlement_to_publish
                .map(verified_proof_bundle_from_json)
                .transpose()?,
        })
    }
}

impl TryFrom<&VerificationTaskRecord> for VerificationTaskWriteRow {
    type Error = ApplicationError;

    fn try_from(task: &VerificationTaskRecord) -> Result<Self, Self::Error> {
        let bundle_creator = task
            .submitted_proof_bundle
            .pubky_lock_resource
            .creator()
            .clone();
        if task.creator != bundle_creator {
            return Err(ApplicationError::Storage {
                message: "verification task record creator diverges from submitted proof bundle"
                    .to_owned(),
            });
        }

        Ok(Self {
            task_id: task.task_id.to_string(),
            creator: bundle_creator.to_string(),
            bundle_id: task.submitted_proof_bundle.bundle_id.to_string(),
            status: status_to_database(task.status),
            submitted_proof_bundle: submitted_proof_bundle_to_json(&task.submitted_proof_bundle)?,
            submitted_at: task.submitted_at,
            started_at: task.started_at,
            completed_at: task.completed_at,
            failure_message: task.failure_message.clone(),
            terminal_reason: task.terminal_reason.map(VerificationTerminalReason::as_str),
            entitlement_to_publish: task
                .entitlement_to_publish
                .as_ref()
                .map(verified_proof_bundle_to_json)
                .transpose()?,
        })
    }
}

fn terminal_reason_from_database(
    value: &str,
) -> Result<VerificationTerminalReason, ApplicationError> {
    VerificationTerminalReason::from_storage_value(value).ok_or_else(|| ApplicationError::Storage {
        message: format!("invalid verification task terminal_reason stored in Postgres: {value}"),
    })
}

fn submitted_proof_bundle_to_json(
    submitted_proof_bundle: &SubmittedProofBundle,
) -> Result<serde_json::Value, ApplicationError> {
    serde_json::to_value(submitted_proof_bundle).map_err(|error| ApplicationError::Storage {
        message: format!("serialize submitted proof bundle for Postgres: {error}"),
    })
}

fn submitted_proof_bundle_from_json(
    value: serde_json::Value,
) -> Result<SubmittedProofBundle, ApplicationError> {
    serde_json::from_value(value).map_err(|error| ApplicationError::Storage {
        message: format!("deserialize submitted proof bundle from Postgres: {error}"),
    })
}

pub(super) fn verified_proof_bundle_to_json(
    entitlement: &VerifiedProofBundle,
) -> Result<serde_json::Value, ApplicationError> {
    serde_json::to_value(entitlement).map_err(|error| ApplicationError::Storage {
        message: format!("serialize entitlement publication payload for Postgres: {error}"),
    })
}

fn verified_proof_bundle_from_json(
    value: serde_json::Value,
) -> Result<VerifiedProofBundle, ApplicationError> {
    serde_json::from_value(value).map_err(|error| ApplicationError::Storage {
        message: format!("deserialize entitlement publication payload from Postgres: {error}"),
    })
}

pub(super) fn status_to_database(status: VerificationTaskStatus) -> &'static str {
    match status {
        VerificationTaskStatus::Pending => "pending",
        VerificationTaskStatus::InProgress => "in_progress",
        VerificationTaskStatus::PublishingEntitlement => "publishing_entitlement",
        VerificationTaskStatus::Completed => "completed",
        VerificationTaskStatus::Failed => "failed",
        VerificationTaskStatus::Expired => "expired",
    }
}

fn status_from_database(status: &str) -> Result<VerificationTaskStatus, ApplicationError> {
    match status {
        "pending" => Ok(VerificationTaskStatus::Pending),
        "in_progress" => Ok(VerificationTaskStatus::InProgress),
        "publishing_entitlement" => Ok(VerificationTaskStatus::PublishingEntitlement),
        "completed" => Ok(VerificationTaskStatus::Completed),
        "failed" => Ok(VerificationTaskStatus::Failed),
        "expired" => Ok(VerificationTaskStatus::Expired),
        _ => Err(ApplicationError::Storage {
            message: format!("invalid verification task status stored in Postgres: {status}"),
        }),
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

    use super::PostgresVerificationTaskRepository;
    use crate::application::errors::ApplicationError;
    use crate::application::models::{
        INVOICE_ADMISSION_INTENT_VERSION, InvoiceAdmissionIntentV1, InvoiceAdmissionPhase,
        InvoiceAdmissionRetryReason, VerificationTaskRecord, VerificationTaskStatus,
        VerificationTerminalReason,
    };
    use crate::application::ports::{InvoiceAdmissionRepository, VerificationTaskRepository};
    use crate::infrastructure::postgres::testing::TestDatabase;

    const TASK_ID: &str = "018fc6ec-2f3d-4f7e-8b7d-6f5c4b3a2d10";
    const MISSING_TASK_ID: &str = "018fc6ec-2f3d-4f7e-8b7d-6f5c4b3a2d11";
    const DUPLICATE_HANDLE_TASK_ID: &str = "018fc6ec-2f3d-4f7e-8b7d-6f5c4b3a2d12";
    const LOCK_ID: &str = "000G40R40M30E209185GR38E1W8124GK2GAHC5RR34D1P70X3RFG";
    const BUNDLE_ID: &str = "000G40R40M30E209185GR38E1W";

    #[tokio::test]
    async fn insert_read_update_delete_and_duplicate_semantics_match_port_contract() {
        let database = TestDatabase::create().await;
        let repo = PostgresVerificationTaskRepository::new(database.pool().clone());
        let task_id = TaskId::from_str(TASK_ID).unwrap();
        let missing_task_id = TaskId::from_str(MISSING_TASK_ID).unwrap();
        let pending = task(VerificationTaskStatus::Pending);
        let failed = pending
            .transition_to(
                VerificationTaskStatus::InProgress,
                datetime!(2026-05-29 12:01:00 UTC),
                None,
            )
            .unwrap()
            .transition_to(
                VerificationTaskStatus::Failed,
                datetime!(2026-05-29 12:02:00 UTC),
                Some("verifier rejected proof".to_owned()),
            )
            .unwrap();

        assert_eq!(repo.get_verification_task(&task_id).await.unwrap(), None);
        assert_eq!(
            repo.update_verification_task(pending.clone()).await,
            Err(ApplicationError::MissingRecord {
                record: "verification_task",
            })
        );

        repo.insert_verification_task(pending.clone())
            .await
            .unwrap();
        assert_eq!(
            repo.insert_verification_task(pending).await,
            Err(ApplicationError::DuplicateRecord {
                record: "verification_task",
            })
        );
        assert_eq!(
            repo.insert_verification_task(task_with(
                DUPLICATE_HANDLE_TASK_ID,
                "pubkytkrq8zmwb8a3m9k15csu3q17qmfgqnp9dskbrg9uq1rydpyxp7qy",
                BUNDLE_ID,
                VerificationTaskStatus::Pending,
            ))
            .await,
            Err(ApplicationError::DuplicateRecord {
                record: "verification_task",
            })
        );

        repo.update_verification_task(failed.clone()).await.unwrap();
        assert_eq!(
            repo.get_verification_task(&task_id).await.unwrap(),
            Some(failed)
        );

        repo.delete_verification_task(&task_id).await.unwrap();
        repo.delete_verification_task(&missing_task_id)
            .await
            .unwrap();
        assert_eq!(repo.get_verification_task(&task_id).await.unwrap(), None);

        database.cleanup().await;
    }

    #[tokio::test]
    async fn record_survives_repository_wrapper_recreation_and_preserves_submitted_bundle_json() {
        let database = TestDatabase::create().await;
        let original_repo = PostgresVerificationTaskRepository::new(database.pool().clone());
        let recreated_repo = PostgresVerificationTaskRepository::new(database.pool().clone());
        let task_id = TaskId::from_str(TASK_ID).unwrap();
        let pending = task(VerificationTaskStatus::Pending);

        original_repo
            .insert_verification_task(pending.clone())
            .await
            .unwrap();

        assert_eq!(
            recreated_repo
                .get_verification_task(&task_id)
                .await
                .unwrap(),
            Some(pending.clone())
        );
        assert_eq!(
            recreated_repo
                .get_verification_task_by_handle(
                    &CreatorPubky::from_str(
                        "pubkytkrq8zmwb8a3m9k15csu3q17qmfgqnp9dskbrg9uq1rydpyxp7qy"
                    )
                    .unwrap(),
                    &BundleId::from_str(BUNDLE_ID).unwrap(),
                )
                .await
                .unwrap(),
            Some(pending)
        );
        assert_eq!(
            recreated_repo
                .get_verification_task_by_handle(
                    &CreatorPubky::from_str(
                        "pubkyorhzqdiexwmi6iidktucgud63ufa5nwtsuzdxe176a8izd6jsqky"
                    )
                    .unwrap(),
                    &BundleId::from_str(BUNDLE_ID).unwrap(),
                )
                .await
                .unwrap(),
            None
        );

        database.cleanup().await;
    }

    #[tokio::test]
    async fn invoice_admission_survives_restart_with_db_time_deadline_and_exact_replay() {
        let database = TestDatabase::create().await;
        let original_repo = PostgresVerificationTaskRepository::new(database.pool().clone());
        let recreated_repo = PostgresVerificationTaskRepository::new(database.pool().clone());
        let mut task = task(VerificationTaskStatus::Pending);
        let reader =
            CreatorPubky::from_str("pubkyorhzqdiexwmi6iidktucgud63ufa5nwtsuzdxe176a8izd6jsqky")
                .unwrap();
        task.submitted_proof_bundle.reader_public_key = Some(reader.clone());
        let intent = InvoiceAdmissionIntentV1 {
            version: INVOICE_ADMISSION_INTENT_VERSION,
            creator: task.creator.clone(),
            bundle_id: task.submitted_proof_bundle.bundle_id.clone(),
            lock_resource: task.submitted_proof_bundle.pubky_lock_resource.clone(),
            reader,
        };

        let inserted = original_repo
            .insert_invoice_pending_task(task.clone(), intent.clone())
            .await
            .unwrap();
        let restarted = recreated_repo
            .get_invoice_admission(&task.task_id)
            .await
            .unwrap()
            .unwrap();
        let replayed = recreated_repo
            .insert_invoice_pending_task(task.clone(), intent.clone())
            .await
            .unwrap();

        assert_eq!(inserted, restarted);
        assert_eq!(restarted, replayed);
        assert_eq!(restarted.phase, InvoiceAdmissionPhase::InvoicePending);
        assert_eq!(restarted.intent, intent);
        assert_eq!(
            restarted.admission_deadline_at - restarted.next_attempt_at.unwrap(),
            time::Duration::minutes(10)
        );
        assert_eq!(restarted.attempt_count, 0);

        let mut changed = replayed.intent;
        changed.reader =
            CreatorPubky::from_str("pubky7ir1ttte48bcp4zjychjyscicrwi1j34mtt91ptsafdbjmr8g9eo")
                .unwrap();
        let mut changed_task = task;
        changed_task.submitted_proof_bundle.reader_public_key = Some(changed.reader.clone());
        assert_eq!(
            recreated_repo
                .insert_invoice_pending_task(changed_task, changed)
                .await,
            Err(ApplicationError::VerificationTaskConflict)
        );

        database.cleanup().await;
    }

    #[tokio::test]
    async fn invoice_admission_postgres_ready_transition_rejects_stale_claim_token() {
        let database = TestDatabase::create().await;
        let repo = PostgresVerificationTaskRepository::new(database.pool().clone());
        let mut task = task(VerificationTaskStatus::Pending);
        let reader =
            CreatorPubky::from_str("pubkyorhzqdiexwmi6iidktucgud63ufa5nwtsuzdxe176a8izd6jsqky")
                .unwrap();
        task.submitted_proof_bundle.reader_public_key = Some(reader.clone());
        let intent = InvoiceAdmissionIntentV1 {
            version: INVOICE_ADMISSION_INTENT_VERSION,
            creator: task.creator.clone(),
            bundle_id: task.submitted_proof_bundle.bundle_id.clone(),
            lock_resource: task.submitted_proof_bundle.pubky_lock_resource.clone(),
            reader,
        };
        repo.insert_invoice_pending_task(task.clone(), intent)
            .await
            .unwrap();
        let claim = repo
            .claim_next_invoice_admission("worker-a", task.submitted_at, time::Duration::minutes(1))
            .await
            .unwrap()
            .unwrap();

        assert_eq!(
            repo.mark_invoice_admission_ready(
                &task.task_id,
                "worker-a",
                &uuid::Uuid::new_v4(),
                task.submitted_at,
            )
            .await
            .unwrap(),
            None
        );
        assert_eq!(
            repo.schedule_invoice_admission_retry(
                &task.task_id,
                "worker-a",
                &uuid::Uuid::new_v4(),
                task.submitted_at,
                time::Duration::seconds(5),
                None,
            )
            .await
            .unwrap(),
            None
        );
        assert_eq!(
            repo.mark_invoice_admission_failed(
                &task.task_id,
                "worker-a",
                &uuid::Uuid::new_v4(),
                task.submitted_at,
                "invoice admission failed",
            )
            .await
            .unwrap(),
            None
        );
        let ready = repo
            .mark_invoice_admission_ready(
                &task.task_id,
                "worker-a",
                &claim.claim_token,
                task.submitted_at,
            )
            .await
            .unwrap()
            .unwrap();
        assert_eq!(ready.phase, InvoiceAdmissionPhase::Ready);
        assert_eq!(ready.task.status, VerificationTaskStatus::Pending);
        assert_eq!(ready.next_attempt_at, None);

        database.cleanup().await;
    }

    #[tokio::test]
    async fn invoice_admission_postgres_reclaims_expired_lease_with_fresh_token() {
        let database = TestDatabase::create().await;
        let repo = PostgresVerificationTaskRepository::new(database.pool().clone());
        let mut task = task(VerificationTaskStatus::Pending);
        let reader =
            CreatorPubky::from_str("pubkyorhzqdiexwmi6iidktucgud63ufa5nwtsuzdxe176a8izd6jsqky")
                .unwrap();
        task.submitted_proof_bundle.reader_public_key = Some(reader.clone());
        repo.insert_invoice_pending_task(
            task.clone(),
            InvoiceAdmissionIntentV1 {
                version: INVOICE_ADMISSION_INTENT_VERSION,
                creator: task.creator.clone(),
                bundle_id: task.submitted_proof_bundle.bundle_id.clone(),
                lock_resource: task.submitted_proof_bundle.pubky_lock_resource.clone(),
                reader,
            },
        )
        .await
        .unwrap();
        let first = repo
            .claim_next_invoice_admission("worker-a", task.submitted_at, time::Duration::minutes(1))
            .await
            .unwrap()
            .unwrap();
        sqlx::query(
            "UPDATE verification_tasks
             SET claim_expires_at = clock_timestamp() - INTERVAL '1 microsecond'
             WHERE task_id = $1::uuid",
        )
        .bind(task.task_id.to_string())
        .execute(database.pool())
        .await
        .unwrap();

        let second = repo
            .claim_next_invoice_admission("worker-b", task.submitted_at, time::Duration::minutes(1))
            .await
            .unwrap()
            .unwrap();

        assert_ne!(second.claim_token, first.claim_token);
        assert_eq!(second.admission.attempt_count, 2);
        database.cleanup().await;
    }

    #[tokio::test]
    async fn concurrent_invoice_admission_claims_return_one_lease_for_one_task() {
        let database = TestDatabase::create().await;
        let repo = PostgresVerificationTaskRepository::new(database.pool().clone());
        let mut task = task(VerificationTaskStatus::Pending);
        task.submitted_proof_bundle.reader_public_key = Some(
            CreatorPubky::from_str("pubkyorhzqdiexwmi6iidktucgud63ufa5nwtsuzdxe176a8izd6jsqky")
                .unwrap(),
        );
        repo.insert_invoice_pending_task(
            task.clone(),
            InvoiceAdmissionIntentV1 {
                version: INVOICE_ADMISSION_INTENT_VERSION,
                creator: task.creator.clone(),
                bundle_id: task.submitted_proof_bundle.bundle_id.clone(),
                lock_resource: task.submitted_proof_bundle.pubky_lock_resource.clone(),
                reader: task
                    .submitted_proof_bundle
                    .reader_public_key
                    .clone()
                    .unwrap(),
            },
        )
        .await
        .unwrap();

        let repo_a = repo.clone();
        let repo_b = repo.clone();
        let (claim_a, claim_b) = tokio::join!(
            repo_a.claim_next_invoice_admission(
                "worker-a",
                task.submitted_at,
                time::Duration::minutes(1),
            ),
            repo_b.claim_next_invoice_admission(
                "worker-b",
                task.submitted_at,
                time::Duration::minutes(1),
            ),
        );
        let claims = [claim_a.unwrap(), claim_b.unwrap()];
        assert_eq!(claims.iter().filter(|claim| claim.is_some()).count(), 1);

        database.cleanup().await;
    }

    #[tokio::test]
    async fn invoice_admission_fractional_claim_and_retry_durations_keep_precision() {
        let database = TestDatabase::create().await;
        let repo = PostgresVerificationTaskRepository::new(database.pool().clone());
        let mut task = task(VerificationTaskStatus::Pending);
        let reader =
            CreatorPubky::from_str("pubkyorhzqdiexwmi6iidktucgud63ufa5nwtsuzdxe176a8izd6jsqky")
                .unwrap();
        task.submitted_proof_bundle.reader_public_key = Some(reader.clone());
        let intent = InvoiceAdmissionIntentV1 {
            version: INVOICE_ADMISSION_INTENT_VERSION,
            creator: task.creator.clone(),
            bundle_id: task.submitted_proof_bundle.bundle_id.clone(),
            lock_resource: task.submitted_proof_bundle.pubky_lock_resource.clone(),
            reader,
        };
        repo.insert_invoice_pending_task(task.clone(), intent)
            .await
            .unwrap();

        let claim = repo
            .claim_next_invoice_admission(
                "worker-a",
                task.submitted_at,
                time::Duration::milliseconds(750),
            )
            .await
            .unwrap()
            .unwrap();
        let claim_kept_fractional_precision: bool = sqlx::query_scalar(
            "SELECT claim_expires_at = updated_at + INTERVAL '750 milliseconds'
             FROM verification_tasks WHERE task_id = $1::uuid",
        )
        .bind(task.task_id.to_string())
        .fetch_one(database.pool())
        .await
        .unwrap();
        assert!(claim_kept_fractional_precision);

        sqlx::query(
            "UPDATE verification_tasks
             SET claim_expires_at = clock_timestamp() + INTERVAL '1 minute'
             WHERE task_id = $1::uuid",
        )
        .bind(task.task_id.to_string())
        .execute(database.pool())
        .await
        .unwrap();
        let retry = repo
            .schedule_invoice_admission_retry(
                &task.task_id,
                "worker-a",
                &claim.claim_token,
                task.submitted_at,
                time::Duration::milliseconds(1_250),
                None,
            )
            .await
            .unwrap()
            .unwrap();
        let retry_kept_fractional_precision: bool = sqlx::query_scalar(
            "SELECT next_attempt_at = updated_at + INTERVAL '1250 milliseconds'
             FROM verification_tasks WHERE task_id = $1::uuid",
        )
        .bind(task.task_id.to_string())
        .fetch_one(database.pool())
        .await
        .unwrap();
        assert!(retry_kept_fractional_precision);
        assert!(retry.next_attempt_at.is_some());

        database.cleanup().await;
    }

    #[tokio::test]
    async fn invoice_admission_ready_uses_time_after_waiting_for_row_lock() {
        let database = TestDatabase::create().await;
        let repo = PostgresVerificationTaskRepository::new(database.pool().clone());
        let mut task = task(VerificationTaskStatus::Pending);
        let reader =
            CreatorPubky::from_str("pubkyorhzqdiexwmi6iidktucgud63ufa5nwtsuzdxe176a8izd6jsqky")
                .unwrap();
        task.submitted_proof_bundle.reader_public_key = Some(reader.clone());
        let intent = InvoiceAdmissionIntentV1 {
            version: INVOICE_ADMISSION_INTENT_VERSION,
            creator: task.creator.clone(),
            bundle_id: task.submitted_proof_bundle.bundle_id.clone(),
            lock_resource: task.submitted_proof_bundle.pubky_lock_resource.clone(),
            reader,
        };
        repo.insert_invoice_pending_task(task.clone(), intent)
            .await
            .unwrap();
        let claim = repo
            .claim_next_invoice_admission("worker-a", task.submitted_at, time::Duration::seconds(1))
            .await
            .unwrap()
            .unwrap();

        let mut blocker = database.pool().begin().await.unwrap();
        sqlx::query("SELECT task_id FROM verification_tasks WHERE task_id = $1::uuid FOR UPDATE")
            .bind(task.task_id.to_string())
            .fetch_one(&mut *blocker)
            .await
            .unwrap();
        let transition_repo = repo.clone();
        let task_id = task.task_id;
        let claim_token = claim.claim_token;
        let transition = tokio::spawn(async move {
            transition_repo
                .mark_invoice_admission_ready(
                    &task_id,
                    "worker-a",
                    &claim_token,
                    time::OffsetDateTime::now_utc(),
                )
                .await
        });
        tokio::time::sleep(std::time::Duration::from_millis(1_100)).await;
        blocker.commit().await.unwrap();

        assert_eq!(transition.await.unwrap().unwrap(), None);
        assert_eq!(
            repo.get_invoice_admission(&task.task_id)
                .await
                .unwrap()
                .unwrap()
                .phase,
            InvoiceAdmissionPhase::InvoicePending
        );

        database.cleanup().await;
    }

    #[tokio::test]
    async fn invoice_admission_postgres_retry_and_expired_failure_survive_repository_recreation() {
        let database = TestDatabase::create().await;
        let original_repo = PostgresVerificationTaskRepository::new(database.pool().clone());
        let recreated_repo = PostgresVerificationTaskRepository::new(database.pool().clone());
        let mut task = task(VerificationTaskStatus::Pending);
        let reader =
            CreatorPubky::from_str("pubkyorhzqdiexwmi6iidktucgud63ufa5nwtsuzdxe176a8izd6jsqky")
                .unwrap();
        task.submitted_proof_bundle.reader_public_key = Some(reader.clone());
        let intent = InvoiceAdmissionIntentV1 {
            version: INVOICE_ADMISSION_INTENT_VERSION,
            creator: task.creator.clone(),
            bundle_id: task.submitted_proof_bundle.bundle_id.clone(),
            lock_resource: task.submitted_proof_bundle.pubky_lock_resource.clone(),
            reader,
        };
        original_repo
            .insert_invoice_pending_task(task.clone(), intent)
            .await
            .unwrap();
        let first = original_repo
            .claim_next_invoice_admission("worker-a", task.submitted_at, time::Duration::minutes(1))
            .await
            .unwrap()
            .unwrap();
        let retry = original_repo
            .schedule_invoice_admission_retry(
                &task.task_id,
                "worker-a",
                &first.claim_token,
                task.submitted_at,
                time::Duration::seconds(5),
                Some(InvoiceAdmissionRetryReason::ReaderWalletSetupNeeded),
            )
            .await
            .unwrap()
            .unwrap();
        assert_eq!(retry.phase, InvoiceAdmissionPhase::InvoicePending);
        assert_eq!(retry.attempt_count, 1);
        assert!(retry.next_attempt_at.is_some());
        assert_eq!(
            recreated_repo
                .get_invoice_admission(&task.task_id)
                .await
                .unwrap()
                .unwrap()
                .retry_reason,
            Some(InvoiceAdmissionRetryReason::ReaderWalletSetupNeeded)
        );

        sqlx::query(
            "UPDATE verification_tasks SET next_attempt_at = clock_timestamp() WHERE task_id = $1::uuid",
        )
        .bind(task.task_id.to_string())
        .execute(database.pool())
        .await
        .unwrap();
        let transient_claim = recreated_repo
            .claim_next_invoice_admission("worker-b", task.submitted_at, time::Duration::minutes(1))
            .await
            .unwrap()
            .unwrap();
        let transient_retry = recreated_repo
            .schedule_invoice_admission_retry(
                &task.task_id,
                "worker-b",
                &transient_claim.claim_token,
                task.submitted_at,
                time::Duration::seconds(1),
                None,
            )
            .await
            .unwrap()
            .unwrap();
        assert_eq!(transient_retry.attempt_count, 2);
        assert_eq!(
            transient_retry.retry_reason,
            Some(InvoiceAdmissionRetryReason::ReaderWalletSetupNeeded)
        );

        sqlx::query(
            "UPDATE verification_tasks
             SET next_attempt_at = clock_timestamp(),
                 admission_deadline_at = clock_timestamp()
             WHERE task_id = $1::uuid",
        )
        .bind(task.task_id.to_string())
        .execute(database.pool())
        .await
        .unwrap();
        let expired_claim = recreated_repo
            .claim_next_invoice_admission("worker-b", task.submitted_at, time::Duration::minutes(1))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(expired_claim.admission.attempt_count, 3);
        assert!(expired_claim.deadline_expired);
        assert_eq!(
            recreated_repo
                .mark_invoice_admission_ready(
                    &task.task_id,
                    "worker-b",
                    &expired_claim.claim_token,
                    task.submitted_at,
                )
                .await
                .unwrap(),
            None
        );
        let failed = recreated_repo
            .mark_invoice_admission_failed(
                &task.task_id,
                "worker-b",
                &expired_claim.claim_token,
                task.submitted_at,
                "invoice admission failed",
            )
            .await
            .unwrap()
            .unwrap();
        assert_eq!(failed.phase, InvoiceAdmissionPhase::Failed);
        assert_eq!(failed.task.status, VerificationTaskStatus::Failed);
        assert_eq!(
            failed.task.failure_message.as_deref(),
            Some("invoice admission failed")
        );
        assert_eq!(failed.task.entitlement_to_publish, None);
        assert_eq!(failed.next_attempt_at, None);

        database.cleanup().await;
    }

    #[tokio::test]
    async fn expired_terminal_reason_survives_repository_wrapper_recreation() {
        let database = TestDatabase::create().await;
        let original_repo = PostgresVerificationTaskRepository::new(database.pool().clone());
        let recreated_repo = PostgresVerificationTaskRepository::new(database.pool().clone());
        let task_id = TaskId::from_str(TASK_ID).unwrap();
        let expired = task(VerificationTaskStatus::Pending)
            .transition_to(
                VerificationTaskStatus::InProgress,
                datetime!(2026-05-29 12:01:00 UTC),
                None,
            )
            .unwrap()
            .expire(
                VerificationTerminalReason::PaymentRequestCanceled,
                datetime!(2026-05-29 12:02:00 UTC),
            )
            .unwrap();

        original_repo
            .insert_verification_task(expired.clone())
            .await
            .unwrap();

        assert_eq!(
            recreated_repo
                .get_verification_task(&task_id)
                .await
                .unwrap(),
            Some(expired)
        );

        database.cleanup().await;
    }

    #[tokio::test]
    async fn insert_rejects_task_when_record_creator_diverges_from_submitted_bundle() {
        let database = TestDatabase::create().await;
        let repo = PostgresVerificationTaskRepository::new(database.pool().clone());
        let mut task = task(VerificationTaskStatus::Pending);
        task.creator =
            CreatorPubky::from_str("pubkyorhzqdiexwmi6iidktucgud63ufa5nwtsuzdxe176a8izd6jsqky")
                .unwrap();

        assert!(matches!(
            repo.insert_verification_task(task).await,
            Err(ApplicationError::Storage { message })
                if message.contains("verification task record creator diverges")
        ));

        assert_eq!(
            repo.get_verification_task(&TaskId::from_str(TASK_ID).unwrap())
                .await
                .unwrap(),
            None
        );

        database.cleanup().await;
    }

    #[tokio::test]
    async fn read_rejects_rows_when_handle_columns_diverge_from_submitted_bundle() {
        let database = TestDatabase::create().await;
        let repo = PostgresVerificationTaskRepository::new(database.pool().clone());
        let task_id = TaskId::from_str(TASK_ID).unwrap();

        repo.insert_verification_task(task(VerificationTaskStatus::Pending))
            .await
            .unwrap();
        sqlx::query("UPDATE verification_tasks SET creator = $1 WHERE task_id = $2::uuid")
            .bind("pubkyorhzqdiexwmi6iidktucgud63ufa5nwtsuzdxe176a8izd6jsqky")
            .bind(TASK_ID)
            .execute(database.pool())
            .await
            .unwrap();

        assert!(matches!(
            repo.get_verification_task(&task_id).await,
            Err(ApplicationError::Storage { message })
                if message.contains("verification task handle columns diverge")
        ));

        database.cleanup().await;
    }

    fn task(status: VerificationTaskStatus) -> VerificationTaskRecord {
        task_with(
            TASK_ID,
            "pubkytkrq8zmwb8a3m9k15csu3q17qmfgqnp9dskbrg9uq1rydpyxp7qy",
            BUNDLE_ID,
            status,
        )
    }

    fn task_with(
        task_id: &str,
        creator: &str,
        bundle_id: &str,
        status: VerificationTaskStatus,
    ) -> VerificationTaskRecord {
        VerificationTaskRecord {
            task_id: TaskId::from_str(task_id).unwrap(),
            creator: CreatorPubky::from_str(creator).unwrap(),
            submitted_proof_bundle: SubmittedProofBundle {
                version: SUBMITTED_PROOF_BUNDLE_VERSION,
                bundle_id: BundleId::from_str(bundle_id).unwrap(),
                pubky_lock_resource: PubkyLockResource::from_str(&format!(
                    "{creator}/pub/app.locks/{LOCK_ID}.json"
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
            submitted_at: datetime!(2026-05-29 12:00:00 UTC),
            started_at: None,
            completed_at: None,
            failure_message: None,
            terminal_reason: None,
            entitlement_to_publish: None,
        }
    }
}
