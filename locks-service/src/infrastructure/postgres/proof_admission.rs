use locks_core::ids::{CreatorPubky, LockId};
use sqlx::{PgPool, Postgres, Transaction};

use crate::application::errors::ApplicationError;
use crate::application::models::VerificationTaskRecord;
use locks_core::verification::SubmittedProofBundle;

use super::verification_tasks::{
    VERIFICATION_TASK_ROW_COLUMNS, VerificationTaskRow, VerificationTaskWriteRow, row_to_task,
};

const PROOF_ADMISSION_LOCK_NAMESPACE: &str = "locks:proof-admission:v1";

/// Result of durably reserving one Paykit-backed proof admission.
#[derive(Debug)]
pub struct PaykitTaskAdmission {
    /// The durable task associated with the public Bundle handle.
    pub task: VerificationTaskRecord,
    /// Whether the caller must create/reconcile the Paykit invoice before making the task claimable.
    pub requires_paykit: bool,
}

/// PostgreSQL coordinator for durable persist-before-Paykit admission.
#[derive(Debug, Clone)]
pub struct PostgresPaykitTaskAdmissionRepository {
    pool: PgPool,
}

impl PostgresPaykitTaskAdmissionRepository {
    /// Creates a coordinator backed by the migrated PostgreSQL pool.
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    /// Returns durable replay state without consulting mutable lock or reader discovery state.
    pub async fn find_existing(
        &self,
        submitted: &SubmittedProofBundle,
    ) -> Result<Option<PaykitTaskAdmission>, ApplicationError> {
        let sql = format!(
            "SELECT {VERIFICATION_TASK_ROW_COLUMNS},
                    COALESCE(admission.ready, TRUE) AS paykit_ready
             FROM verification_tasks AS task
             LEFT JOIN paykit_task_admissions AS admission
               ON admission.verification_task_id = task.task_id
             WHERE task.creator = $1 AND task.bundle_id = $2"
        );
        let Some(existing) = sqlx::query_as::<_, PaykitAdmissionRow>(&sql)
            .bind(submitted.pubky_lock_resource.creator().to_string())
            .bind(submitted.bundle_id.to_string())
            .fetch_optional(&self.pool)
            .await
            .map_err(storage_error)?
        else {
            return Ok(None);
        };
        let ready = existing.paykit_ready;
        let existing = row_to_task(existing.task)?;
        if existing.submitted_proof_bundle != *submitted {
            return Err(ApplicationError::VerificationTaskConflict);
        }
        Ok(Some(PaykitTaskAdmission {
            task: existing,
            requires_paykit: !ready,
        }))
    }

    /// Reserves a task before Paykit mutation, serialized against deletion start.
    pub async fn reserve(
        &self,
        task: VerificationTaskRecord,
    ) -> Result<PaykitTaskAdmission, ApplicationError> {
        let row = VerificationTaskWriteRow::try_from(&task)?;
        let lock_id = task.submitted_proof_bundle.pubky_lock_resource.lock_id();
        let mut transaction = self.pool.begin().await.map_err(storage_error)?;
        lock_proof_admission(&mut transaction, &task.creator, lock_id).await?;

        let existing_sql = format!(
            "SELECT {VERIFICATION_TASK_ROW_COLUMNS},
                    COALESCE(admission.ready, TRUE) AS paykit_ready
             FROM verification_tasks AS task
             LEFT JOIN paykit_task_admissions AS admission
               ON admission.verification_task_id = task.task_id
             WHERE task.creator = $1 AND task.bundle_id = $2"
        );
        if let Some(existing) = sqlx::query_as::<_, PaykitAdmissionRow>(&existing_sql)
            .bind(&row.creator)
            .bind(&row.bundle_id)
            .fetch_optional(&mut *transaction)
            .await
            .map_err(storage_error)?
        {
            let ready = existing.paykit_ready;
            let existing = row_to_task(existing.task)?;
            if existing.submitted_proof_bundle != task.submitted_proof_bundle {
                return Err(ApplicationError::VerificationTaskConflict);
            }
            transaction.commit().await.map_err(storage_error)?;
            return Ok(PaykitTaskAdmission {
                task: existing,
                requires_paykit: !ready,
            });
        }

        let deletion_exists = sqlx::query_scalar::<_, bool>(
            "SELECT EXISTS (
                SELECT 1 FROM content_lock_deletion_jobs WHERE creator = $1 AND lock_id = $2
            )",
        )
        .bind(&row.creator)
        .bind(lock_id.to_string())
        .fetch_one(&mut *transaction)
        .await
        .map_err(storage_error)?;
        if deletion_exists {
            return Err(ApplicationError::ContentLockDeletionInProgress);
        }

        insert_task(&mut transaction, row).await?;
        sqlx::query(
            "INSERT INTO paykit_task_admissions (verification_task_id, ready)
             VALUES ($1::uuid, FALSE)",
        )
        .bind(task.task_id.to_string())
        .execute(&mut *transaction)
        .await
        .map_err(storage_error)?;
        transaction.commit().await.map_err(storage_error)?;

        Ok(PaykitTaskAdmission {
            task,
            requires_paykit: true,
        })
    }

    /// Makes a reserved task claimable after Paykit confirms invoice creation/replay.
    pub async fn mark_ready(&self, task: &VerificationTaskRecord) -> Result<(), ApplicationError> {
        let result = sqlx::query(
            "UPDATE paykit_task_admissions
             SET ready = TRUE, ready_at = COALESCE(ready_at, now())
             WHERE verification_task_id = $1::uuid",
        )
        .bind(task.task_id.to_string())
        .execute(&self.pool)
        .await
        .map_err(storage_error)?;
        if result.rows_affected() == 0 {
            return Err(ApplicationError::MissingRecord {
                record: "paykit_task_admission",
            });
        }
        Ok(())
    }
}

#[derive(sqlx::FromRow)]
struct PaykitAdmissionRow {
    #[sqlx(flatten)]
    task: VerificationTaskRow,
    paykit_ready: bool,
}

async fn insert_task(
    transaction: &mut Transaction<'_, Postgres>,
    row: VerificationTaskWriteRow,
) -> Result<(), ApplicationError> {
    sqlx::query(
        "INSERT INTO verification_tasks (
            task_id, creator, bundle_id, status, submitted_proof_bundle,
            submitted_at, started_at, completed_at, failure_message
         ) VALUES ($1::uuid, $2, $3, $4, $5, $6, $7, $8, $9)",
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
    .execute(&mut **transaction)
    .await
    .map_err(storage_error)?;
    Ok(())
}

pub(super) async fn lock_proof_admission(
    transaction: &mut Transaction<'_, Postgres>,
    creator: &CreatorPubky,
    lock_id: &LockId,
) -> Result<(), ApplicationError> {
    sqlx::query("SELECT pg_advisory_xact_lock(hashtextextended($1, 0))")
        .bind(proof_admission_lock_key(creator, lock_id))
        .execute(&mut **transaction)
        .await
        .map_err(storage_error)?;
    Ok(())
}

fn proof_admission_lock_key(creator: &CreatorPubky, lock_id: &LockId) -> String {
    format!(
        "{PROOF_ADMISSION_LOCK_NAMESPACE}:{creator}:{}",
        lock_id.as_str()
    )
}

fn storage_error(error: sqlx::Error) -> ApplicationError {
    ApplicationError::Storage {
        message: error.to_string(),
    }
}
