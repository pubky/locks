use std::str::FromStr;

use async_trait::async_trait;
use sqlx::{FromRow, PgPool};

use locks_core::ids::{BundleId, CreatorPubky, TaskId};
use locks_core::verification::SubmittedProofBundle;

use crate::application::errors::ApplicationError;
use crate::application::models::{VerificationTaskRecord, VerificationTaskStatus};
use crate::application::ports::VerificationTaskRepository;
use crate::infrastructure::postgres::proof_admission::lock_proof_admission;

/// Postgres-backed repository for Lock Server private verification task state.
#[derive(Debug, Clone)]
pub struct PostgresVerificationTaskRepository {
    pool: PgPool,
}

#[derive(Debug, FromRow)]
pub(super) struct VerificationTaskRow {
    pub(super) task_id: String,
    pub(super) creator: String,
    pub(super) bundle_id: String,
    pub(super) status: String,
    pub(super) submitted_proof_bundle: serde_json::Value,
    pub(super) submitted_at: time::OffsetDateTime,
    pub(super) started_at: Option<time::OffsetDateTime>,
    pub(super) completed_at: Option<time::OffsetDateTime>,
    pub(super) failure_message: Option<String>,
}

pub(super) struct VerificationTaskWriteRow {
    pub(super) task_id: String,
    pub(super) creator: String,
    pub(super) bundle_id: String,
    pub(super) status: &'static str,
    pub(super) submitted_proof_bundle: serde_json::Value,
    pub(super) submitted_at: time::OffsetDateTime,
    pub(super) started_at: Option<time::OffsetDateTime>,
    pub(super) completed_at: Option<time::OffsetDateTime>,
    pub(super) failure_message: Option<String>,
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
    failure_message";

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
        let lock_id = task.submitted_proof_bundle.pubky_lock_resource.lock_id();
        let mut transaction = self.pool.begin().await.map_err(storage_error)?;
        lock_proof_admission(&mut transaction, &task.creator, lock_id).await?;

        let handle_exists = sqlx::query_scalar::<_, bool>(
            "SELECT EXISTS (
                SELECT 1 FROM verification_tasks WHERE creator = $1 AND bundle_id = $2
            )",
        )
        .bind(&row.creator)
        .bind(&row.bundle_id)
        .fetch_one(&mut *transaction)
        .await
        .map_err(storage_error)?;
        if handle_exists {
            return Err(ApplicationError::DuplicateRecord {
                record: "verification_task",
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
                failure_message
            )
            VALUES ($1::uuid, $2, $3, $4, $5, $6, $7, $8, $9)
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
        .execute(&mut *transaction)
        .await
        .map_err(storage_error)?;

        if result.rows_affected() == 0 {
            return Err(ApplicationError::DuplicateRecord {
                record: "verification_task",
            });
        }

        transaction.commit().await.map_err(storage_error)
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
                updated_at = now()
            WHERE task_id = $1::uuid
              AND NOT EXISTS (
                  SELECT 1 FROM content_lock_deletion_task_snapshot AS snapshot
                  WHERE snapshot.verification_task_id = verification_tasks.task_id
              )",
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
            WHERE task_id = $1::uuid
              AND NOT EXISTS (
                  SELECT 1 FROM paykit_task_admissions
                  WHERE verification_task_id = verification_tasks.task_id
                    AND (
                        ready = FALSE
                        OR payment_in_hours IS NULL
                        OR payment_in_hours <= 0
                        OR invoice_created_at IS NULL
                        OR payment_deadline IS NULL
                        OR invoice_created_at > payment_deadline
                    )
              )"
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
            WHERE creator = $1 AND bundle_id = $2
              AND NOT EXISTS (
                  SELECT 1 FROM paykit_task_admissions
                  WHERE verification_task_id = verification_tasks.task_id
                    AND (
                        ready = FALSE
                        OR payment_in_hours IS NULL
                        OR payment_in_hours <= 0
                        OR invoice_created_at IS NULL
                        OR payment_deadline IS NULL
                        OR invoice_created_at > payment_deadline
                    )
              )"
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
        })
    }
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

pub(super) fn status_to_database(status: VerificationTaskStatus) -> &'static str {
    match status {
        VerificationTaskStatus::Pending => "pending",
        VerificationTaskStatus::InProgress => "in_progress",
        VerificationTaskStatus::Completed => "completed",
        VerificationTaskStatus::Failed => "failed",
        VerificationTaskStatus::Expired => "expired",
    }
}

fn status_from_database(status: &str) -> Result<VerificationTaskStatus, ApplicationError> {
    match status {
        "pending" => Ok(VerificationTaskStatus::Pending),
        "in_progress" => Ok(VerificationTaskStatus::InProgress),
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
    use crate::application::models::{VerificationTaskRecord, VerificationTaskStatus};
    use crate::application::ports::VerificationTaskRepository;
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
    async fn legacy_paykit_admission_without_authoritative_window_is_hidden_from_all_lookups() {
        let database = TestDatabase::create().await;
        let repo = PostgresVerificationTaskRepository::new(database.pool().clone());
        let pending = task(VerificationTaskStatus::Pending);
        let task_id = pending.task_id;
        let creator = pending.creator.clone();
        let bundle_id = pending.submitted_proof_bundle.bundle_id.clone();
        repo.insert_verification_task(pending).await.unwrap();
        sqlx::query(
            "INSERT INTO paykit_task_admissions
                 (verification_task_id, ready, ready_at)
             VALUES ($1::uuid, TRUE, now())",
        )
        .bind(task_id.to_string())
        .execute(database.pool())
        .await
        .unwrap();

        assert_eq!(repo.get_verification_task(&task_id).await.unwrap(), None);
        assert_eq!(
            repo.get_verification_task_by_handle(&creator, &bundle_id)
                .await
                .unwrap(),
            None
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
                    "{creator}/pub/locks.app/{LOCK_ID}.json"
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
        }
    }
}
