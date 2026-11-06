use std::str::FromStr;

use async_trait::async_trait;
use locks_core::ids::{BundleId, CreatorPubky, TaskId};
use locks_core::verification::{SatisfiedByV2, VerifiedProofBundleV2};
use sqlx::{Connection, PgConnection, PgPool};

use crate::application::errors::ApplicationError;
use crate::application::models::{
    ClaimedGraphTask, GraphRootState, GraphTaskRecord, GraphTaskSnapshot, VerificationTaskStatus,
    VerificationTerminalReason,
};
use crate::application::ports::{
    EvaluatorMode, EvaluatorTaskKernel, GraphPublicationActionGuard, GraphTaskClaimer,
    GraphTaskRepository, build_evaluator_publication,
};

use super::evaluator_tasks::{
    EVALUATOR_TASK_COLUMNS as GRAPH_TASK_COLUMNS, EvaluatorSnapshotTransition,
    EvaluatorTaskRow as GraphTaskRow, claim_next, complete_publication_connection,
    complete_publication_pool, persist_claimed_snapshot, release_claim, schedule_claimed_retry,
};
use super::proof_admission::{lock_proof_admission, proof_admission_lock_key};
use super::verification_tasks::{reject_deletion_cutoff, status_from_database};

#[derive(Debug, Clone)]
pub struct PostgresGraphTaskRepository {
    pool: PgPool,
}

struct PostgresGraphPublicationActionGuard {
    connection: Option<PgConnection>,
    task_id: String,
    worker_id: String,
    claim_token: uuid::Uuid,
    expected_revision: i64,
    publication_token: uuid::Uuid,
}

#[async_trait]
impl GraphPublicationActionGuard for PostgresGraphPublicationActionGuard {
    async fn complete(&mut self) -> Result<Option<GraphTaskRecord>, ApplicationError> {
        let connection = self
            .connection
            .as_mut()
            .ok_or_else(|| ApplicationError::Storage {
                message: "graph publication action ownership was already released".to_owned(),
            })?;
        let row = complete_publication_connection(
            connection,
            EvaluatorMode::Graph,
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
        let connection = self
            .connection
            .take()
            .ok_or_else(|| ApplicationError::Storage {
                message: "graph publication action ownership was already released".to_owned(),
            })?;
        connection.close().await.map_err(storage_error)
    }
}

impl PostgresGraphTaskRepository {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }
}

#[async_trait]
impl GraphTaskRepository for PostgresGraphTaskRepository {
    async fn insert_graph_task(
        &self,
        task: GraphTaskRecord,
    ) -> Result<GraphTaskRecord, ApplicationError> {
        validate_new_task(&task)?;
        let snapshot = task.snapshot.as_v1();
        let snapshot_json = task.snapshot.to_json().map_err(snapshot_encode_error)?;
        let submitted = serde_json::json!({
            "version": 2,
            "bundle_id": snapshot.bundle_id,
            "pubky_lock_resource": snapshot.pubky_lock_resource,
        });
        let mut transaction = self.pool.begin().await.map_err(storage_error)?;
        lock_proof_admission(
            &mut transaction,
            &snapshot.creator,
            snapshot.pubky_lock_resource.lock_id(),
        )
        .await?;

        let pending_folded_auth: bool = sqlx::query_scalar(
            "SELECT EXISTS(
                SELECT 1 FROM folded_auth_attempts
                WHERE creator = $1 AND bundle_id = $2 AND status = 'pending'
             )",
        )
        .bind(snapshot.creator.to_string())
        .bind(snapshot.bundle_id.to_string())
        .fetch_one(&mut *transaction)
        .await
        .map_err(storage_error)?;
        if pending_folded_auth {
            return Err(ApplicationError::VerificationTaskConflict);
        }

        let existing = sqlx::query_as::<_, GraphTaskRow>(&format!(
            "SELECT {GRAPH_TASK_COLUMNS} FROM verification_tasks
             WHERE creator = $1 AND bundle_id = $2
               AND evaluator_mode = 'graph' AND evaluator_snapshot IS NOT NULL
             FOR UPDATE"
        ))
        .bind(snapshot.creator.to_string())
        .bind(snapshot.bundle_id.to_string())
        .fetch_optional(&mut *transaction)
        .await
        .map_err(storage_error)?;
        if let Some(existing) = existing {
            let existing = row_to_task(existing)?;
            if existing.task_id != task.task_id
                || ensure_same_binding(&existing.snapshot, &task.snapshot).is_err()
            {
                return Err(ApplicationError::VerificationTaskConflict);
            }
            transaction.commit().await.map_err(storage_error)?;
            return self
                .get_graph_task_by_handle(&snapshot.creator, &snapshot.bundle_id)
                .await?
                .ok_or(ApplicationError::VerificationTaskConflict);
        }

        reject_deletion_cutoff(
            &mut transaction,
            &snapshot.creator,
            snapshot.pubky_lock_resource.lock_id(),
        )
        .await?;
        let inserted = sqlx::query_as::<_, GraphTaskRow>(&format!(
            "INSERT INTO verification_tasks (
                task_id, creator, bundle_id, status, submitted_proof_bundle,
                submitted_at, invoice_admission_phase, evaluator_mode, evaluator_snapshot,
                evaluator_revision
             ) VALUES ($1::uuid, $2, $3, 'pending', $4, $5, 'ready', 'graph', $6, 0)
             ON CONFLICT DO NOTHING
             RETURNING {GRAPH_TASK_COLUMNS}"
        ))
        .bind(task.task_id.to_string())
        .bind(snapshot.creator.to_string())
        .bind(snapshot.bundle_id.to_string())
        .bind(submitted)
        .bind(snapshot.accepted_at)
        .bind(snapshot_json)
        .fetch_optional(&mut *transaction)
        .await
        .map_err(storage_error)?;
        let Some(inserted) = inserted else {
            let winner = sqlx::query_as::<_, GraphTaskRow>(&format!(
                "SELECT {GRAPH_TASK_COLUMNS} FROM verification_tasks
                 WHERE creator = $1 AND bundle_id = $2
                   AND evaluator_mode = 'graph' AND evaluator_snapshot IS NOT NULL
                 FOR UPDATE"
            ))
            .bind(snapshot.creator.to_string())
            .bind(snapshot.bundle_id.to_string())
            .fetch_optional(&mut *transaction)
            .await
            .map_err(storage_error)?;
            if let Some(winner) = winner {
                let winner = row_to_task(winner)?;
                if winner.task_id == task.task_id
                    && ensure_same_binding(&winner.snapshot, &task.snapshot).is_ok()
                {
                    transaction.commit().await.map_err(storage_error)?;
                    return Ok(winner);
                }
                return Err(ApplicationError::VerificationTaskConflict);
            }
            let handle_exists: bool = sqlx::query_scalar(
                "SELECT EXISTS(
                    SELECT 1 FROM verification_tasks
                    WHERE creator = $1 AND bundle_id = $2
                )",
            )
            .bind(snapshot.creator.to_string())
            .bind(snapshot.bundle_id.to_string())
            .fetch_one(&mut *transaction)
            .await
            .map_err(storage_error)?;
            if handle_exists {
                return Err(ApplicationError::VerificationTaskConflict);
            }
            return Err(ApplicationError::DuplicateRecord {
                record: "verification_task",
            });
        };
        transaction.commit().await.map_err(storage_error)?;
        row_to_task(inserted)
    }

    async fn get_graph_task_by_handle(
        &self,
        creator: &CreatorPubky,
        bundle_id: &BundleId,
    ) -> Result<Option<GraphTaskRecord>, ApplicationError> {
        let row = sqlx::query_as::<_, GraphTaskRow>(&format!(
            "SELECT {GRAPH_TASK_COLUMNS} FROM verification_tasks
             WHERE creator = $1 AND bundle_id = $2 AND evaluator_mode = 'graph' AND evaluator_snapshot IS NOT NULL"
        ))
        .bind(creator.to_string())
        .bind(bundle_id.to_string())
        .fetch_optional(&self.pool)
        .await
        .map_err(storage_error)?;
        row.map(row_to_task).transpose()
    }
}

#[async_trait]
impl GraphTaskClaimer for PostgresGraphTaskRepository {
    async fn claim_next_graph_task(
        &self,
        worker_id: &str,
        now: time::OffsetDateTime,
        claim_expires_at: time::OffsetDateTime,
    ) -> Result<Option<ClaimedGraphTask>, ApplicationError> {
        EvaluatorTaskKernel::claim_ttl(now, claim_expires_at).map_err(|_| {
            ApplicationError::InvalidVerificationTaskState {
                message: "graph task claim expiry must follow claim time".to_owned(),
            }
        })?;
        let row = claim_next(
            &self.pool,
            EvaluatorMode::Graph,
            worker_id,
            now,
            claim_expires_at,
        )
        .await?;
        row.map(|(row, claim_token)| {
            row_to_task(row).map(|task| ClaimedGraphTask { task, claim_token })
        })
        .transpose()
    }

    async fn persist_claimed_graph_snapshot(
        &self,
        task_id: &TaskId,
        worker_id: &str,
        claim_token: &uuid::Uuid,
        _now: time::OffsetDateTime,
        expected_revision: u64,
        snapshot: GraphTaskSnapshot,
    ) -> Result<Option<GraphTaskRecord>, ApplicationError> {
        EvaluatorTaskKernel::next_revision(expected_revision)?;
        let expected_revision = match i64::try_from(expected_revision) {
            Ok(revision) => revision,
            Err(_) => return Ok(None),
        };
        let row = persist_claimed_snapshot(
            &self.pool,
            EvaluatorMode::Graph,
            &task_id.to_string(),
            worker_id,
            claim_token,
            expected_revision,
            |current, database_now| {
                let current = row_to_task(current.clone())?;
                snapshot.as_v1().validate().map_err(invalid_snapshot)?;
                snapshot
                    .as_v1()
                    .validate_observations_not_after(database_now)
                    .map_err(invalid_snapshot)?;
                ensure_same_binding(&current.snapshot, &snapshot)?;
                if current.snapshot != snapshot
                    || current.snapshot.as_v1().root == GraphRootState::Pending
                {
                    snapshot
                        .as_v1()
                        .validate_progress_from(current.snapshot.as_v1())
                        .map_err(invalid_snapshot)?;
                }
                let root = snapshot.as_v1().root;
                if matches!(root, GraphRootState::Expired(_))
                    && database_now < snapshot.as_v1().deadline_at
                {
                    return Err(ApplicationError::InvalidVerificationTaskState {
                        message: "graph task cannot expire before its deadline".to_owned(),
                    });
                }
                let (status, reason, entitlement, publication_token) = match root {
                    GraphRootState::Pending => ("in_progress", None, None, None),
                    GraphRootState::Satisfied => {
                        let publication = build_evaluator_publication(
                            &snapshot.as_v1().identity(),
                            EvaluatorMode::Graph,
                            database_now,
                        )
                        .map_err(|error| ApplicationError::Storage {
                            message: format!("invalid graph entitlement publication: {error}"),
                        })?;
                        (
                            "publishing_entitlement",
                            None,
                            Some(serde_json::to_value(publication).map_err(snapshot_encode_error)?),
                            Some(uuid::Uuid::new_v4()),
                        )
                    }
                    GraphRootState::Expired(
                        crate::application::models::GraphDeadlineResult::False,
                    ) => (
                        "expired",
                        Some("graph_condition_not_observed_by_deadline".to_owned()),
                        None,
                        None,
                    ),
                    GraphRootState::Expired(
                        crate::application::models::GraphDeadlineResult::DependencyUnavailable,
                    ) => (
                        "failed",
                        Some("verification_unavailable_by_deadline".to_owned()),
                        None,
                        None,
                    ),
                    GraphRootState::FailedInvalidResponse => (
                        "failed",
                        Some("invalid_verifier_response".to_owned()),
                        None,
                        None,
                    ),
                };
                Ok((
                    snapshot.to_json().map_err(snapshot_encode_error)?,
                    EvaluatorSnapshotTransition {
                        status,
                        terminal_reason: reason,
                        failure_message: None,
                        entitlement,
                        publication_token,
                        retain_claim: matches!(
                            root,
                            GraphRootState::Pending | GraphRootState::Satisfied
                        ),
                    },
                ))
            },
        )
        .await?;
        row.map(row_to_task).transpose()
    }

    async fn schedule_graph_task_retry(
        &self,
        task_id: &TaskId,
        worker_id: &str,
        claim_token: &uuid::Uuid,
        now: time::OffsetDateTime,
        expected_revision: u64,
        snapshot: GraphTaskSnapshot,
        next_attempt_at: time::OffsetDateTime,
    ) -> Result<Option<GraphTaskRecord>, ApplicationError> {
        EvaluatorTaskKernel::next_revision(expected_revision)?;
        let expected_revision = match i64::try_from(expected_revision) {
            Ok(revision) => revision,
            Err(_) => return Ok(None),
        };

        let row = schedule_claimed_retry(
            &self.pool,
            EvaluatorMode::Graph,
            &task_id.to_string(),
            worker_id,
            claim_token,
            now,
            expected_revision,
            next_attempt_at,
            |current, database_now| {
                let current = row_to_task(current.clone())?;
                snapshot.as_v1().validate().map_err(invalid_snapshot)?;
                snapshot
                    .as_v1()
                    .validate_observations_not_after(database_now)
                    .map_err(invalid_snapshot)?;
                ensure_same_binding(&current.snapshot, &snapshot)?;
                snapshot
                    .as_v1()
                    .validate_progress_from(current.snapshot.as_v1())
                    .map_err(invalid_snapshot)?;
                if snapshot.as_v1().root != GraphRootState::Pending {
                    return Err(ApplicationError::InvalidVerificationTaskState {
                        message: "graph retry requires pending snapshot".to_owned(),
                    });
                }
                Ok((
                    snapshot.to_json().map_err(snapshot_encode_error)?,
                    snapshot.as_v1().deadline_at,
                ))
            },
        )
        .await?;
        row.map(row_to_task).transpose()
    }

    async fn release_claimed_graph_task(
        &self,
        task_id: &TaskId,
        worker_id: &str,
        claim_token: &uuid::Uuid,
    ) -> Result<bool, ApplicationError> {
        release_claim(
            &self.pool,
            EvaluatorMode::Graph,
            &task_id.to_string(),
            worker_id,
            claim_token,
        )
        .await
    }

    async fn acquire_graph_publication_action(
        &self,
        task_id: &TaskId,
        worker_id: &str,
        claim_token: &uuid::Uuid,
        expected_revision: u64,
        publication_token: uuid::Uuid,
    ) -> Result<Option<Box<dyn GraphPublicationActionGuard>>, ApplicationError> {
        let expected_revision = match i64::try_from(expected_revision) {
            Ok(revision) => revision,
            Err(_) => return Ok(None),
        };
        let identity = sqlx::query_as::<_, GraphTaskRow>(&format!(
            "SELECT {GRAPH_TASK_COLUMNS} FROM verification_tasks WHERE task_id = $1::uuid AND evaluator_mode = 'graph'"
        ))
        .bind(task_id.to_string())
        .fetch_optional(&self.pool)
        .await
        .map_err(storage_error)?
        .map(row_to_task)
        .transpose()?;
        let Some(identity) = identity else {
            return Ok(None);
        };
        let snapshot = identity.snapshot.as_v1();
        let lock_key =
            proof_admission_lock_key(&snapshot.creator, snapshot.pubky_lock_resource.lock_id());
        let mut pooled = self.pool.acquire().await.map_err(storage_error)?;
        pooled.close_on_drop();
        sqlx::query("SELECT pg_advisory_lock(hashtextextended($1, 0))")
            .bind(lock_key)
            .execute(&mut *pooled)
            .await
            .map_err(storage_error)?;
        let mut connection = pooled.detach();
        let live: bool = sqlx::query_scalar(
            "SELECT EXISTS (
                SELECT 1 FROM verification_tasks
                WHERE task_id = $1::uuid
               AND evaluator_mode = 'graph'
                  AND status = 'publishing_entitlement'
                  AND claimed_by = $2 AND claim_token = $3
                  AND claim_expires_at > clock_timestamp()
                  AND evaluator_revision = $4
                  AND entitlement_publication_claim_token = $5
                  AND deletion_job_id IS NULL
                  AND NOT EXISTS (
                    SELECT 1 FROM content_lock_deletion_task_snapshot deletion_snapshot
                    WHERE deletion_snapshot.verification_task_id = verification_tasks.task_id
                  )
             )",
        )
        .bind(task_id.to_string())
        .bind(worker_id)
        .bind(claim_token)
        .bind(expected_revision)
        .bind(publication_token)
        .fetch_one(&mut connection)
        .await
        .map_err(storage_error)?;
        if !live {
            connection.close().await.map_err(storage_error)?;
            return Ok(None);
        }
        Ok(Some(Box::new(PostgresGraphPublicationActionGuard {
            connection: Some(connection),
            task_id: task_id.to_string(),
            worker_id: worker_id.to_owned(),
            claim_token: *claim_token,
            expected_revision,
            publication_token,
        })))
    }

    async fn complete_claimed_graph_publication(
        &self,
        task_id: &TaskId,
        worker_id: &str,
        claim_token: &uuid::Uuid,
        _now: time::OffsetDateTime,
        expected_revision: u64,
        publication_token: uuid::Uuid,
    ) -> Result<Option<GraphTaskRecord>, ApplicationError> {
        let expected_revision = match i64::try_from(expected_revision) {
            Ok(revision) => revision,
            Err(_) => return Ok(None),
        };
        let row = complete_publication_pool(
            &self.pool,
            EvaluatorMode::Graph,
            &task_id.to_string(),
            worker_id,
            *claim_token,
            expected_revision,
            publication_token,
        )
        .await?;
        row.map(row_to_task).transpose()
    }
}

fn row_to_task(row: GraphTaskRow) -> Result<GraphTaskRecord, ApplicationError> {
    if row.evaluator_mode != "graph" {
        return Err(ApplicationError::Storage {
            message: "non-graph evaluator row reached graph decoder".to_owned(),
        });
    }
    let task_id = TaskId::from_str(&row.task_id).map_err(|error| ApplicationError::Storage {
        message: format!("invalid graph task id stored in Postgres: {error}"),
    })?;
    let snapshot = GraphTaskSnapshot::from_json(row.evaluator_snapshot).map_err(|error| {
        ApplicationError::Storage {
            message: format!("invalid graph task snapshot stored in Postgres: {error}"),
        }
    })?;
    let identity = snapshot.as_v1();
    let submitted_bundle = row
        .submitted_proof_bundle
        .get("bundle_id")
        .and_then(serde_json::Value::as_str);
    let submitted_resource = row
        .submitted_proof_bundle
        .get("pubky_lock_resource")
        .and_then(serde_json::Value::as_str);
    if row.creator != identity.creator.to_string()
        || row.bundle_id != identity.bundle_id.to_string()
        || row.submitted_at != identity.accepted_at
        || submitted_bundle != Some(identity.bundle_id.to_string().as_str())
        || submitted_resource != Some(identity.pubky_lock_resource.to_string().as_str())
    {
        return Err(ApplicationError::Storage {
            message: "graph task snapshot diverges from relational identity or timing".to_owned(),
        });
    }
    let evaluator_revision =
        u64::try_from(row.evaluator_revision).map_err(|_| ApplicationError::Storage {
            message: "negative graph evaluator revision stored in Postgres".to_owned(),
        })?;
    let entitlement_to_publish: Option<VerifiedProofBundleV2> = row
        .entitlement_to_publish
        .map(|value| {
            serde_json::from_value(value).map_err(|error| ApplicationError::Storage {
                message: format!("invalid graph entitlement payload stored in Postgres: {error}"),
            })
        })
        .transpose()?;
    if let Some(entitlement) = entitlement_to_publish.as_ref() {
        entitlement
            .validate()
            .map_err(|error| ApplicationError::Storage {
                message: format!("invalid graph entitlement payload stored in Postgres: {error}"),
            })?;
        if entitlement.creator != identity.creator
            || entitlement.reader != identity.reader
            || entitlement.bundle_id != identity.bundle_id
            || entitlement.pubky_lock_resource != identity.pubky_lock_resource
            || entitlement.satisfied_by != [SatisfiedByV2::Graph]
        {
            return Err(ApplicationError::Storage {
                message: "graph entitlement payload diverges from evaluator snapshot".to_owned(),
            });
        }
    }
    let status = status_from_database(&row.status)?;
    let terminal_reason = row
        .terminal_reason
        .as_deref()
        .map(|value| {
            VerificationTerminalReason::from_storage_value(value).ok_or_else(|| {
                ApplicationError::Storage {
                    message: format!(
                        "invalid graph task terminal reason stored in Postgres: {value}"
                    ),
                }
            })
        })
        .transpose()?;
    let lifecycle_matches = match status {
        VerificationTaskStatus::Pending => {
            identity.root == GraphRootState::Pending
                && entitlement_to_publish.is_none()
                && row.entitlement_publication_claim_token.is_none()
                && row.completed_at.is_none()
        }
        VerificationTaskStatus::InProgress => {
            // Pre-0030 graph workers could persist a terminal root while leaving the
            // relational lifecycle in progress. Keep that exact shape readable so a
            // fresh claim can finalize it through the fenced current transition.
            entitlement_to_publish.is_none()
                && row.entitlement_publication_claim_token.is_none()
                && row.completed_at.is_none()
        }
        VerificationTaskStatus::PublishingEntitlement => {
            identity.root == GraphRootState::Satisfied
                && entitlement_to_publish.is_some()
                && row.entitlement_publication_claim_token.is_some()
                && row.completed_at.is_none()
        }
        VerificationTaskStatus::Completed => {
            identity.root == GraphRootState::Satisfied
                && entitlement_to_publish.is_some()
                && row.entitlement_publication_claim_token.is_none()
                && row.completed_at.is_some()
        }
        VerificationTaskStatus::Expired => {
            (identity.root
                == GraphRootState::Expired(crate::application::models::GraphDeadlineResult::False)
                || terminal_reason == Some(VerificationTerminalReason::ContentLockDeleted))
                && entitlement_to_publish.is_none()
                && row.entitlement_publication_claim_token.is_none()
                && row.completed_at.is_some()
        }
        VerificationTaskStatus::Failed => {
            matches!(
                identity.root,
                GraphRootState::Expired(
                    crate::application::models::GraphDeadlineResult::DependencyUnavailable
                ) | GraphRootState::FailedInvalidResponse
            ) && entitlement_to_publish.is_none()
                && row.entitlement_publication_claim_token.is_none()
                && row.completed_at.is_some()
        }
    };
    if !lifecycle_matches {
        return Err(ApplicationError::Storage {
            message: "graph task lifecycle diverges from evaluator snapshot".to_owned(),
        });
    }
    Ok(GraphTaskRecord {
        task_id,
        snapshot,
        evaluator_revision,
        status,
        entitlement_to_publish,
        entitlement_publication_token: row.entitlement_publication_claim_token,
        completed_at: row.completed_at,
    })
}

fn validate_new_task(task: &GraphTaskRecord) -> Result<(), ApplicationError> {
    if task.status != VerificationTaskStatus::Pending
        || task.evaluator_revision != 0
        || task.entitlement_to_publish.is_some()
        || task.entitlement_publication_token.is_some()
        || task.completed_at.is_some()
    {
        return Err(ApplicationError::InvalidVerificationTaskState {
            message: "new graph task must be pristine and pending at evaluator revision zero"
                .to_owned(),
        });
    }
    task.snapshot
        .as_v1()
        .validate_pristine_admission()
        .map_err(invalid_snapshot)
}

fn ensure_same_binding(
    current: &GraphTaskSnapshot,
    next: &GraphTaskSnapshot,
) -> Result<(), ApplicationError> {
    let current = current.as_v1();
    let next = next.as_v1();
    if current.creator != next.creator
        || current.bundle_id != next.bundle_id
        || current.pubky_lock_resource != next.pubky_lock_resource
        || current.lock_hash != next.lock_hash
        || current.reader != next.reader
        || current.accepted_at != next.accepted_at
        || current.deadline_at != next.deadline_at
        || current.operator != next.operator
        || current
            .predicates
            .iter()
            .map(|state| &state.predicate)
            .ne(next.predicates.iter().map(|state| &state.predicate))
    {
        return Err(ApplicationError::InvalidVerificationTaskState {
            message: "graph snapshot replacement changed immutable task binding".to_owned(),
        });
    }
    Ok(())
}

fn invalid_snapshot(message: String) -> ApplicationError {
    ApplicationError::Storage {
        message: format!("invalid graph task snapshot: {message}"),
    }
}

fn snapshot_encode_error(error: serde_json::Error) -> ApplicationError {
    ApplicationError::Storage {
        message: format!("serialize graph task snapshot: {error}"),
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

    use locks_core::ids::{BundleId, CreatorPubky, LockHash, LockId, PubkyLockResource, TaskId};
    use locks_core::lock_policy::{GraphParticipantV2, GraphPolicyV2, GraphPredicateV2};
    use time::macros::datetime;

    use super::PostgresGraphTaskRepository;
    use crate::application::graph0conf_reducer::reduce_graph_snapshot;
    use crate::application::models::{
        GraphObservationOutcome, GraphRootState, GraphTaskRecord, GraphTaskSnapshot,
        GraphTaskSnapshotV1, VerificationTaskStatus,
    };
    use crate::application::ports::{
        GraphTaskClaimer, GraphTaskRepository, VerificationTaskClaimer, VerificationTaskRepository,
    };
    use crate::infrastructure::postgres::testing::TestDatabase;
    use crate::infrastructure::postgres::{
        PostgresVerificationTaskClaimer, PostgresVerificationTaskRepository,
    };

    const CREATOR: &str = "pubkytkrq8zmwb8a3m9k15csu3q17qmfgqnp9dskbrg9uq1rydpyxp7qy";
    const READER: &str = "pubkyorhzqdiexwmi6iidktucgud63ufa5nwtsuzdxe176a8izd6jsqky";
    const BUNDLE_ID: &str = "000G40R40M30E209185GR38E1W";
    const TASK_ID: &str = "018fc6ec-2f3d-4f7e-8b7d-6f5c4b3a2d10";
    const NOW: time::OffsetDateTime = datetime!(2026-10-01 12:00:00 UTC);

    #[tokio::test]
    async fn graph_snapshot_persistence_is_fenced_and_has_no_invoice_intent() {
        let database = TestDatabase::create().await;
        let repository = PostgresGraphTaskRepository::new(database.pool().clone());
        let inserted = repository.insert_graph_task(task()).await.unwrap();
        let invoice_intent: Option<serde_json::Value> = sqlx::query_scalar(
            "SELECT invoice_admission_intent FROM verification_tasks WHERE task_id = $1::uuid",
        )
        .bind(inserted.task_id.to_string())
        .fetch_one(database.pool())
        .await
        .unwrap();
        assert_eq!(invoice_intent, None);
        assert!(
            sqlx::query(
                "UPDATE verification_tasks
                 SET invoice_admission_intent = '{}'::jsonb
                 WHERE task_id = $1::uuid",
            )
            .bind(inserted.task_id.to_string())
            .execute(database.pool())
            .await
            .is_err()
        );

        let ordinary = PostgresVerificationTaskClaimer::new(database.pool().clone());
        assert_eq!(
            ordinary
                .claim_next_verification_task("v1-worker", NOW, NOW + time::Duration::minutes(5),)
                .await
                .unwrap(),
            None
        );
        let ordinary_repository = PostgresVerificationTaskRepository::new(database.pool().clone());
        assert_eq!(
            ordinary_repository
                .get_verification_task_by_handle(&creator(), &bundle_id())
                .await
                .unwrap(),
            None
        );
        ordinary_repository
            .delete_verification_task(&inserted.task_id)
            .await
            .unwrap();
        assert!(
            repository
                .get_graph_task_by_handle(&creator(), &bundle_id())
                .await
                .unwrap()
                .is_some()
        );

        let claim = repository
            .claim_next_graph_task("graph-worker", NOW, NOW + time::Duration::minutes(5))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            repository.insert_graph_task(task()).await.unwrap(),
            claim.task
        );
        let mut next = claim.task.snapshot.clone();
        let GraphTaskSnapshot::V1(snapshot) = &mut next;
        snapshot
            .record_observation(
                0,
                GraphObservationOutcome::False,
                NOW + time::Duration::seconds(1),
            )
            .unwrap();
        let persisted = repository
            .persist_claimed_graph_snapshot(
                &claim.task.task_id,
                "graph-worker",
                &claim.claim_token,
                NOW + time::Duration::seconds(2),
                0,
                next.clone(),
            )
            .await
            .unwrap()
            .unwrap();
        assert_eq!(persisted.evaluator_revision, 1);
        assert_eq!(persisted.snapshot, next);
        assert_eq!(
            repository.insert_graph_task(task()).await.unwrap(),
            persisted
        );
        assert_eq!(
            repository
                .persist_claimed_graph_snapshot(
                    &claim.task.task_id,
                    "graph-worker",
                    &claim.claim_token,
                    NOW + time::Duration::seconds(3),
                    0,
                    next,
                )
                .await
                .unwrap(),
            None
        );

        let mut terminal = persisted.snapshot;
        let GraphTaskSnapshot::V1(snapshot) = &mut terminal;
        snapshot
            .record_observation(
                0,
                GraphObservationOutcome::True,
                NOW + time::Duration::seconds(4),
            )
            .unwrap();
        reduce_graph_snapshot(snapshot, NOW + time::Duration::seconds(4)).unwrap();
        let publishing = repository
            .persist_claimed_graph_snapshot(
                &claim.task.task_id,
                "graph-worker",
                &claim.claim_token,
                NOW + time::Duration::seconds(5),
                1,
                terminal,
            )
            .await
            .unwrap()
            .unwrap();
        let publication_token = publishing.entitlement_publication_token.unwrap();
        let action = repository
            .acquire_graph_publication_action(
                &publishing.task_id,
                "graph-worker",
                &claim.claim_token,
                publishing.evaluator_revision,
                publication_token,
            )
            .await
            .unwrap()
            .unwrap();
        let blocked_admission = {
            let pool = database.pool().clone();
            let creator = publishing.snapshot.as_v1().creator.clone();
            let lock_id = publishing
                .snapshot
                .as_v1()
                .pubky_lock_resource
                .lock_id()
                .clone();
            tokio::spawn(async move {
                let mut transaction = pool.begin().await.unwrap();
                super::super::proof_admission::lock_proof_admission(
                    &mut transaction,
                    &creator,
                    &lock_id,
                )
                .await
                .unwrap();
                transaction.rollback().await.unwrap();
            })
        };
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(20), async {
                while !blocked_admission.is_finished() {
                    tokio::task::yield_now().await;
                }
            })
            .await
            .is_err(),
            "deletion/admission transaction must wait for publication action ownership"
        );
        action.release().await.unwrap();
        blocked_admission.await.unwrap();
        assert_eq!(
            repository
                .claim_next_graph_task(
                    "other-worker",
                    NOW + time::Duration::minutes(5),
                    NOW + time::Duration::minutes(10),
                )
                .await
                .unwrap(),
            None
        );
        database.cleanup().await;
    }

    #[tokio::test]
    async fn graph_retry_snapshot_schedule_and_claim_release_are_atomic_and_fenced() {
        let database = TestDatabase::create().await;
        let repository = PostgresGraphTaskRepository::new(database.pool().clone());
        let raw_now = time::OffsetDateTime::now_utc();
        let now = raw_now
            .replace_nanosecond((raw_now.nanosecond() / 1_000) * 1_000)
            .unwrap();
        repository.insert_graph_task(task_at(now)).await.unwrap();
        let claim = repository
            .claim_next_graph_task("graph-worker", now, now + time::Duration::minutes(5))
            .await
            .unwrap()
            .unwrap();
        let mut next = claim.task.snapshot.clone();
        let GraphTaskSnapshot::V1(snapshot) = &mut next;
        snapshot
            .record_nexus_observation(
                0,
                GraphObservationOutcome::False,
                now,
                "https://nexus.example".to_owned(),
                Some(200),
                None,
                Some(now + time::Duration::seconds(5)),
                0,
            )
            .unwrap();
        let scheduled = repository
            .schedule_graph_task_retry(
                &claim.task.task_id,
                "graph-worker",
                &claim.claim_token,
                now + time::Duration::seconds(1),
                0,
                next.clone(),
                now + time::Duration::seconds(5),
            )
            .await
            .unwrap()
            .unwrap();
        assert_eq!(scheduled.evaluator_revision, 1);
        assert_eq!(scheduled.snapshot, next);
        assert_eq!(scheduled.status, VerificationTaskStatus::Pending);
        assert_eq!(
            repository
                .schedule_graph_task_retry(
                    &claim.task.task_id,
                    "graph-worker",
                    &claim.claim_token,
                    now + time::Duration::seconds(2),
                    0,
                    scheduled.snapshot.clone(),
                    now + time::Duration::seconds(6),
                )
                .await
                .unwrap(),
            None
        );
        let persisted_due: Option<time::OffsetDateTime> = sqlx::query_scalar(
            "SELECT next_attempt_at FROM verification_tasks WHERE task_id = $1::uuid",
        )
        .bind(claim.task.task_id.to_string())
        .fetch_one(database.pool())
        .await
        .unwrap();
        assert!(persisted_due.is_some());
        database.cleanup().await;
    }

    #[tokio::test]
    async fn postgres_final_graph_retry_is_clamped_to_deadline_after_database_time_advances() {
        let database = TestDatabase::create().await;
        let repository = PostgresGraphTaskRepository::new(database.pool().clone());
        let database_now: time::OffsetDateTime =
            sqlx::query_scalar("SELECT date_trunc('microseconds', clock_timestamp())")
                .fetch_one(database.pool())
                .await
                .unwrap();
        let accepted_at = database_now - time::Duration::seconds(59);
        repository
            .insert_graph_task(task_at(accepted_at))
            .await
            .unwrap();
        let claim = repository
            .claim_next_graph_task(
                "graph-worker",
                database_now,
                database_now + time::Duration::minutes(5),
            )
            .await
            .unwrap()
            .unwrap();
        let mut next = claim.task.snapshot.clone();
        let GraphTaskSnapshot::V1(snapshot) = &mut next;
        let deadline = snapshot.deadline_at;
        snapshot
            .record_nexus_observation(
                0,
                GraphObservationOutcome::False,
                database_now,
                "https://nexus.example".to_owned(),
                Some(200),
                None,
                Some(deadline),
                0,
            )
            .unwrap();

        let scheduled = repository
            .schedule_graph_task_retry(
                &claim.task.task_id,
                "graph-worker",
                &claim.claim_token,
                database_now - time::Duration::SECOND,
                claim.task.evaluator_revision,
                next,
                deadline,
            )
            .await
            .unwrap()
            .expect("final retry remains schedulable at deadline");
        assert_eq!(scheduled.status, VerificationTaskStatus::Pending);
        let persisted_due: time::OffsetDateTime = sqlx::query_scalar(
            "SELECT next_attempt_at FROM verification_tasks WHERE task_id = $1::uuid",
        )
        .bind(claim.task.task_id.to_string())
        .fetch_one(database.pool())
        .await
        .unwrap();
        assert_eq!(persisted_due, deadline);
        database.cleanup().await;
    }

    #[tokio::test]
    async fn claims_legacy_terminal_graph_snapshot_left_in_progress_by_prior_schema() {
        let database = TestDatabase::create().await;
        let repository = PostgresGraphTaskRepository::new(database.pool().clone());
        let task = task();
        repository.insert_graph_task(task.clone()).await.unwrap();
        let mut terminal = task.snapshot;
        let GraphTaskSnapshot::V1(snapshot) = &mut terminal;
        snapshot
            .record_observation(
                0,
                GraphObservationOutcome::True,
                NOW + time::Duration::seconds(1),
            )
            .unwrap();
        reduce_graph_snapshot(snapshot, NOW + time::Duration::seconds(1)).unwrap();
        sqlx::query(
            "UPDATE verification_tasks
             SET status = 'in_progress', started_at = $2,
                 evaluator_snapshot = $3, claimed_by = NULL,
                 claim_token = NULL, claim_expires_at = NULL
             WHERE task_id = $1::uuid",
        )
        .bind(task.task_id.to_string())
        .bind(NOW)
        .bind(terminal.to_json().unwrap())
        .execute(database.pool())
        .await
        .unwrap();

        let claimed = repository
            .claim_next_graph_task(
                "migration-recovery-worker",
                NOW + time::Duration::seconds(2),
                NOW + time::Duration::minutes(5),
            )
            .await
            .unwrap()
            .expect("legacy terminal snapshot must remain recoverable");
        assert_eq!(
            claimed.task.snapshot.as_v1().root,
            GraphRootState::Satisfied
        );
        assert_eq!(claimed.task.status, VerificationTaskStatus::InProgress);
        let recovered = repository
            .persist_claimed_graph_snapshot(
                &claimed.task.task_id,
                "migration-recovery-worker",
                &claimed.claim_token,
                NOW + time::Duration::seconds(3),
                claimed.task.evaluator_revision,
                claimed.task.snapshot,
            )
            .await
            .unwrap()
            .expect("legacy terminal snapshot must transition under the fresh claim");
        assert_eq!(
            recovered.status,
            VerificationTaskStatus::PublishingEntitlement
        );
        assert!(recovered.entitlement_to_publish.is_some());
        database.cleanup().await;
    }

    #[tokio::test]
    async fn graph_terminal_roots_persist_closed_status_reason_without_internal_message() {
        let database = TestDatabase::create().await;
        let repository = PostgresGraphTaskRepository::new(database.pool().clone());
        repository.insert_graph_task(task()).await.unwrap();
        let claim = repository
            .claim_next_graph_task("worker", NOW, NOW + time::Duration::minutes(5))
            .await
            .unwrap()
            .unwrap();
        let mut terminal = claim.task.snapshot.clone();
        let GraphTaskSnapshot::V1(snapshot) = &mut terminal;
        snapshot
            .record_observation(
                0,
                GraphObservationOutcome::DependencyUnavailable,
                snapshot.deadline_at,
            )
            .unwrap();
        reduce_graph_snapshot(snapshot, snapshot.deadline_at).unwrap();

        let persisted = repository
            .persist_claimed_graph_snapshot(
                &claim.task.task_id,
                "worker",
                &claim.claim_token,
                snapshot.deadline_at,
                0,
                terminal,
            )
            .await
            .unwrap()
            .expect("terminal graph state persists");
        assert_eq!(persisted.status, VerificationTaskStatus::Failed);
        let (status, reason, failure_message): (String, Option<String>, Option<String>) =
            sqlx::query_as(
                "SELECT status, terminal_reason, failure_message
                 FROM verification_tasks WHERE task_id = $1::uuid",
            )
            .bind(claim.task.task_id.to_string())
            .fetch_one(database.pool())
            .await
            .unwrap();
        assert_eq!(status, "failed");
        assert_eq!(
            reason.as_deref(),
            Some("verification_unavailable_by_deadline")
        );
        assert_eq!(failure_message, None);
        database.cleanup().await;
    }

    #[tokio::test]
    async fn satisfied_graph_publication_completion_is_token_revision_and_lease_fenced() {
        let database = TestDatabase::create().await;
        let repository = PostgresGraphTaskRepository::new(database.pool().clone());
        repository.insert_graph_task(task()).await.unwrap();
        let claim = repository
            .claim_next_graph_task("worker", NOW, NOW + time::Duration::minutes(5))
            .await
            .unwrap()
            .unwrap();
        let mut satisfied = claim.task.snapshot.clone();
        let GraphTaskSnapshot::V1(snapshot) = &mut satisfied;
        snapshot
            .record_observation(
                0,
                GraphObservationOutcome::True,
                NOW + time::Duration::seconds(1),
            )
            .unwrap();
        reduce_graph_snapshot(snapshot, NOW + time::Duration::seconds(1)).unwrap();
        let publishing = repository
            .persist_claimed_graph_snapshot(
                &claim.task.task_id,
                "worker",
                &claim.claim_token,
                NOW + time::Duration::seconds(1),
                0,
                satisfied,
            )
            .await
            .unwrap()
            .expect("satisfied graph enters publishing state");
        assert_eq!(
            publishing.status,
            VerificationTaskStatus::PublishingEntitlement
        );
        let publication_token = publishing
            .entitlement_publication_token
            .expect("publication token");
        assert!(publishing.entitlement_to_publish.is_some());

        assert!(
            repository
                .complete_claimed_graph_publication(
                    &claim.task.task_id,
                    "worker",
                    &claim.claim_token,
                    NOW + time::Duration::seconds(2),
                    1,
                    uuid::Uuid::new_v4(),
                )
                .await
                .unwrap()
                .is_none()
        );
        assert!(
            repository
                .complete_claimed_graph_publication(
                    &claim.task.task_id,
                    "worker",
                    &claim.claim_token,
                    NOW + time::Duration::seconds(2),
                    0,
                    publication_token,
                )
                .await
                .unwrap()
                .is_none()
        );
        let completed = repository
            .complete_claimed_graph_publication(
                &claim.task.task_id,
                "worker",
                &claim.claim_token,
                NOW + time::Duration::seconds(2),
                1,
                publication_token,
            )
            .await
            .unwrap()
            .expect("matching completion fence");
        assert_eq!(completed.status, VerificationTaskStatus::Completed);
        assert!(completed.entitlement_to_publish.is_some());
        assert!(completed.entitlement_publication_token.is_none());
        database.cleanup().await;
    }

    #[tokio::test]
    async fn mismatched_graph_entitlement_identity_fails_closed_on_read() {
        let database = TestDatabase::create().await;
        let repository = PostgresGraphTaskRepository::new(database.pool().clone());
        repository.insert_graph_task(task()).await.unwrap();
        let claim = repository
            .claim_next_graph_task("worker", NOW, NOW + time::Duration::minutes(5))
            .await
            .unwrap()
            .unwrap();
        let mut satisfied = claim.task.snapshot.clone();
        let GraphTaskSnapshot::V1(snapshot) = &mut satisfied;
        snapshot
            .record_observation(
                0,
                GraphObservationOutcome::True,
                NOW + time::Duration::seconds(1),
            )
            .unwrap();
        reduce_graph_snapshot(snapshot, NOW + time::Duration::seconds(1)).unwrap();
        repository
            .persist_claimed_graph_snapshot(
                &claim.task.task_id,
                "worker",
                &claim.claim_token,
                NOW + time::Duration::seconds(1),
                0,
                satisfied,
            )
            .await
            .unwrap()
            .unwrap();

        sqlx::query(
            "UPDATE verification_tasks
             SET entitlement_to_publish = jsonb_set(
                 entitlement_to_publish,
                 '{reader}',
                 to_jsonb($2::text)
             )
             WHERE task_id = $1::uuid",
        )
        .bind(claim.task.task_id.to_string())
        .bind(creator().to_string())
        .execute(database.pool())
        .await
        .unwrap();

        assert!(
            repository
                .get_graph_task_by_handle(&creator(), &bundle_id())
                .await
                .is_err()
        );
        database.cleanup().await;
    }

    #[tokio::test]
    async fn corrupt_snapshot_fails_closed_on_read() {
        let database = TestDatabase::create().await;
        let repository = PostgresGraphTaskRepository::new(database.pool().clone());
        let task = repository.insert_graph_task(task()).await.unwrap();
        sqlx::query(
            "UPDATE verification_tasks
             SET evaluator_snapshot = jsonb_set(evaluator_snapshot, '{schema_version}', '99')
             WHERE task_id = $1::uuid",
        )
        .bind(task.task_id.to_string())
        .execute(database.pool())
        .await
        .unwrap();
        assert!(
            repository
                .get_graph_task_by_handle(&creator(), &bundle_id())
                .await
                .is_err()
        );
        database.cleanup().await;
    }

    #[tokio::test]
    async fn stale_postgres_claim_fences_precede_candidate_validation() {
        let database = TestDatabase::create().await;
        let repository = PostgresGraphTaskRepository::new(database.pool().clone());
        repository.insert_graph_task(task()).await.unwrap();
        let claim = repository
            .claim_next_graph_task("worker", NOW, NOW + time::Duration::minutes(5))
            .await
            .unwrap()
            .unwrap();
        let mut invalid = claim.task.snapshot.clone();
        let GraphTaskSnapshot::V1(snapshot) = &mut invalid;
        snapshot.predicates[0].first_true_observation =
            Some(crate::application::models::GraphPredicateObservation {
                outcome: GraphObservationOutcome::True,
                observed_at: NOW,
                origin: None,
                http_status: None,
            });
        assert_eq!(
            repository
                .persist_claimed_graph_snapshot(
                    &claim.task.task_id,
                    "worker",
                    &uuid::Uuid::new_v4(),
                    NOW,
                    0,
                    invalid.clone(),
                )
                .await
                .unwrap(),
            None
        );
        assert_eq!(
            repository
                .schedule_graph_task_retry(
                    &claim.task.task_id,
                    "worker",
                    &uuid::Uuid::new_v4(),
                    NOW,
                    0,
                    invalid.clone(),
                    NOW,
                )
                .await
                .unwrap(),
            None
        );
        assert_eq!(
            repository
                .persist_claimed_graph_snapshot(
                    &claim.task.task_id,
                    "worker",
                    &claim.claim_token,
                    NOW,
                    1,
                    invalid.clone(),
                )
                .await
                .unwrap(),
            None
        );
        sqlx::query(
            "UPDATE verification_tasks SET claim_expires_at = clock_timestamp()
             WHERE task_id = $1::uuid",
        )
        .bind(claim.task.task_id.to_string())
        .execute(database.pool())
        .await
        .unwrap();
        assert_eq!(
            repository
                .persist_claimed_graph_snapshot(
                    &claim.task.task_id,
                    "worker",
                    &claim.claim_token,
                    NOW,
                    0,
                    invalid,
                )
                .await
                .unwrap(),
            None
        );
        database.cleanup().await;
    }

    #[tokio::test]
    async fn postgres_rejects_true_evidence_later_than_authoritative_database_time() {
        let database = TestDatabase::create().await;
        let repository = PostgresGraphTaskRepository::new(database.pool().clone());
        let database_now: time::OffsetDateTime =
            sqlx::query_scalar("SELECT date_trunc('microseconds', clock_timestamp())")
                .fetch_one(database.pool())
                .await
                .unwrap();
        let mut fresh = task();
        let GraphTaskSnapshot::V1(snapshot) = &mut fresh.snapshot;
        snapshot.accepted_at = database_now;
        snapshot.deadline_at = database_now + time::Duration::seconds(60);
        repository.insert_graph_task(fresh).await.unwrap();
        let claim = repository
            .claim_next_graph_task(
                "worker",
                database_now,
                database_now + time::Duration::minutes(5),
            )
            .await
            .unwrap()
            .unwrap();
        let mut future = claim.task.snapshot.clone();
        let GraphTaskSnapshot::V1(snapshot) = &mut future;
        let future_time = database_now + time::Duration::seconds(30);
        snapshot
            .record_observation(0, GraphObservationOutcome::True, future_time)
            .unwrap();
        reduce_graph_snapshot(snapshot, future_time).unwrap();
        assert!(
            repository
                .persist_claimed_graph_snapshot(
                    &claim.task.task_id,
                    "worker",
                    &claim.claim_token,
                    future_time,
                    0,
                    future,
                )
                .await
                .is_err()
        );
        database.cleanup().await;
    }

    #[tokio::test]
    async fn valid_snapshot_json_must_match_relational_identity_and_timing() {
        let database = TestDatabase::create().await;
        let repository = PostgresGraphTaskRepository::new(database.pool().clone());
        let inserted = repository.insert_graph_task(task()).await.unwrap();
        let replacement_resource = format!(
            "{READER}/pub/app.locks/{}.json",
            LockId::from_hash(LockHash::from_bytes([0; 32]))
        );
        sqlx::query(
            "UPDATE verification_tasks
             SET evaluator_snapshot = jsonb_set(
                    jsonb_set(evaluator_snapshot, '{creator}', to_jsonb($2::text)),
                    '{pubky_lock_resource}', to_jsonb($3::text)
                 )
             WHERE task_id = $1::uuid",
        )
        .bind(inserted.task_id.to_string())
        .bind(READER)
        .bind(replacement_resource)
        .execute(database.pool())
        .await
        .unwrap();
        assert!(
            repository
                .get_graph_task_by_handle(&creator(), &bundle_id())
                .await
                .is_err()
        );

        let original = task().snapshot.to_json().unwrap();
        sqlx::query(
            "UPDATE verification_tasks
             SET evaluator_snapshot = jsonb_set(
                    jsonb_set($2, '{accepted_at}', to_jsonb($3::text)),
                    '{deadline_at}', to_jsonb($4::text)
                 )
             WHERE task_id = $1::uuid",
        )
        .bind(inserted.task_id.to_string())
        .bind(original)
        .bind((NOW + time::Duration::seconds(1)).to_string())
        .bind((NOW + time::Duration::seconds(61)).to_string())
        .execute(database.pool())
        .await
        .unwrap();
        assert!(
            repository
                .get_graph_task_by_handle(&creator(), &bundle_id())
                .await
                .is_err()
        );
        database.cleanup().await;
    }

    #[tokio::test]
    async fn concurrent_changed_binding_reports_conflict_for_loser() {
        let database = TestDatabase::create().await;
        let repository = PostgresGraphTaskRepository::new(database.pool().clone());
        let first = task();
        let mut changed = task();
        changed.task_id = TaskId::from_str("018fc6ec-2f3d-4f7e-8b7d-6f5c4b3a2d11").unwrap();
        let GraphTaskSnapshot::V1(snapshot) = &mut changed.snapshot;
        snapshot.lock_hash = [1; 32];
        let changed_lock_id = LockId::from_hash(LockHash::from_bytes([1; 32]));
        snapshot.pubky_lock_resource =
            PubkyLockResource::from_str(&format!("{CREATOR}/pub/app.locks/{changed_lock_id}.json"))
                .unwrap();

        let barrier = std::sync::Arc::new(tokio::sync::Barrier::new(2));
        let left = {
            let repository = repository.clone();
            let barrier = std::sync::Arc::clone(&barrier);
            tokio::spawn(async move {
                barrier.wait().await;
                repository.insert_graph_task(first).await
            })
        };
        let right = {
            let repository = repository.clone();
            tokio::spawn(async move {
                barrier.wait().await;
                repository.insert_graph_task(changed).await
            })
        };
        let (left, right) = tokio::join!(left, right);
        let results = [left.unwrap(), right.unwrap()];
        assert_eq!(results.iter().filter(|result| result.is_ok()).count(), 1);
        assert!(results.iter().any(|result| matches!(
            result,
            Err(crate::application::errors::ApplicationError::VerificationTaskConflict)
        )));
        database.cleanup().await;
    }

    fn task() -> GraphTaskRecord {
        task_at(NOW)
    }

    fn task_at(now: time::OffsetDateTime) -> GraphTaskRecord {
        GraphTaskRecord {
            task_id: TaskId::from_str(TASK_ID).unwrap(),
            snapshot: GraphTaskSnapshot::V1(
                GraphTaskSnapshotV1::new(
                    creator(),
                    bundle_id(),
                    resource(),
                    [0; 32],
                    CreatorPubky::from_str(READER).unwrap(),
                    now,
                    60,
                    GraphPolicyV2 {
                        predicates: vec![GraphPredicateV2::Follows {
                            follower: GraphParticipantV2::Reader,
                            followed: GraphParticipantV2::Creator,
                        }],
                        operator: None,
                    },
                )
                .unwrap(),
            ),
            evaluator_revision: 0,
            status: VerificationTaskStatus::Pending,
            entitlement_to_publish: None,
            entitlement_publication_token: None,
            completed_at: None,
        }
    }

    fn creator() -> CreatorPubky {
        CreatorPubky::from_str(CREATOR).unwrap()
    }

    fn bundle_id() -> BundleId {
        BundleId::from_str(BUNDLE_ID).unwrap()
    }

    fn resource() -> PubkyLockResource {
        let lock_id = LockId::from_hash(LockHash::from_bytes([0; 32]));
        PubkyLockResource::from_str(&format!("{CREATOR}/pub/app.locks/{lock_id}.json")).unwrap()
    }
}
