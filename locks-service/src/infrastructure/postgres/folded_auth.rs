use std::str::FromStr;

use async_trait::async_trait;
use locks_core::ids::{BundleId, CreatorPubky, PubkyLockResource, TaskId};
use locks_core::lock_policy::GraphPolicyV2;
use locks_core::verification::SubmittedProofBundleV2;
use sqlx::{FromRow, PgPool, Postgres, Transaction};
use time::OffsetDateTime;

use crate::application::errors::ApplicationError;
use crate::application::models::{
    EncryptedFoldedAuthFlowSecret, FOLDED_AUTH_POLL_ABANDONED_AFTER, FOLDED_AUTH_TOMBSTONE_TTL,
    FOLDED_AUTH_TTL, FoldedAuthAttempt, FoldedAuthSecretContext, FoldedAuthStartDisposition,
    FoldedAuthStartResult, FoldedAuthStateHash, FoldedAuthStatus, GraphTaskRecord,
    RestoredFoldedAuthAttempt, StartFoldedAuthAttempt,
};
use crate::application::ports::{FoldedAuthRepository, GraphTaskRepository};
use crate::infrastructure::folded_auth::FoldedAuthSecretCipher;

use super::proof_admission::{lock_proof_admission, lock_proof_handle_admission};
use super::verification_tasks::reject_deletion_cutoff;

#[derive(Debug, Clone)]
pub struct PostgresFoldedAuthRepository {
    pool: PgPool,
    cipher: FoldedAuthSecretCipher,
}

impl PostgresFoldedAuthRepository {
    pub fn new(pool: PgPool, cipher: FoldedAuthSecretCipher) -> Self {
        Self { pool, cipher }
    }
}

#[derive(Debug, FromRow)]
struct FoldedAuthRow {
    creator: String,
    bundle_id: String,
    generation: i64,
    state_hash: Vec<u8>,
    status: String,
    submitted_proof_bundle: serde_json::Value,
    pubky_lock_resource: String,
    lock_hash: Vec<u8>,
    verification_window_seconds: i64,
    graph_policy: serde_json::Value,
    encrypted_flow_secret: Option<String>,
    created_at: OffsetDateTime,
    expires_at: OffsetDateTime,
    poll_dispatched_at: Option<OffsetDateTime>,
    terminal_at: Option<OffsetDateTime>,
    purge_after: Option<OffsetDateTime>,
    task_id: Option<String>,
}

const COLUMNS: &str = "creator, bundle_id, generation, state_hash, status,
    submitted_proof_bundle, pubky_lock_resource, lock_hash, verification_window_seconds,
    graph_policy, encrypted_flow_secret, created_at, expires_at, poll_dispatched_at,
    terminal_at, purge_after, task_id::text AS task_id";

#[async_trait]
impl FoldedAuthRepository for PostgresFoldedAuthRepository {
    async fn get_latest_by_handle(
        &self,
        creator: &CreatorPubky,
        bundle_id: &BundleId,
    ) -> Result<Option<FoldedAuthAttempt>, ApplicationError> {
        sqlx::query_as::<_, FoldedAuthRow>(&format!(
            "SELECT {COLUMNS} FROM folded_auth_attempts
             WHERE creator = $1 AND bundle_id = $2
             ORDER BY generation DESC LIMIT 1"
        ))
        .bind(creator.to_string())
        .bind(bundle_id.to_string())
        .fetch_optional(&self.pool)
        .await
        .map_err(storage_error)?
        .map(row_to_stored)
        .transpose()
        .map(|stored| stored.map(|stored| stored.0))
    }

    async fn accepted_graph_task(
        &self,
        state_hash: &FoldedAuthStateHash,
    ) -> Result<Option<GraphTaskRecord>, ApplicationError> {
        let Some(attempt) = self.get_by_state_hash(state_hash).await? else {
            return Ok(None);
        };
        if attempt.status != FoldedAuthStatus::Accepted {
            return Ok(None);
        }
        super::graph_tasks::PostgresGraphTaskRepository::new(self.pool.clone())
            .get_graph_task_by_handle(&attempt.creator, &attempt.bundle_id)
            .await
    }

    async fn start_or_rotate(
        &self,
        request: StartFoldedAuthAttempt,
    ) -> Result<FoldedAuthStartResult, ApplicationError> {
        request.validate().map_err(invalid_state)?;
        let mut transaction = self.pool.begin().await.map_err(storage_error)?;
        // Match acceptance's row-before-admission lock order. The second read below
        // closes the no-row race after admission fences serialize concurrent starts.
        sqlx::query(
            "SELECT state_hash FROM folded_auth_attempts
             WHERE creator = $1 AND bundle_id = $2
             ORDER BY generation DESC LIMIT 1 FOR UPDATE",
        )
        .bind(request.creator().to_string())
        .bind(request.bundle_id().to_string())
        .fetch_optional(&mut *transaction)
        .await
        .map_err(storage_error)?;
        lock_proof_admission(
            &mut transaction,
            request.creator(),
            request.pubky_lock_resource.lock_id(),
        )
        .await?;
        lock_proof_handle_admission(&mut transaction, request.creator(), request.bundle_id())
            .await?;
        let latest = sqlx::query_as::<_, FoldedAuthRow>(&format!(
            "SELECT {COLUMNS} FROM folded_auth_attempts
             WHERE creator = $1 AND bundle_id = $2
             ORDER BY generation DESC LIMIT 1 FOR UPDATE"
        ))
        .bind(request.creator().to_string())
        .bind(request.bundle_id().to_string())
        .fetch_optional(&mut *transaction)
        .await
        .map_err(storage_error)?
        .map(row_to_stored)
        .transpose()?;
        let existing_task: bool = sqlx::query_scalar(
            "SELECT EXISTS(
                SELECT 1 FROM verification_tasks WHERE creator = $1 AND bundle_id = $2
             )",
        )
        .bind(request.creator().to_string())
        .bind(request.bundle_id().to_string())
        .fetch_one(&mut *transaction)
        .await
        .map_err(storage_error)?;
        if existing_task {
            return Err(ApplicationError::VerificationTaskConflict);
        }
        let database_now: OffsetDateTime = sqlx::query_scalar("SELECT clock_timestamp()")
            .fetch_one(&mut *transaction)
            .await
            .map_err(storage_error)?;
        let expires_at = database_now
            .checked_add(FOLDED_AUTH_TTL)
            .ok_or_else(|| storage_message("folded auth expiry overflow"))?;

        let mut disposition = FoldedAuthStartDisposition::Started;
        let (generation, previous_terminal) = if let Some((latest, _)) = &latest {
            if !latest.exact_request_matches(&request)
                || latest.status == FoldedAuthStatus::Accepted
            {
                return Err(ApplicationError::VerificationTaskConflict);
            }
            let generation = latest
                .generation
                .checked_add(1)
                .ok_or_else(|| storage_message("folded auth generation overflow"))?;
            let previous = if latest.status == FoldedAuthStatus::Pending {
                let status = if latest.expires_at <= database_now {
                    FoldedAuthStatus::Expired
                } else {
                    disposition = FoldedAuthStartDisposition::Rotated;
                    FoldedAuthStatus::RestartRequired
                };
                Some((status, checked_purge_after(database_now)?))
            } else {
                None
            };
            (generation, previous)
        } else {
            (1, None)
        };
        let context = FoldedAuthSecretContext {
            creator: request.creator().clone(),
            bundle_id: request.bundle_id().clone(),
            generation,
            pubky_lock_resource: request.pubky_lock_resource.clone(),
            lock_hash: request.lock_hash,
        };
        let encrypted = self.cipher.encrypt(&context, &request.flow_secret)?;
        let generation_i64 = i64::try_from(generation)
            .map_err(|_| storage_message("folded auth generation exceeds Postgres BIGINT"))?;
        let bundle_json = serde_json::to_value(&request.submitted_proof_bundle)
            .map_err(|_| storage_message("failed to serialize folded auth bundle"))?;
        let graph_json = serde_json::to_value(&request.graph)
            .map_err(|_| storage_message("failed to serialize folded auth graph"))?;
        if let (Some((latest, _)), Some((status, purge_after))) = (&latest, previous_terminal) {
            let updated = sqlx::query(
                "UPDATE folded_auth_attempts
                 SET status = $2, encrypted_flow_secret = NULL,
                     poll_dispatched_at = NULL, terminal_at = $3, purge_after = $4
                 WHERE state_hash = $1 AND status = 'pending'",
            )
            .bind(latest.state_hash.as_bytes().as_slice())
            .bind(status.as_str())
            .bind(database_now)
            .bind(purge_after)
            .execute(&mut *transaction)
            .await
            .map_err(storage_error)?;
            if updated.rows_affected() != 1 {
                return Err(ApplicationError::VerificationTaskConflict);
            }
        }
        let row = sqlx::query_as::<_, FoldedAuthRow>(&format!(
            "INSERT INTO folded_auth_attempts (
                creator, bundle_id, generation, state_hash, status,
                submitted_proof_bundle, pubky_lock_resource, lock_hash,
                verification_window_seconds, graph_policy, encrypted_flow_secret,
                created_at, expires_at
             ) VALUES ($1, $2, $3, $4, 'pending', $5, $6, $7, $8, $9, $10, $11, $12)
             RETURNING {COLUMNS}"
        ))
        .bind(request.creator().to_string())
        .bind(request.bundle_id().to_string())
        .bind(generation_i64)
        .bind(request.state_hash.as_bytes().as_slice())
        .bind(bundle_json)
        .bind(request.pubky_lock_resource.to_string())
        .bind(request.lock_hash.as_slice())
        .bind(i64::try_from(request.verification_window_seconds).unwrap_or(i64::MAX))
        .bind(graph_json)
        .bind(encrypted.as_str())
        .bind(database_now)
        .bind(expires_at)
        .fetch_one(&mut *transaction)
        .await
        .map_err(storage_error)?;
        transaction.commit().await.map_err(storage_error)?;
        Ok(FoldedAuthStartResult {
            attempt: row_to_stored(row)?.0,
            disposition,
        })
    }

    async fn get_by_state_hash(
        &self,
        state_hash: &FoldedAuthStateHash,
    ) -> Result<Option<FoldedAuthAttempt>, ApplicationError> {
        Ok(load_row(&self.pool, state_hash)
            .await?
            .map(row_to_stored)
            .transpose()?
            .map(|(attempt, _)| attempt))
    }

    async fn restore_pending(
        &self,
        state_hash: &FoldedAuthStateHash,
    ) -> Result<Option<RestoredFoldedAuthAttempt>, ApplicationError> {
        let Some((attempt, encrypted)) = load_row(&self.pool, state_hash)
            .await?
            .map(row_to_stored)
            .transpose()?
        else {
            return Ok(None);
        };
        if attempt.status != FoldedAuthStatus::Pending || attempt.poll_dispatched {
            return Ok(None);
        }
        let encrypted = encrypted
            .ok_or_else(|| invalid_state("pending folded auth attempt lacks flow secret"))?;
        let flow_secret = self.cipher.decrypt(&attempt.context(), &encrypted)?;
        Ok(Some(RestoredFoldedAuthAttempt {
            attempt,
            flow_secret,
        }))
    }

    async fn mark_poll_dispatched(
        &self,
        state_hash: &FoldedAuthStateHash,
        expected_generation: u64,
        _now: OffsetDateTime,
    ) -> Result<Option<FoldedAuthAttempt>, ApplicationError> {
        set_poll_dispatched(&self.pool, state_hash, expected_generation, true).await
    }

    async fn confirm_not_attempted(
        &self,
        state_hash: &FoldedAuthStateHash,
        expected_generation: u64,
        _now: OffsetDateTime,
    ) -> Result<Option<FoldedAuthAttempt>, ApplicationError> {
        set_poll_dispatched(&self.pool, state_hash, expected_generation, false).await
    }

    async fn confirm_pending_generation(
        &self,
        state_hash: &FoldedAuthStateHash,
        expected_generation: u64,
        _now: OffsetDateTime,
    ) -> Result<Option<FoldedAuthAttempt>, ApplicationError> {
        transition_after_poll(&self.pool, state_hash, expected_generation, None).await
    }

    async fn require_restart(
        &self,
        state_hash: &FoldedAuthStateHash,
        expected_generation: u64,
        _now: OffsetDateTime,
    ) -> Result<Option<FoldedAuthAttempt>, ApplicationError> {
        transition_after_poll(
            &self.pool,
            state_hash,
            expected_generation,
            Some(FoldedAuthStatus::RestartRequired),
        )
        .await
    }

    async fn expire(
        &self,
        state_hash: &FoldedAuthStateHash,
        expected_generation: u64,
        _now: OffsetDateTime,
    ) -> Result<Option<FoldedAuthAttempt>, ApplicationError> {
        transition_after_poll(
            &self.pool,
            state_hash,
            expected_generation,
            Some(FoldedAuthStatus::Expired),
        )
        .await
    }

    async fn accept_and_insert_graph_task(
        &self,
        state_hash: &FoldedAuthStateHash,
        expected_generation: u64,
        task_id: TaskId,
        reader: CreatorPubky,
        _now: OffsetDateTime,
    ) -> Result<Option<GraphTaskRecord>, ApplicationError> {
        let generation = match i64::try_from(expected_generation) {
            Ok(value) => value,
            Err(_) => return Ok(None),
        };
        let mut transaction = self.pool.begin().await.map_err(storage_error)?;
        let row = sqlx::query_as::<_, FoldedAuthRow>(&format!(
            "SELECT {COLUMNS} FROM folded_auth_attempts
             WHERE state_hash = $1 AND generation = $2 AND status = 'pending'
             FOR UPDATE"
        ))
        .bind(state_hash.as_bytes().as_slice())
        .bind(generation)
        .fetch_optional(&mut *transaction)
        .await
        .map_err(storage_error)?;
        let Some((attempt, _)) = row.map(row_to_stored).transpose()? else {
            transaction.rollback().await.map_err(storage_error)?;
            return Ok(None);
        };
        lock_proof_admission(
            &mut transaction,
            &attempt.creator,
            attempt.pubky_lock_resource.lock_id(),
        )
        .await?;
        lock_proof_handle_admission(&mut transaction, &attempt.creator, &attempt.bundle_id).await?;
        let database_now: OffsetDateTime = sqlx::query_scalar("SELECT clock_timestamp()")
            .fetch_one(&mut *transaction)
            .await
            .map_err(storage_error)?;
        if attempt.expires_at <= database_now {
            terminalize_pending_tx(
                &mut transaction,
                state_hash,
                generation,
                FoldedAuthStatus::Expired,
                database_now,
            )
            .await?;
            transaction.commit().await.map_err(storage_error)?;
            return Ok(None);
        }
        let task = attempt
            .accepted_graph_task(task_id, reader, database_now)
            .map_err(invalid_state)?;
        insert_graph_task_tx(&mut transaction, &task).await?;
        let purge_after = checked_purge_after(database_now)?;
        let updated = sqlx::query(
            "UPDATE folded_auth_attempts
             SET status = 'accepted', encrypted_flow_secret = NULL,
                 poll_dispatched_at = NULL, terminal_at = $3,
                 purge_after = $4, task_id = $5::uuid
             WHERE state_hash = $1 AND generation = $2 AND status = 'pending'",
        )
        .bind(state_hash.as_bytes().as_slice())
        .bind(generation)
        .bind(database_now)
        .bind(purge_after)
        .bind(task.task_id.to_string())
        .execute(&mut *transaction)
        .await
        .map_err(storage_error)?;
        if updated.rows_affected() != 1 {
            transaction.rollback().await.map_err(storage_error)?;
            return Ok(None);
        }
        transaction.commit().await.map_err(storage_error)?;
        Ok(Some(task))
    }

    async fn purge_terminal_before(&self, _now: OffsetDateTime) -> Result<u64, ApplicationError> {
        let mut transaction = self.pool.begin().await.map_err(storage_error)?;
        let database_now: OffsetDateTime = sqlx::query_scalar("SELECT clock_timestamp()")
            .fetch_one(&mut *transaction)
            .await
            .map_err(storage_error)?;
        let purge_after = checked_purge_after(database_now)?;
        let abandoned_before = database_now - FOLDED_AUTH_POLL_ABANDONED_AFTER;
        sqlx::query(
            "UPDATE folded_auth_attempts
             SET status = 'expired', encrypted_flow_secret = NULL,
                 poll_dispatched_at = NULL, terminal_at = $1, purge_after = $2
             WHERE status = 'pending' AND expires_at <= $1",
        )
        .bind(database_now)
        .bind(purge_after)
        .execute(&mut *transaction)
        .await
        .map_err(storage_error)?;
        sqlx::query(
            "UPDATE folded_auth_attempts
             SET status = 'restart_required', encrypted_flow_secret = NULL,
                 poll_dispatched_at = NULL, terminal_at = $1, purge_after = $2
             WHERE status = 'pending' AND poll_dispatched_at <= $3",
        )
        .bind(database_now)
        .bind(purge_after)
        .bind(abandoned_before)
        .execute(&mut *transaction)
        .await
        .map_err(storage_error)?;
        let deleted = sqlx::query(
            "DELETE FROM folded_auth_attempts
             WHERE status IN ('accepted', 'expired', 'restart_required') AND purge_after <= $1",
        )
        .bind(database_now)
        .execute(&mut *transaction)
        .await
        .map_err(storage_error)?
        .rows_affected();
        transaction.commit().await.map_err(storage_error)?;
        Ok(deleted)
    }
}

async fn set_poll_dispatched(
    pool: &PgPool,
    state_hash: &FoldedAuthStateHash,
    expected_generation: u64,
    dispatched: bool,
) -> Result<Option<FoldedAuthAttempt>, ApplicationError> {
    let generation = match i64::try_from(expected_generation) {
        Ok(value) => value,
        Err(_) => return Ok(None),
    };
    let predicate = if dispatched {
        "poll_dispatched_at IS NULL"
    } else {
        "poll_dispatched_at IS NOT NULL"
    };
    let assignment = if dispatched {
        "clock_timestamp()"
    } else {
        "NULL"
    };
    let row = sqlx::query_as::<_, FoldedAuthRow>(&format!(
        "UPDATE folded_auth_attempts
         SET poll_dispatched_at = {assignment}
         WHERE state_hash = $1 AND generation = $2 AND status = 'pending'
           AND {predicate}
         RETURNING {COLUMNS}"
    ))
    .bind(state_hash.as_bytes().as_slice())
    .bind(generation)
    .fetch_optional(pool)
    .await
    .map_err(storage_error)?;
    row.map(row_to_stored)
        .transpose()
        .map(|stored| stored.map(|stored| stored.0))
}

async fn transition_after_poll(
    pool: &PgPool,
    state_hash: &FoldedAuthStateHash,
    expected_generation: u64,
    requested_terminal: Option<FoldedAuthStatus>,
) -> Result<Option<FoldedAuthAttempt>, ApplicationError> {
    let generation = match i64::try_from(expected_generation) {
        Ok(value) => value,
        Err(_) => return Ok(None),
    };
    let mut transaction = pool.begin().await.map_err(storage_error)?;
    let row = sqlx::query_as::<_, FoldedAuthRow>(&format!(
        "SELECT {COLUMNS} FROM folded_auth_attempts
         WHERE state_hash = $1 AND generation = $2 AND status = 'pending'
         FOR UPDATE"
    ))
    .bind(state_hash.as_bytes().as_slice())
    .bind(generation)
    .fetch_optional(&mut *transaction)
    .await
    .map_err(storage_error)?;
    let Some((mut attempt, _)) = row.map(row_to_stored).transpose()? else {
        transaction.rollback().await.map_err(storage_error)?;
        return Ok(None);
    };
    let database_now: OffsetDateTime = sqlx::query_scalar("SELECT clock_timestamp()")
        .fetch_one(&mut *transaction)
        .await
        .map_err(storage_error)?;
    let terminal = if attempt.expires_at <= database_now {
        Some(FoldedAuthStatus::Expired)
    } else {
        requested_terminal.filter(|status| *status != FoldedAuthStatus::Expired)
    };
    if let Some(status) = terminal {
        terminalize_pending_tx(
            &mut transaction,
            state_hash,
            generation,
            status,
            database_now,
        )
        .await?;
        attempt.status = status;
        attempt.poll_dispatched = false;
        attempt.terminal_at = Some(database_now);
        attempt.purge_after = Some(checked_purge_after(database_now)?);
    } else {
        sqlx::query(
            "UPDATE folded_auth_attempts SET poll_dispatched_at = NULL
             WHERE state_hash = $1 AND generation = $2 AND status = 'pending'",
        )
        .bind(state_hash.as_bytes().as_slice())
        .bind(generation)
        .execute(&mut *transaction)
        .await
        .map_err(storage_error)?;
        attempt.poll_dispatched = false;
    }
    transaction.commit().await.map_err(storage_error)?;
    Ok(Some(attempt))
}

async fn terminalize_pending_tx(
    transaction: &mut Transaction<'_, Postgres>,
    state_hash: &FoldedAuthStateHash,
    generation: i64,
    status: FoldedAuthStatus,
    now: OffsetDateTime,
) -> Result<(), ApplicationError> {
    let purge_after = checked_purge_after(now)?;
    let updated = sqlx::query(
        "UPDATE folded_auth_attempts
         SET status = $3, encrypted_flow_secret = NULL,
             poll_dispatched_at = NULL, terminal_at = $4, purge_after = $5
         WHERE state_hash = $1 AND generation = $2 AND status = 'pending'",
    )
    .bind(state_hash.as_bytes().as_slice())
    .bind(generation)
    .bind(status.as_str())
    .bind(now)
    .bind(purge_after)
    .execute(&mut **transaction)
    .await
    .map_err(storage_error)?;
    if updated.rows_affected() != 1 {
        return Err(ApplicationError::VerificationTaskConflict);
    }
    Ok(())
}

async fn insert_graph_task_tx(
    transaction: &mut Transaction<'_, Postgres>,
    task: &GraphTaskRecord,
) -> Result<(), ApplicationError> {
    let snapshot = task.snapshot.as_v1();
    let snapshot_json = task
        .snapshot
        .to_json()
        .map_err(|_| storage_message("failed to serialize graph task snapshot"))?;
    let submitted = serde_json::json!({
        "version": 2,
        "bundle_id": snapshot.bundle_id,
        "pubky_lock_resource": snapshot.pubky_lock_resource,
    });
    lock_proof_admission(
        transaction,
        &snapshot.creator,
        snapshot.pubky_lock_resource.lock_id(),
    )
    .await?;
    reject_deletion_cutoff(
        transaction,
        &snapshot.creator,
        snapshot.pubky_lock_resource.lock_id(),
    )
    .await?;
    let inserted = sqlx::query(
        "INSERT INTO verification_tasks (
            task_id, creator, bundle_id, status, submitted_proof_bundle,
            submitted_at, invoice_admission_phase, evaluator_snapshot, evaluator_revision
         ) VALUES ($1::uuid, $2, $3, 'pending', $4, $5, 'ready', $6, 0)
         ON CONFLICT DO NOTHING",
    )
    .bind(task.task_id.to_string())
    .bind(snapshot.creator.to_string())
    .bind(snapshot.bundle_id.to_string())
    .bind(submitted)
    .bind(snapshot.accepted_at)
    .bind(snapshot_json)
    .execute(&mut **transaction)
    .await
    .map_err(storage_error)?;
    if inserted.rows_affected() != 1 {
        return Err(ApplicationError::VerificationTaskConflict);
    }
    Ok(())
}

async fn load_row(
    pool: &PgPool,
    state_hash: &FoldedAuthStateHash,
) -> Result<Option<FoldedAuthRow>, ApplicationError> {
    sqlx::query_as::<_, FoldedAuthRow>(&format!(
        "SELECT {COLUMNS} FROM folded_auth_attempts WHERE state_hash = $1"
    ))
    .bind(state_hash.as_bytes().as_slice())
    .fetch_optional(pool)
    .await
    .map_err(storage_error)
}

fn row_to_stored(
    row: FoldedAuthRow,
) -> Result<(FoldedAuthAttempt, Option<EncryptedFoldedAuthFlowSecret>), ApplicationError> {
    let state_hash: [u8; 32] = row
        .state_hash
        .try_into()
        .map_err(|_| invalid_state("invalid folded auth state hash length"))?;
    let lock_hash: [u8; 32] = row
        .lock_hash
        .try_into()
        .map_err(|_| invalid_state("invalid folded auth lock hash length"))?;
    let creator = CreatorPubky::from_str(&row.creator)
        .map_err(|_| invalid_state("invalid folded auth creator"))?;
    let bundle_id = BundleId::from_str(&row.bundle_id)
        .map_err(|_| invalid_state("invalid folded auth bundle id"))?;
    let generation = u64::try_from(row.generation)
        .map_err(|_| invalid_state("invalid folded auth generation"))?;
    let submitted_proof_bundle: SubmittedProofBundleV2 =
        serde_json::from_value(row.submitted_proof_bundle)
            .map_err(|_| invalid_state("invalid folded auth submitted bundle"))?;
    let pubky_lock_resource = PubkyLockResource::from_str(&row.pubky_lock_resource)
        .map_err(|_| invalid_state("invalid folded auth lock resource"))?;
    let verification_window_seconds = u64::try_from(row.verification_window_seconds)
        .map_err(|_| invalid_state("invalid folded auth verification window"))?;
    let graph: GraphPolicyV2 = serde_json::from_value(row.graph_policy)
        .map_err(|_| invalid_state("invalid folded auth graph policy"))?;
    let status = FoldedAuthStatus::from_storage_value(&row.status)
        .ok_or_else(|| invalid_state("invalid folded auth status"))?;
    let task_id = row
        .task_id
        .map(|value| TaskId::from_str(&value))
        .transpose()
        .map_err(|_| invalid_state("invalid folded auth task id"))?;
    let attempt = FoldedAuthAttempt {
        creator,
        bundle_id,
        generation,
        state_hash: FoldedAuthStateHash::from_bytes(state_hash),
        status,
        submitted_proof_bundle,
        pubky_lock_resource,
        lock_hash,
        verification_window_seconds,
        graph,
        created_at: row.created_at,
        expires_at: row.expires_at,
        poll_dispatched: row.poll_dispatched_at.is_some(),
        terminal_at: row.terminal_at,
        purge_after: row.purge_after,
        task_id,
    };
    validate_stored(&attempt, row.encrypted_flow_secret.is_some())?;
    Ok((
        attempt,
        row.encrypted_flow_secret
            .map(EncryptedFoldedAuthFlowSecret::new),
    ))
}

fn validate_stored(attempt: &FoldedAuthAttempt, has_secret: bool) -> Result<(), ApplicationError> {
    let expected_expiry = attempt
        .created_at
        .checked_add(crate::application::models::FOLDED_AUTH_TTL)
        .ok_or_else(|| invalid_state("folded auth expiry overflow"))?;
    if expected_expiry != attempt.expires_at
        || attempt.pubky_lock_resource.creator() != &attempt.creator
        || attempt.submitted_proof_bundle.bundle_id != attempt.bundle_id
        || attempt.submitted_proof_bundle.pubky_lock_resource != attempt.pubky_lock_resource
        || attempt.submitted_proof_bundle.reader_pubky.is_some()
    {
        return Err(invalid_state("folded auth durable binding is invalid"));
    }
    let pending = attempt.status == FoldedAuthStatus::Pending;
    if pending != has_secret
        || pending != attempt.terminal_at.is_none()
        || pending != attempt.purge_after.is_none()
        || (!pending && attempt.poll_dispatched)
        || (attempt.status == FoldedAuthStatus::Accepted) != attempt.task_id.is_some()
    {
        return Err(invalid_state("folded auth durable lifecycle is invalid"));
    }
    Ok(())
}

fn checked_purge_after(now: OffsetDateTime) -> Result<OffsetDateTime, ApplicationError> {
    now.checked_add(FOLDED_AUTH_TOMBSTONE_TTL)
        .ok_or_else(|| storage_message("folded auth tombstone expiry overflow"))
}

fn invalid_state(message: impl Into<String>) -> ApplicationError {
    ApplicationError::InvalidFoldedAuthState {
        message: message.into(),
    }
}

fn storage_message(message: &'static str) -> ApplicationError {
    ApplicationError::Storage {
        message: message.to_owned(),
    }
}

fn storage_error(error: sqlx::Error) -> ApplicationError {
    ApplicationError::Storage {
        message: error.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::str::FromStr;
    use std::sync::Arc;

    use async_trait::async_trait;
    use locks_core::ids::{
        BundleId, ContentLockPath, CreatorPubky, GuardedResourceHash, PubkyLockResource, TaskId,
    };
    use locks_core::lock_policy::{
        AccessPolicy, ContentLockV2, GraphParticipantV2, GraphPolicyV2, GraphPredicateV2,
        GuardedResource, LockServerConfig, VerifierType,
    };
    use locks_core::verification::{
        Proof, SUBMITTED_PROOF_BUNDLE_VERSION, SubmittedProofBundle, SubmittedProofBundleV2,
    };
    use serde_json::json;
    use time::macros::datetime;
    use tokio::sync::Notify;

    use super::PostgresFoldedAuthRepository;
    use crate::application::models::{
        FoldedAuthFlowSecret, FoldedAuthStateHash, FoldedAuthStatus, GraphTaskRecord,
        GraphTaskSnapshot, GraphTaskSnapshotV1, StartFoldedAuthAttempt, VerificationTaskRecord,
        VerificationTaskStatus,
    };
    use crate::application::ports::{
        Clock, FoldedAuthApprovalPoll, FoldedAuthApprovalPollError, FoldedAuthApprovalPoller,
        FoldedAuthPollCall, FoldedAuthRepository, GraphTaskRepository, VerificationTaskRepository,
    };
    use crate::application::use_cases::poll_folded_auth::{
        PollFoldedAuthAttempt, PollFoldedAuthOutcome,
    };
    use crate::infrastructure::folded_auth::FoldedAuthSecretCipher;
    use crate::infrastructure::postgres::graph_tasks::PostgresGraphTaskRepository;
    use crate::infrastructure::postgres::testing::TestDatabase;
    use crate::infrastructure::postgres::verification_tasks::PostgresVerificationTaskRepository;
    use crate::infrastructure::runtime_master_key::RuntimeMasterKey;

    const CREATOR: &str = "pubkytkrq8zmwb8a3m9k15csu3q17qmfgqnp9dskbrg9uq1rydpyxp7qy";
    const READER: &str = "pubkyorhzqdiexwmi6iidktucgud63ufa5nwtsuzdxe176a8izd6jsqky";
    const BUNDLE_ID: &str = "000G40R40M30E209185GR38E1W";
    const TASK_ID: &str = "550e8400-e29b-41d4-a716-446655440000";

    #[tokio::test]
    async fn encrypted_pending_flow_restores_after_process_reconstruction_and_key_loss_fails_closed()
     {
        let database = TestDatabase::create().await;
        let repository = PostgresFoldedAuthRepository::new(
            database.pool().clone(),
            cipher("AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"),
        );
        let lock = graph_lock();
        let secret = b"relay-secret-pending-flow";
        let started = repository
            .start_or_rotate(start_request(
                &lock,
                FoldedAuthStateHash::derive(b"state-a"),
                secret,
                datetime!(2026-10-30 12:00:00 UTC),
            ))
            .await
            .unwrap();
        let ciphertext: String = sqlx::query_scalar(
            "SELECT encrypted_flow_secret FROM folded_auth_attempts WHERE state_hash = $1",
        )
        .bind(started.attempt.state_hash.as_bytes().as_slice())
        .fetch_one(database.pool())
        .await
        .unwrap();
        assert!(!ciphertext.contains("relay-secret"));

        let reconstructed = PostgresFoldedAuthRepository::new(
            database.pool().clone(),
            cipher("AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"),
        );
        assert_eq!(
            reconstructed
                .restore_pending(&started.attempt.state_hash)
                .await
                .unwrap()
                .unwrap()
                .flow_secret
                .expose_bytes(),
            secret
        );
        let wrong_key = PostgresFoldedAuthRepository::new(
            database.pool().clone(),
            cipher("AQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQE"),
        );
        let error = wrong_key
            .restore_pending(&started.attempt.state_hash)
            .await
            .unwrap_err();
        assert_eq!(
            error.to_string(),
            "folded auth secret error: invalid folded auth flow envelope"
        );
        assert!(!format!("{error:?}").contains("relay-secret"));
        sqlx::query(
            "UPDATE folded_auth_attempts SET encrypted_flow_secret = encrypted_flow_secret || 'x' WHERE state_hash = $1",
        )
        .bind(started.attempt.state_hash.as_bytes().as_slice())
        .execute(database.pool())
        .await
        .unwrap();
        let corrupt_error = reconstructed
            .restore_pending(&started.attempt.state_hash)
            .await
            .unwrap_err();
        assert_eq!(
            corrupt_error.to_string(),
            "folded auth secret error: invalid folded auth flow envelope"
        );
        assert!(!format!("{corrupt_error:?}").contains("relay-secret"));
        sqlx::query(
            "WITH current AS (SELECT clock_timestamp() AS value)
             UPDATE folded_auth_attempts
             SET created_at = current.value - INTERVAL '10 minutes 1 second',
                 expires_at = current.value - INTERVAL '1 second'
             FROM current
             WHERE state_hash = $1",
        )
        .bind(started.attempt.state_hash.as_bytes().as_slice())
        .execute(database.pool())
        .await
        .unwrap();
        assert_eq!(
            repository
                .purge_terminal_before(datetime!(2000-01-01 00:00:00 UTC))
                .await
                .unwrap(),
            0
        );
        assert_eq!(
            repository
                .get_by_state_hash(&started.attempt.state_hash)
                .await
                .unwrap()
                .unwrap()
                .status,
            FoldedAuthStatus::Expired
        );
        let encrypted_after_expiry: Option<String> = sqlx::query_scalar(
            "SELECT encrypted_flow_secret FROM folded_auth_attempts WHERE state_hash = $1",
        )
        .bind(started.attempt.state_hash.as_bytes().as_slice())
        .fetch_one(database.pool())
        .await
        .unwrap();
        assert!(encrypted_after_expiry.is_none());
        database.cleanup().await;
    }

    #[tokio::test]
    async fn accepted_generation_atomically_inserts_bound_graph_task_and_erases_ciphertext() {
        let database = TestDatabase::create().await;
        let repository = PostgresFoldedAuthRepository::new(
            database.pool().clone(),
            cipher("AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"),
        );
        let lock = graph_lock();
        let started = repository
            .start_or_rotate(start_request(
                &lock,
                FoldedAuthStateHash::derive(b"state-b"),
                b"flow-b",
                time::OffsetDateTime::now_utc(),
            ))
            .await
            .unwrap();
        let task = repository
            .accept_and_insert_graph_task(
                &started.attempt.state_hash,
                started.attempt.generation,
                TaskId::from_str(TASK_ID).unwrap(),
                reader(),
                datetime!(2000-01-01 00:00:00 UTC),
            )
            .await
            .unwrap()
            .unwrap();
        assert!(task.snapshot.as_v1().accepted_at > datetime!(2020-01-01 00:00:00 UTC));
        let graph = PostgresGraphTaskRepository::new(database.pool().clone());
        assert_eq!(
            graph
                .get_graph_task_by_handle(&creator(), &BundleId::from_str(BUNDLE_ID).unwrap())
                .await
                .unwrap(),
            Some(task.clone())
        );
        let row: (String, Option<String>, Option<uuid::Uuid>) = sqlx::query_as(
            "SELECT status, encrypted_flow_secret, task_id FROM folded_auth_attempts",
        )
        .fetch_one(database.pool())
        .await
        .unwrap();
        assert_eq!(row.0, "accepted");
        assert!(row.1.is_none());
        assert_eq!(row.2, Some(task.task_id.as_uuid()));
        sqlx::query(
            "UPDATE folded_auth_attempts
             SET purge_after = clock_timestamp() - INTERVAL '1 second'",
        )
        .execute(database.pool())
        .await
        .unwrap();
        assert_eq!(
            repository
                .purge_terminal_before(datetime!(2000-01-01 00:00:00 UTC))
                .await
                .unwrap(),
            1
        );
        let folded_count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM folded_auth_attempts")
            .fetch_one(database.pool())
            .await
            .unwrap();
        assert_eq!(folded_count, 0);
        assert_eq!(
            graph
                .get_graph_task_by_handle(&creator(), &BundleId::from_str(BUNDLE_ID).unwrap())
                .await
                .unwrap(),
            Some(task)
        );
        database.cleanup().await;
    }

    #[tokio::test]
    async fn accept_storage_failure_leaves_dispatch_marker_and_startup_recovery_requires_restart() {
        let database = TestDatabase::create().await;
        let repository = PostgresFoldedAuthRepository::new(
            database.pool().clone(),
            cipher("AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"),
        );
        let lock = graph_lock();
        let started = repository
            .start_or_rotate(start_request(
                &lock,
                FoldedAuthStateHash::derive(b"postgres-accept-storage-failure"),
                b"consumed-postgres-flow",
                time::OffsetDateTime::now_utc(),
            ))
            .await
            .unwrap();
        repository
            .mark_poll_dispatched(
                &started.attempt.state_hash,
                started.attempt.generation,
                time::OffsetDateTime::now_utc(),
            )
            .await
            .unwrap()
            .unwrap();
        sqlx::query(
            "CREATE FUNCTION fail_folded_graph_insert() RETURNS trigger LANGUAGE plpgsql AS $$
             BEGIN RAISE EXCEPTION 'injected graph persistence failure'; END $$",
        )
        .execute(database.pool())
        .await
        .unwrap();
        sqlx::query(
            "CREATE TRIGGER fail_folded_graph_insert
             BEFORE INSERT ON verification_tasks
             FOR EACH ROW EXECUTE FUNCTION fail_folded_graph_insert()",
        )
        .execute(database.pool())
        .await
        .unwrap();

        assert!(matches!(
            repository
                .accept_and_insert_graph_task(
                    &started.attempt.state_hash,
                    started.attempt.generation,
                    TaskId::from_str(TASK_ID).unwrap(),
                    reader(),
                    time::OffsetDateTime::now_utc(),
                )
                .await,
            Err(crate::application::errors::ApplicationError::Storage { .. })
        ));
        assert!(
            repository
                .get_by_state_hash(&started.attempt.state_hash)
                .await
                .unwrap()
                .unwrap()
                .poll_dispatched
        );

        let reconstructed = PostgresFoldedAuthRepository::new(
            database.pool().clone(),
            cipher("AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"),
        );
        reconstructed
            .purge_terminal_before(time::OffsetDateTime::now_utc())
            .await
            .unwrap();
        assert_eq!(
            reconstructed
                .get_by_state_hash(&started.attempt.state_hash)
                .await
                .unwrap()
                .unwrap()
                .status,
            FoldedAuthStatus::RestartRequired
        );
        database.cleanup().await;
    }

    #[tokio::test]
    async fn rotation_fences_stale_results_and_late_acceptance_expires_atomically() {
        let database = TestDatabase::create().await;
        let repository = PostgresFoldedAuthRepository::new(
            database.pool().clone(),
            cipher("AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"),
        );
        let lock = graph_lock();
        let now = time::OffsetDateTime::now_utc();
        let first = repository
            .start_or_rotate(start_request(
                &lock,
                FoldedAuthStateHash::derive(b"postgres-state-one"),
                b"postgres-flow-one",
                now,
            ))
            .await
            .unwrap();
        let second = repository
            .start_or_rotate(start_request(
                &lock,
                FoldedAuthStateHash::derive(b"postgres-state-two"),
                b"postgres-flow-two",
                now + time::Duration::SECOND,
            ))
            .await
            .unwrap();
        assert_eq!(second.attempt.generation, first.attempt.generation + 1);
        assert!(
            repository
                .require_restart(
                    &first.attempt.state_hash,
                    first.attempt.generation,
                    now + time::Duration::seconds(2),
                )
                .await
                .unwrap()
                .is_none()
        );
        assert_eq!(
            repository
                .get_by_state_hash(&first.attempt.state_hash)
                .await
                .unwrap()
                .unwrap()
                .status,
            FoldedAuthStatus::RestartRequired
        );

        sqlx::query(
            "WITH current AS (SELECT clock_timestamp() AS value)
             UPDATE folded_auth_attempts
             SET created_at = current.value - INTERVAL '10 minutes',
                 expires_at = current.value
             FROM current
             WHERE state_hash = $1",
        )
        .bind(second.attempt.state_hash.as_bytes().as_slice())
        .execute(database.pool())
        .await
        .unwrap();
        assert!(
            repository
                .accept_and_insert_graph_task(
                    &second.attempt.state_hash,
                    second.attempt.generation,
                    TaskId::from_str(TASK_ID).unwrap(),
                    reader(),
                    time::OffsetDateTime::now_utc(),
                )
                .await
                .unwrap()
                .is_none()
        );
        let expired = repository
            .get_by_state_hash(&second.attempt.state_hash)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(expired.status, FoldedAuthStatus::Expired);
        assert!(
            repository
                .restore_pending(&second.attempt.state_hash)
                .await
                .unwrap()
                .is_none()
        );
        database.cleanup().await;
    }

    #[tokio::test]
    async fn blocked_pubky_poll_holds_no_postgres_lock_and_rotation_fences_late_result() {
        let database = TestDatabase::create().await;
        let repository = Arc::new(PostgresFoldedAuthRepository::new(
            database.pool().clone(),
            cipher("AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"),
        ));
        let lock = graph_lock();
        let now = time::OffsetDateTime::now_utc();
        let first = repository
            .start_or_rotate(start_request(
                &lock,
                FoldedAuthStateHash::derive(b"postgres-blocked-one"),
                b"postgres-blocked-flow-one",
                now,
            ))
            .await
            .unwrap();
        let entered = Arc::new(Notify::new());
        let release = Arc::new(Notify::new());
        let use_case = Arc::new(PollFoldedAuthAttempt::new(
            Arc::clone(&repository),
            Arc::new(BlockingPoller {
                entered: Arc::clone(&entered),
                release: Arc::clone(&release),
            }),
            Arc::new(FixedClock(now)),
        ));
        let state_hash = first.attempt.state_hash.clone();
        let polling = tokio::spawn(async move { use_case.execute(&state_hash).await.unwrap() });
        entered.notified().await;

        let second = tokio::time::timeout(
            std::time::Duration::from_secs(1),
            repository.start_or_rotate(start_request(
                &lock,
                FoldedAuthStateHash::derive(b"postgres-blocked-two"),
                b"postgres-blocked-flow-two",
                now,
            )),
        )
        .await
        .expect("rotation must not wait on Pubky I/O")
        .unwrap();
        release.notify_one();

        assert_eq!(polling.await.unwrap(), PollFoldedAuthOutcome::Stale);
        assert_eq!(
            repository
                .get_by_state_hash(&second.attempt.state_hash)
                .await
                .unwrap()
                .unwrap()
                .status,
            FoldedAuthStatus::Pending
        );
        database.cleanup().await;
    }

    #[tokio::test]
    async fn postgres_cleanup_preserves_active_approval_poll_and_accepts_its_result() {
        let database = TestDatabase::create().await;
        let repository = Arc::new(PostgresFoldedAuthRepository::new(
            database.pool().clone(),
            cipher("AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"),
        ));
        let now = time::OffsetDateTime::now_utc();
        let started = repository
            .start_or_rotate(start_request(
                &graph_lock(),
                FoldedAuthStateHash::derive(b"postgres-active-cleanup"),
                b"postgres-active-cleanup-flow",
                now,
            ))
            .await
            .unwrap();
        let entered = Arc::new(Notify::new());
        let release = Arc::new(Notify::new());
        let use_case = Arc::new(PollFoldedAuthAttempt::new(
            Arc::clone(&repository),
            Arc::new(BlockingApprovedPoller {
                entered: Arc::clone(&entered),
                release: Arc::clone(&release),
                reader: reader(),
            }),
            Arc::new(FixedClock(now)),
        ));
        let state_hash = started.attempt.state_hash.clone();
        let polling = tokio::spawn(async move { use_case.execute(&state_hash).await.unwrap() });
        entered.notified().await;

        assert_eq!(repository.purge_terminal_before(now).await.unwrap(), 0);
        assert_eq!(
            repository
                .get_by_state_hash(&started.attempt.state_hash)
                .await
                .unwrap()
                .unwrap()
                .status,
            FoldedAuthStatus::Pending
        );
        release.notify_one();

        assert!(matches!(
            polling.await.unwrap(),
            PollFoldedAuthOutcome::Accepted(_)
        ));
        assert_eq!(
            repository
                .get_by_state_hash(&started.attempt.state_hash)
                .await
                .unwrap()
                .unwrap()
                .status,
            FoldedAuthStatus::Accepted
        );
        database.cleanup().await;
    }

    #[tokio::test]
    async fn postgres_cleanup_requires_restart_for_abandoned_approval_dispatch() {
        let database = TestDatabase::create().await;
        let repository = PostgresFoldedAuthRepository::new(
            database.pool().clone(),
            cipher("AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"),
        );
        let now = time::OffsetDateTime::now_utc();
        let started = repository
            .start_or_rotate(start_request(
                &graph_lock(),
                FoldedAuthStateHash::derive(b"postgres-abandoned-cleanup"),
                b"postgres-abandoned-cleanup-flow",
                now,
            ))
            .await
            .unwrap();
        repository
            .mark_poll_dispatched(&started.attempt.state_hash, started.attempt.generation, now)
            .await
            .unwrap()
            .unwrap();
        sqlx::query(
            "UPDATE folded_auth_attempts
             SET poll_dispatched_at = clock_timestamp() - INTERVAL '61 seconds'
             WHERE state_hash = $1",
        )
        .bind(started.attempt.state_hash.as_bytes().as_slice())
        .execute(database.pool())
        .await
        .unwrap();

        assert_eq!(repository.purge_terminal_before(now).await.unwrap(), 0);
        assert_eq!(
            repository
                .get_by_state_hash(&started.attempt.state_hash)
                .await
                .unwrap()
                .unwrap()
                .status,
            FoldedAuthStatus::RestartRequired
        );
        database.cleanup().await;
    }

    #[tokio::test]
    async fn acceptance_and_rotation_share_row_before_admission_lock_order() {
        let database = TestDatabase::create().await;
        let repository = PostgresFoldedAuthRepository::new(
            database.pool().clone(),
            cipher("AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"),
        );
        let lock = graph_lock();
        let started = repository
            .start_or_rotate(start_request(
                &lock,
                FoldedAuthStateHash::derive(b"lock-order-initial"),
                b"lock-order-flow",
                time::OffsetDateTime::now_utc(),
            ))
            .await
            .unwrap();

        let mut blocker = database.pool().begin().await.unwrap();
        let blocker_pid: i32 = sqlx::query_scalar("SELECT pg_backend_pid()")
            .fetch_one(&mut *blocker)
            .await
            .unwrap();
        sqlx::query("SELECT state_hash FROM folded_auth_attempts WHERE state_hash = $1 FOR UPDATE")
            .bind(started.attempt.state_hash.as_bytes().as_slice())
            .fetch_one(&mut *blocker)
            .await
            .unwrap();

        let accepting_repository = repository.clone();
        let accepting_state = started.attempt.state_hash.clone();
        let accepting = tokio::spawn(async move {
            accepting_repository
                .accept_and_insert_graph_task(
                    &accepting_state,
                    started.attempt.generation,
                    TaskId::from_str(TASK_ID).unwrap(),
                    reader(),
                    time::OffsetDateTime::now_utc(),
                )
                .await
        });
        wait_for_lock_waiters(database.pool(), blocker_pid, 1).await;

        let rotating_repository = repository.clone();
        let rotating_lock = lock.clone();
        let rotating = tokio::spawn(async move {
            rotating_repository
                .start_or_rotate(start_request(
                    &rotating_lock,
                    FoldedAuthStateHash::derive(b"lock-order-rotation"),
                    b"lock-order-flow-rotation",
                    time::OffsetDateTime::now_utc(),
                ))
                .await
        });
        wait_for_lock_waiters(database.pool(), blocker_pid, 2).await;
        blocker.rollback().await.unwrap();

        let accepted = tokio::time::timeout(std::time::Duration::from_secs(2), accepting)
            .await
            .expect("acceptance must not deadlock with rotation")
            .unwrap()
            .unwrap();
        let rotated = tokio::time::timeout(std::time::Duration::from_secs(2), rotating)
            .await
            .expect("rotation must not deadlock with acceptance")
            .unwrap();
        assert!(accepted.is_some());
        assert!(matches!(
            rotated,
            Err(crate::application::errors::ApplicationError::VerificationTaskConflict)
        ));
        database.cleanup().await;
    }

    #[tokio::test]
    async fn pending_folded_auth_and_ordinary_v1_admission_are_exclusive_in_both_commit_orders() {
        for folded_first in [true, false] {
            let database = TestDatabase::create().await;
            let folded_repository = PostgresFoldedAuthRepository::new(
                database.pool().clone(),
                cipher("AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"),
            );
            let verification_repository =
                PostgresVerificationTaskRepository::new(database.pool().clone());
            let lock = graph_lock();
            let request = start_request(
                &lock,
                FoldedAuthStateHash::derive(if folded_first {
                    b"v1-folded-first"
                } else {
                    b"v1-ordinary-first"
                }),
                b"v1-exclusive-flow",
                time::OffsetDateTime::now_utc(),
            );
            let task = ordinary_v1_task();

            if folded_first {
                folded_repository.start_or_rotate(request).await.unwrap();
                assert!(matches!(
                    verification_repository.insert_verification_task(task).await,
                    Err(crate::application::errors::ApplicationError::VerificationTaskConflict)
                ));
            } else {
                verification_repository
                    .insert_verification_task(task)
                    .await
                    .unwrap();
                assert!(matches!(
                    folded_repository.start_or_rotate(request).await,
                    Err(crate::application::errors::ApplicationError::VerificationTaskConflict)
                ));
            }
            let folded_count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM folded_auth_attempts")
                .fetch_one(database.pool())
                .await
                .unwrap();
            let ordinary_count: i64 = sqlx::query_scalar(
                "SELECT COUNT(*) FROM verification_tasks WHERE evaluator_snapshot IS NULL",
            )
            .fetch_one(database.pool())
            .await
            .unwrap();
            assert_eq!(
                (folded_count, ordinary_count),
                if folded_first { (1, 0) } else { (0, 1) }
            );
            database.cleanup().await;
        }
    }

    #[tokio::test]
    async fn pending_folded_auth_and_ordinary_graph_admission_are_mutually_exclusive_in_both_orders()
     {
        for folded_first in [true, false] {
            let database = TestDatabase::create().await;
            let folded_repository = PostgresFoldedAuthRepository::new(
                database.pool().clone(),
                cipher("AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"),
            );
            let graph_repository = PostgresGraphTaskRepository::new(database.pool().clone());
            let lock = graph_lock();
            let request = start_request(
                &lock,
                FoldedAuthStateHash::derive(if folded_first {
                    b"folded-first"
                } else {
                    b"graph-first"
                }),
                b"exclusive-flow",
                time::OffsetDateTime::now_utc(),
            );
            let task = ordinary_graph_task(&lock);

            let mut blocker = database.pool().begin().await.unwrap();
            let blocker_pid: i32 = sqlx::query_scalar("SELECT pg_backend_pid()")
                .fetch_one(&mut *blocker)
                .await
                .unwrap();
            super::super::proof_admission::lock_proof_admission(
                &mut blocker,
                &creator(),
                &lock.lock_id().unwrap(),
            )
            .await
            .unwrap();

            let (first, second) = if folded_first {
                let first_repository = folded_repository.clone();
                let first = tokio::spawn(async move {
                    first_repository.start_or_rotate(request).await.map(|_| ())
                });
                wait_for_advisory_waiters(database.pool(), blocker_pid, 1).await;
                let second = tokio::spawn(async move {
                    graph_repository.insert_graph_task(task).await.map(|_| ())
                });
                (first, second)
            } else {
                let first_repository = graph_repository.clone();
                let first = tokio::spawn(async move {
                    first_repository.insert_graph_task(task).await.map(|_| ())
                });
                wait_for_advisory_waiters(database.pool(), blocker_pid, 1).await;
                let second = tokio::spawn(async move {
                    folded_repository.start_or_rotate(request).await.map(|_| ())
                });
                (first, second)
            };
            wait_for_advisory_waiters(database.pool(), blocker_pid, 2).await;
            blocker.rollback().await.unwrap();

            assert!(first.await.unwrap().is_ok());
            assert!(matches!(
                second.await.unwrap(),
                Err(crate::application::errors::ApplicationError::VerificationTaskConflict)
            ));
            let folded_count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM folded_auth_attempts")
                .fetch_one(database.pool())
                .await
                .unwrap();
            let graph_count: i64 = sqlx::query_scalar(
                "SELECT COUNT(*) FROM verification_tasks WHERE evaluator_snapshot IS NOT NULL",
            )
            .fetch_one(database.pool())
            .await
            .unwrap();
            assert_eq!(
                (folded_count, graph_count),
                if folded_first { (1, 0) } else { (0, 1) }
            );
            database.cleanup().await;
        }
    }

    async fn wait_for_lock_waiters(pool: &sqlx::PgPool, blocker_pid: i32, expected: i64) {
        tokio::time::timeout(std::time::Duration::from_secs(2), async {
            loop {
                let waiters: i64 = sqlx::query_scalar(
                    "WITH RECURSIVE blocked(pid) AS (
                         SELECT pid FROM pg_stat_activity
                         WHERE $1 = ANY(pg_blocking_pids(pid))
                         UNION
                         SELECT activity.pid FROM pg_stat_activity AS activity
                         JOIN blocked ON blocked.pid = ANY(pg_blocking_pids(activity.pid))
                     )
                     SELECT COUNT(*) FROM blocked",
                )
                .bind(blocker_pid)
                .fetch_one(pool)
                .await
                .unwrap();
                if waiters >= expected {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("contenders wait on the folded-auth row lock");
    }

    async fn wait_for_advisory_waiters(pool: &sqlx::PgPool, blocker_pid: i32, expected: i64) {
        tokio::time::timeout(std::time::Duration::from_secs(2), async {
            loop {
                let waiters: i64 = sqlx::query_scalar(
                    "SELECT COUNT(*) FROM pg_stat_activity
                     WHERE $1 = ANY(pg_blocking_pids(pid)) AND wait_event = 'advisory'",
                )
                .bind(blocker_pid)
                .fetch_one(pool)
                .await
                .unwrap();
                if waiters >= expected {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("admission contender waits on the shared advisory fence");
    }

    struct BlockingPoller {
        entered: Arc<Notify>,
        release: Arc<Notify>,
    }

    #[async_trait]
    impl FoldedAuthApprovalPoller for BlockingPoller {
        async fn poll_approval(&self, _flow_secret: &FoldedAuthFlowSecret) -> FoldedAuthPollCall {
            self.entered.notify_one();
            self.release.notified().await;
            FoldedAuthPollCall::Attempted(Err(FoldedAuthApprovalPollError))
        }
    }

    struct BlockingApprovedPoller {
        entered: Arc<Notify>,
        release: Arc<Notify>,
        reader: CreatorPubky,
    }

    #[async_trait]
    impl FoldedAuthApprovalPoller for BlockingApprovedPoller {
        async fn poll_approval(&self, _flow_secret: &FoldedAuthFlowSecret) -> FoldedAuthPollCall {
            self.entered.notify_one();
            self.release.notified().await;
            FoldedAuthPollCall::Attempted(Ok(FoldedAuthApprovalPoll::Approved {
                reader: self.reader.clone(),
            }))
        }
    }

    struct FixedClock(time::OffsetDateTime);

    impl Clock for FixedClock {
        fn now(&self) -> time::OffsetDateTime {
            self.0
        }
    }

    fn start_request(
        lock: &ContentLockV2,
        state_hash: FoldedAuthStateHash,
        flow_secret: &[u8],
        created_at: time::OffsetDateTime,
    ) -> StartFoldedAuthAttempt {
        let bundle = SubmittedProofBundleV2 {
            version: 2,
            bundle_id: BundleId::from_str(BUNDLE_ID).unwrap(),
            pubky_lock_resource: PubkyLockResource::new(
                creator(),
                ContentLockPath::from_lock_id(lock.lock_id().unwrap()),
            ),
            reader_pubky: None,
        };
        StartFoldedAuthAttempt {
            state_hash,
            submitted_proof_bundle: bundle.clone(),
            pubky_lock_resource: bundle.pubky_lock_resource.clone(),
            lock_hash: *lock.lock_hash().unwrap().as_bytes(),
            verification_window_seconds: lock.verification_window_seconds,
            graph: lock.graph.clone().unwrap(),
            flow_secret: FoldedAuthFlowSecret::new(flow_secret.to_vec()),
            created_at,
        }
    }

    fn canonical_now() -> time::OffsetDateTime {
        let now = time::OffsetDateTime::now_utc();
        now.replace_nanosecond((now.nanosecond() / 1_000) * 1_000)
            .unwrap()
    }

    fn ordinary_v1_task() -> VerificationTaskRecord {
        VerificationTaskRecord {
            task_id: TaskId::from_str("018fc6ec-2f3d-4f7e-8b7d-6f5c4b3a2d10").unwrap(),
            creator: creator(),
            submitted_proof_bundle: SubmittedProofBundle {
                version: SUBMITTED_PROOF_BUNDLE_VERSION,
                bundle_id: BundleId::from_str(BUNDLE_ID).unwrap(),
                pubky_lock_resource: PubkyLockResource::from_str(&format!(
                    "{CREATOR}/pub/app.locks/100G40R40M30E209185GR38E1W8124GK2GAHC5RR34D1P70X3RFG.json"
                ))
                .unwrap(),
                reader_public_key: None,
                proofs: vec![Proof {
                    criterion_id: "criterion-1".to_owned(),
                    verifier_type: VerifierType::DevStatic,
                    payload: json!({}),
                }],
            },
            status: VerificationTaskStatus::Pending,
            submitted_at: canonical_now(),
            started_at: None,
            completed_at: None,
            failure_message: None,
            terminal_reason: None,
            entitlement_to_publish: None,
        }
    }

    fn ordinary_graph_task(lock: &ContentLockV2) -> GraphTaskRecord {
        GraphTaskRecord {
            task_id: TaskId::from_str(TASK_ID).unwrap(),
            snapshot: GraphTaskSnapshot::V1(
                GraphTaskSnapshotV1::new(
                    creator(),
                    BundleId::from_str(BUNDLE_ID).unwrap(),
                    PubkyLockResource::new(
                        creator(),
                        ContentLockPath::from_lock_id(lock.lock_id().unwrap()),
                    ),
                    *lock.lock_hash().unwrap().as_bytes(),
                    reader(),
                    canonical_now(),
                    lock.verification_window_seconds,
                    lock.graph.clone().unwrap(),
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

    fn graph_lock() -> ContentLockV2 {
        ContentLockV2 {
            version: 2,
            creator: creator(),
            primary_resource: Some(
                GuardedResource::new(
                    "/priv/app.locks/content/item.txt",
                    GuardedResourceHash::from_bytes([3; 32]),
                    "text/plain",
                    4,
                )
                .unwrap(),
            ),
            secondary_resources: BTreeMap::new(),
            payment: None,
            graph: Some(GraphPolicyV2 {
                predicates: vec![GraphPredicateV2::Follows {
                    follower: GraphParticipantV2::Reader,
                    followed: GraphParticipantV2::Creator,
                }],
                operator: None,
            }),
            verification_window_seconds: 300,
            access_policy: AccessPolicy {
                requested_credential_ttl_seconds: 300,
            },
            lock_server: LockServerConfig { override_: None },
            created_at: datetime!(2026-10-30 11:00:00 UTC),
        }
    }

    fn creator() -> CreatorPubky {
        CreatorPubky::from_str(CREATOR).unwrap()
    }

    fn reader() -> CreatorPubky {
        CreatorPubky::from_str(READER).unwrap()
    }

    fn cipher(encoded_master_key: &str) -> FoldedAuthSecretCipher {
        let master_key = RuntimeMasterKey::from_base64url(encoded_master_key).unwrap();
        FoldedAuthSecretCipher::from_runtime_master_key(&master_key)
    }
}
