use async_trait::async_trait;
use sqlx::{Connection, PgConnection, PgPool};
use uuid::Uuid;

use crate::application::{
    errors::ApplicationError,
    models::ContentLockDeletionPhase,
    ports::{
        ContentLockDeletionActionAcquireResult, ContentLockDeletionActionClaim,
        ContentLockDeletionActionGuard, ContentLockDeletionActionOwnership,
    },
};

/// PostgreSQL session-advisory-lock ownership for deletion external actions.
#[derive(Debug, Clone)]
pub struct PostgresContentLockDeletionActionOwnership {
    pool: PgPool,
}

impl PostgresContentLockDeletionActionOwnership {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }
}

#[async_trait]
impl ContentLockDeletionActionOwnership for PostgresContentLockDeletionActionOwnership {
    async fn try_acquire(
        &self,
        claim: ContentLockDeletionActionClaim<'_>,
    ) -> Result<ContentLockDeletionActionAcquireResult, ApplicationError> {
        let lock_key = action_lock_key(claim.job_id);
        let mut pooled = self.pool.acquire().await.map_err(storage_error)?;
        let acquired: bool = sqlx::query_scalar("SELECT pg_try_advisory_lock($1)")
            .bind(lock_key)
            .fetch_one(&mut *pooled)
            .await
            .map_err(storage_error)?;
        if !acquired {
            return Ok(ContentLockDeletionActionAcquireResult::Busy);
        }

        // Detach immediately after locking. Any validation error then closes the
        // session on drop instead of returning a locked connection to the pool.
        let mut connection = pooled.detach();
        let live: bool = sqlx::query_scalar(
            "SELECT EXISTS (
                 SELECT 1
                 FROM content_lock_deletion_jobs
                 WHERE job_id = $1
                   AND state = 'running'
                   AND claimed_by = $2
                   AND claim_token = $3
                   AND claim_expires_at > clock_timestamp()
                   AND phase = $4
                   AND (($5 AND force_requested_at IS NOT NULL)
                     OR (NOT $5 AND force_requested_at IS NULL))
             )",
        )
        .bind(claim.job_id)
        .bind(claim.worker_id)
        .bind(claim.claim_token)
        .bind(phase_to_database(claim.expected_phase))
        .bind(claim.force)
        .fetch_one(&mut connection)
        .await
        .map_err(storage_error)?;

        if !live {
            unlock_and_close(connection, lock_key).await?;
            return Ok(ContentLockDeletionActionAcquireResult::ClaimLost);
        }

        Ok(ContentLockDeletionActionAcquireResult::Acquired(Box::new(
            PostgresContentLockDeletionActionGuard {
                connection: Some(connection),
                lock_key,
            },
        )))
    }
}

struct PostgresContentLockDeletionActionGuard {
    connection: Option<PgConnection>,
    lock_key: i64,
}

#[async_trait]
impl ContentLockDeletionActionGuard for PostgresContentLockDeletionActionGuard {
    async fn release(mut self: Box<Self>) -> Result<(), ApplicationError> {
        let connection = self
            .connection
            .take()
            .ok_or_else(|| ApplicationError::Storage {
                message: "content lock deletion action ownership was already released".to_owned(),
            })?;
        unlock_and_close(connection, self.lock_key).await
    }
}

impl Drop for PostgresContentLockDeletionActionGuard {
    fn drop(&mut self) {
        drop(self.connection.take());
    }
}

async fn unlock_and_close(
    mut connection: PgConnection,
    lock_key: i64,
) -> Result<(), ApplicationError> {
    let unlock_result = sqlx::query_scalar::<_, bool>("SELECT pg_advisory_unlock($1)")
        .bind(lock_key)
        .fetch_one(&mut connection)
        .await;
    let close_result = connection.close().await;
    let unlocked = unlock_result.map_err(storage_error)?;
    close_result.map_err(storage_error)?;
    if !unlocked {
        return Err(ApplicationError::Storage {
            message: "content lock deletion action ownership was not held".to_owned(),
        });
    }
    Ok(())
}

fn phase_to_database(phase: ContentLockDeletionPhase) -> &'static str {
    match phase {
        ContentLockDeletionPhase::Withdraw => "withdraw",
        ContentLockDeletionPhase::StartPaymentDrain => "start_payment_drain",
        ContentLockDeletionPhase::DrainPayments => "drain_payments",
        ContentLockDeletionPhase::DrainExistingCredentials => "drain_existing_credentials",
        ContentLockDeletionPhase::IssueFinalCredentials => "issue_final_credentials",
        ContentLockDeletionPhase::DrainFinalReads => "drain_final_reads",
        ContentLockDeletionPhase::DeleteContent => "delete_content",
        ContentLockDeletionPhase::DeleteTombstone => "delete_tombstone",
        ContentLockDeletionPhase::PurgeOperationalState => "purge_operational_state",
    }
}

fn action_lock_key(job_id: Uuid) -> i64 {
    let digest = blake3::derive_key(
        "pubky-locks content-lock deletion external action ownership v1",
        job_id.as_bytes(),
    );
    i64::from_be_bytes(digest[..8].try_into().expect("eight-byte digest prefix"))
}

fn storage_error(error: sqlx::Error) -> ApplicationError {
    ApplicationError::Storage {
        message: error.to_string(),
    }
}
