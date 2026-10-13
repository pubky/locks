use locks_core::ids::{CreatorPubky, LockId};
use sqlx::{Postgres, Transaction};

use crate::application::errors::ApplicationError;

const PROOF_ADMISSION_LOCK_NAMESPACE: &str = "locks:proof-admission:v1";

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
