use async_trait::async_trait;
use locks_core::ids::{BundleId, CreatorPubky, LockId};
use time::OffsetDateTime;
use uuid::Uuid;

use crate::application::errors::ApplicationError;
use crate::application::models::{
    AccessCredential, AccessCredentialLookupKey, AccessCredentialRecord, DeletionReadAuthorization,
    IssuedDeletionCredential,
};

/// Store for issued opaque access credentials.
#[async_trait]
pub trait AccessCredentialStore: Send + Sync {
    /// Inserts an issued access credential record.
    ///
    /// Returns `DuplicateRecord` if the credential lookup key already exists.
    async fn insert_access_credential(
        &self,
        lock_id: &LockId,
        lookup_key: AccessCredentialLookupKey,
        record: AccessCredentialRecord,
    ) -> Result<(), ApplicationError>;

    /// Resolves an access credential lookup key to its server-side state.
    ///
    /// Returns `Ok(None)` when the credential is absent.
    async fn get_access_credential(
        &self,
        lookup_key: &AccessCredentialLookupKey,
    ) -> Result<Option<AccessCredentialRecord>, ApplicationError>;

    /// Ensures an access credential is absent.
    ///
    /// Deleting a missing credential is successful.
    async fn delete_access_credential(
        &self,
        lookup_key: &AccessCredentialLookupKey,
    ) -> Result<(), ApplicationError>;

    async fn initialize_final_access_windows(
        &self,
        _deletion_job_id: Uuid,
        _worker_id: &str,
        _claim_token: Uuid,
        _now: OffsetDateTime,
        _issuance_deadline: OffsetDateTime,
        _read_deadline: OffsetDateTime,
    ) -> Result<bool, ApplicationError> {
        Ok(false)
    }

    async fn issue_or_replay_final_credential(
        &self,
        _creator: &CreatorPubky,
        _bundle_id: &BundleId,
        _now: OffsetDateTime,
        _candidate: AccessCredential,
    ) -> Result<Option<IssuedDeletionCredential>, ApplicationError> {
        Ok(None)
    }

    /// Reports whether this deletion Bundle may issue or replay its final credential now.
    async fn final_credential_available(
        &self,
        _creator: &CreatorPubky,
        _bundle_id: &BundleId,
        _now: OffsetDateTime,
    ) -> Result<bool, ApplicationError> {
        Ok(false)
    }

    async fn prepare_deletion_read(
        &self,
        _lookup_key: &AccessCredentialLookupKey,
        _path: &str,
        _now: OffsetDateTime,
        _claim_expires_at: OffsetDateTime,
    ) -> Result<Option<DeletionReadAuthorization>, ApplicationError> {
        Ok(None)
    }

    /// Reports whether a credential was enrolled in deletion, regardless of
    /// whether deletion access is currently usable.
    async fn deletion_credential_enrolled(
        &self,
        _lookup_key: &AccessCredentialLookupKey,
    ) -> Result<bool, ApplicationError> {
        Ok(false)
    }

    async fn release_deletion_read(
        &self,
        _lookup_key: &AccessCredentialLookupKey,
        _path: &str,
        _claim_token: Uuid,
        _now: OffsetDateTime,
    ) -> Result<bool, ApplicationError> {
        Ok(false)
    }

    async fn consume_deletion_read(
        &self,
        _lookup_key: &AccessCredentialLookupKey,
        _path: &str,
        _claim_token: Uuid,
        _now: OffsetDateTime,
    ) -> Result<bool, ApplicationError> {
        Ok(false)
    }
}

/// Generator for opaque access credentials.
#[async_trait]
pub trait AccessCredentialGenerator: Send + Sync {
    /// Generates a new raw bearer access credential.
    async fn generate_access_credential(&self) -> Result<AccessCredential, ApplicationError>;
}
