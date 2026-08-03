use async_trait::async_trait;

use crate::application::errors::ApplicationError;
use crate::application::models::{
    AccessCredential, AccessCredentialLookupKey, AccessCredentialRecord,
};

/// Store for issued opaque access credentials.
#[async_trait]
pub trait AccessCredentialStore: Send + Sync {
    /// Inserts an issued access credential record.
    ///
    /// Returns `DuplicateRecord` if the credential lookup key already exists.
    async fn insert_access_credential(
        &self,
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
}

/// Generator for opaque access credentials.
#[async_trait]
pub trait AccessCredentialGenerator: Send + Sync {
    /// Generates a new raw bearer access credential.
    async fn generate_access_credential(&self) -> Result<AccessCredential, ApplicationError>;
}
