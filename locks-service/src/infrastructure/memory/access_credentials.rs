use std::collections::HashMap;

use async_trait::async_trait;
use tokio::sync::RwLock;

use crate::application::errors::ApplicationError;
use crate::application::models::{AccessCredentialLookupKey, AccessCredentialRecord};
use crate::application::ports::AccessCredentialStore;

/// In-memory access credential store keyed by non-secret lookup key.
#[derive(Debug, Default)]
pub struct InMemoryAccessCredentialStore {
    records: RwLock<HashMap<AccessCredentialLookupKey, AccessCredentialRecord>>,
}

impl InMemoryAccessCredentialStore {
    /// Creates an empty store.
    pub fn new() -> Self {
        Self::default()
    }
}

#[async_trait]
impl AccessCredentialStore for InMemoryAccessCredentialStore {
    async fn insert_access_credential(
        &self,
        lookup_key: AccessCredentialLookupKey,
        record: AccessCredentialRecord,
    ) -> Result<(), ApplicationError> {
        let mut records = self.records.write().await;
        if records.contains_key(&lookup_key) {
            return Err(ApplicationError::DuplicateRecord {
                record: "access_credential",
            });
        }
        records.insert(lookup_key, record);
        Ok(())
    }

    async fn get_access_credential(
        &self,
        lookup_key: &AccessCredentialLookupKey,
    ) -> Result<Option<AccessCredentialRecord>, ApplicationError> {
        Ok(self.records.read().await.get(lookup_key).cloned())
    }

    async fn delete_access_credential(
        &self,
        lookup_key: &AccessCredentialLookupKey,
    ) -> Result<(), ApplicationError> {
        self.records.write().await.remove(lookup_key);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use time::macros::datetime;

    use locks_core::ids::{BundleId, CreatorPubky};

    use super::*;
    use crate::application::models::AccessCredential;

    #[tokio::test]
    async fn insert_rejects_duplicate_read_miss_is_none_delete_is_ensure_absent() {
        let store = InMemoryAccessCredentialStore::new();
        let credential = AccessCredential::new("raw-bearer-credential");
        let lookup_key = AccessCredentialLookupKey::derive(&credential);
        let record = record();

        assert_eq!(
            store.get_access_credential(&lookup_key).await.unwrap(),
            None
        );
        store
            .insert_access_credential(lookup_key.clone(), record.clone())
            .await
            .unwrap();
        assert_eq!(
            store.get_access_credential(&lookup_key).await.unwrap(),
            Some(record.clone())
        );
        assert_eq!(
            store
                .insert_access_credential(lookup_key.clone(), record)
                .await,
            Err(ApplicationError::DuplicateRecord {
                record: "access_credential",
            })
        );

        store.delete_access_credential(&lookup_key).await.unwrap();
        store.delete_access_credential(&lookup_key).await.unwrap();
        assert_eq!(
            store.get_access_credential(&lookup_key).await.unwrap(),
            None
        );
    }

    fn record() -> AccessCredentialRecord {
        AccessCredentialRecord {
            creator: CreatorPubky::from_str(
                "pubkytkrq8zmwb8a3m9k15csu3q17qmfgqnp9dskbrg9uq1rydpyxp7qy",
            )
            .unwrap(),
            bundle_id: BundleId::from_str("000G40R40M30E209185GR38E1W").unwrap(),
            expires_at: datetime!(2026-05-29 12:15:00 UTC),
        }
    }
}
