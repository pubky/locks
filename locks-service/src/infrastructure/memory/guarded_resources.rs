use std::collections::HashMap;

use async_trait::async_trait;
use tokio::sync::RwLock;

use locks_core::ids::{CreatorPubky, GuardedResourceHash};

use crate::application::errors::ApplicationError;
use crate::application::models::GuardedResourceRecord;
use crate::application::ports::GuardedResourceRepository;

type GuardedResourceKey = (CreatorPubky, String);

/// In-memory guarded resource repository for local creator publishing and fake proxy reads.
#[derive(Debug, Default)]
pub struct InMemoryGuardedResourceRepository {
    records: RwLock<HashMap<GuardedResourceKey, GuardedResourceRecord>>,
}

impl InMemoryGuardedResourceRepository {
    /// Creates an empty repository.
    pub fn new() -> Self {
        Self::default()
    }
}

#[async_trait]
impl GuardedResourceRepository for InMemoryGuardedResourceRepository {
    async fn upsert_guarded_resource(
        &self,
        guarded_resource: GuardedResourceRecord,
    ) -> Result<(), ApplicationError> {
        self.records.write().await.insert(
            (
                guarded_resource.creator.clone(),
                guarded_resource.path.clone(),
            ),
            guarded_resource,
        );
        Ok(())
    }

    async fn get_guarded_resource(
        &self,
        creator: &CreatorPubky,
        path: &str,
        hash: &GuardedResourceHash,
    ) -> Result<Option<GuardedResourceRecord>, ApplicationError> {
        let record = self
            .records
            .read()
            .await
            .get(&(creator.clone(), path.to_owned()))
            .filter(|record| record.hash == *hash)
            .cloned();
        Ok(record)
    }

    async fn get_current_guarded_resource(
        &self,
        creator: &CreatorPubky,
        path: &str,
    ) -> Result<Option<GuardedResourceRecord>, ApplicationError> {
        Ok(self
            .records
            .read()
            .await
            .get(&(creator.clone(), path.to_owned()))
            .cloned())
    }

    async fn delete_guarded_resource(
        &self,
        creator: &CreatorPubky,
        path: &str,
    ) -> Result<bool, ApplicationError> {
        Ok(self
            .records
            .write()
            .await
            .remove(&(creator.clone(), path.to_owned()))
            .is_some())
    }
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use super::*;

    #[tokio::test]
    async fn upsert_replaces_current_path_record_and_reads_only_when_hash_matches() {
        let repo = InMemoryGuardedResourceRepository::new();
        let creator =
            CreatorPubky::from_str("pubkytkrq8zmwb8a3m9k15csu3q17qmfgqnp9dskbrg9uq1rydpyxp7qy")
                .unwrap();
        let old_hash = GuardedResourceHash::from_bytes([7; 32]);
        let current_hash = GuardedResourceHash::from_bytes([8; 32]);

        assert_eq!(
            repo.get_guarded_resource(&creator, "/pub/file.txt", &old_hash)
                .await
                .unwrap(),
            None
        );
        assert_eq!(
            repo.get_current_guarded_resource(&creator, "/pub/file.txt")
                .await
                .unwrap(),
            None
        );

        repo.upsert_guarded_resource(GuardedResourceRecord {
            creator: creator.clone(),
            path: "/pub/file.txt".to_owned(),
            hash: old_hash,
            content_type: "text/plain".to_owned(),
            size: 5,
            bytes: b"first".to_vec(),
        })
        .await
        .unwrap();
        repo.upsert_guarded_resource(GuardedResourceRecord {
            creator: creator.clone(),
            path: "/pub/file.txt".to_owned(),
            hash: current_hash,
            content_type: "image/png".to_owned(),
            size: 6,
            bytes: b"second".to_vec(),
        })
        .await
        .unwrap();

        assert_eq!(
            repo.get_guarded_resource(&creator, "/pub/file.txt", &old_hash)
                .await
                .unwrap(),
            None
        );
        assert_eq!(
            repo.get_guarded_resource(&creator, "/pub/file.txt", &current_hash)
                .await
                .unwrap(),
            Some(GuardedResourceRecord {
                creator: creator.clone(),
                path: "/pub/file.txt".to_owned(),
                hash: current_hash,
                content_type: "image/png".to_owned(),
                size: 6,
                bytes: b"second".to_vec(),
            })
        );
        assert_eq!(
            repo.get_current_guarded_resource(&creator, "/pub/file.txt")
                .await
                .unwrap(),
            Some(GuardedResourceRecord {
                creator,
                path: "/pub/file.txt".to_owned(),
                hash: current_hash,
                content_type: "image/png".to_owned(),
                size: 6,
                bytes: b"second".to_vec(),
            })
        );
    }

    #[tokio::test]
    async fn delete_guarded_resource_removes_current_record_and_reports_missing() {
        let repo = InMemoryGuardedResourceRepository::new();
        let creator =
            CreatorPubky::from_str("pubkytkrq8zmwb8a3m9k15csu3q17qmfgqnp9dskbrg9uq1rydpyxp7qy")
                .unwrap();
        let hash = GuardedResourceHash::from_bytes([9; 32]);

        assert!(
            !repo
                .delete_guarded_resource(&creator, "/priv/locks.app/content/delete.txt")
                .await
                .unwrap()
        );

        repo.upsert_guarded_resource(GuardedResourceRecord {
            creator: creator.clone(),
            path: "/priv/locks.app/content/delete.txt".to_owned(),
            hash,
            content_type: "text/plain".to_owned(),
            size: 6,
            bytes: b"delete".to_vec(),
        })
        .await
        .unwrap();

        assert!(
            repo.delete_guarded_resource(&creator, "/priv/locks.app/content/delete.txt")
                .await
                .unwrap()
        );
        assert_eq!(
            repo.get_current_guarded_resource(&creator, "/priv/locks.app/content/delete.txt")
                .await
                .unwrap(),
            None
        );
        assert!(
            !repo
                .delete_guarded_resource(&creator, "/priv/locks.app/content/delete.txt")
                .await
                .unwrap()
        );
    }
}
