use std::collections::HashMap;

use async_trait::async_trait;
use tokio::sync::RwLock;

use locks_core::ids::{ContentLockPath, CreatorPubky};
use locks_core::lock_policy::ContentLock;

use crate::application::errors::ApplicationError;
use crate::application::ports::ContentLockRepository;

type ContentLockKey = (CreatorPubky, ContentLockPath);

/// In-memory content lock repository for the first retrieval/access slice.
#[derive(Debug, Default)]
pub struct InMemoryContentLockRepository {
    records: RwLock<HashMap<ContentLockKey, ContentLock>>,
}

impl InMemoryContentLockRepository {
    /// Creates an empty repository.
    pub fn new() -> Self {
        Self::default()
    }
}

#[async_trait]
impl ContentLockRepository for InMemoryContentLockRepository {
    async fn upsert_content_lock(
        &self,
        creator: CreatorPubky,
        content_lock_path: ContentLockPath,
        content_lock: ContentLock,
    ) -> Result<(), ApplicationError> {
        self.records
            .write()
            .await
            .insert((creator, content_lock_path), content_lock);
        Ok(())
    }

    async fn get_content_lock(
        &self,
        creator: &CreatorPubky,
        content_lock_path: &ContentLockPath,
    ) -> Result<Option<ContentLock>, ApplicationError> {
        Ok(self
            .records
            .read()
            .await
            .get(&(creator.clone(), content_lock_path.clone()))
            .cloned())
    }

    async fn delete_content_lock(
        &self,
        creator: &CreatorPubky,
        content_lock_path: &ContentLockPath,
    ) -> Result<bool, ApplicationError> {
        Ok(self
            .records
            .write()
            .await
            .remove(&(creator.clone(), content_lock_path.clone()))
            .is_some())
    }
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use serde_json::json;
    use time::macros::datetime;

    use locks_core::ids::{CreatorPubky, GuardedResourceHash, LockServerPubky};
    use locks_core::lock_policy::{
        AccessPolicy, CONTENT_LOCK_VERSION, ContentLock, Criterion, GuardedResource, LockLogic,
        LockServerConfig, VerifierType,
    };

    use super::*;

    #[tokio::test]
    async fn upsert_replaces_and_missing_read_returns_none() {
        let repo = InMemoryContentLockRepository::new();
        let creator = creator();
        let first = content_lock(900);
        let first_path = first.content_lock_path().unwrap();
        let second = content_lock(901);

        assert_eq!(
            repo.get_content_lock(&creator, &first_path).await.unwrap(),
            None
        );

        repo.upsert_content_lock(creator.clone(), first_path.clone(), first)
            .await
            .unwrap();
        repo.upsert_content_lock(creator.clone(), first_path.clone(), second.clone())
            .await
            .unwrap();

        assert_eq!(
            repo.get_content_lock(&creator, &first_path).await.unwrap(),
            Some(second)
        );
    }

    fn creator() -> CreatorPubky {
        CreatorPubky::from_str("pubkytkrq8zmwb8a3m9k15csu3q17qmfgqnp9dskbrg9uq1rydpyxp7qy").unwrap()
    }

    fn server() -> LockServerPubky {
        LockServerPubky::from_str("pubky7ir1ttte48bcp4zjychjyscicrwi1j34mtt91ptsafdbjmr8g9eo")
            .unwrap()
    }

    fn content_lock(ttl: u64) -> ContentLock {
        ContentLock {
            version: CONTENT_LOCK_VERSION,
            creator: creator(),
            primary_resource: Some(GuardedResource {
                path: "/priv/locks.app/content/hello.txt".to_owned(),
                hash: GuardedResourceHash::from_bytes([7; 32]),
                content_type: "text/plain".to_owned(),
                size: 13,
            }),
            secondary_resources: Default::default(),
            criteria: vec![Criterion {
                criterion_id: "criterion-1".to_owned(),
                verifier_type: VerifierType::DevStatic,
                params: json!({ "satisfied": true }),
            }],
            lock_logic: LockLogic::All {
                criteria: vec!["criterion-1".to_owned()],
            },
            access_policy: AccessPolicy {
                requested_credential_ttl_seconds: ttl,
            },
            lock_server: LockServerConfig {
                override_: Some(server()),
            },
            created_at: datetime!(2026-05-29 12:00:00 UTC),
        }
    }
}
