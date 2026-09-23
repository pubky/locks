use std::{collections::HashMap, sync::Arc};

use locks_core::ids::{ContentLockPath, CreatorPubky};
use tokio::sync::RwLock;

pub(crate) type PublicContentLockKey = (CreatorPubky, ContentLockPath);

/// Shared in-memory backing for the one canonical public content-lock path.
#[derive(Debug, Clone, Default)]
pub struct InMemoryPublicContentLockStore {
    records: Arc<RwLock<HashMap<PublicContentLockKey, Vec<u8>>>>,
}

impl InMemoryPublicContentLockStore {
    pub fn new() -> Self {
        Self::default()
    }

    pub(crate) async fn put(&self, creator: CreatorPubky, path: ContentLockPath, bytes: Vec<u8>) {
        self.records.write().await.insert((creator, path), bytes);
    }

    pub(crate) async fn get(
        &self,
        creator: &CreatorPubky,
        path: &ContentLockPath,
    ) -> Option<Vec<u8>> {
        self.records
            .read()
            .await
            .get(&(creator.clone(), path.clone()))
            .cloned()
    }

    pub(crate) async fn remove(
        &self,
        creator: &CreatorPubky,
        path: &ContentLockPath,
    ) -> Option<Vec<u8>> {
        self.records
            .write()
            .await
            .remove(&(creator.clone(), path.clone()))
    }
}
