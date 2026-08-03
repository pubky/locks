use std::collections::HashMap;

use async_trait::async_trait;
use locks_core::ids::CreatorPubky;
use locks_core::lock_service_pointer::LockServicePointer;
use tokio::sync::RwLock;

use crate::application::errors::ApplicationError;
use crate::application::ports::LockServicePointerRepository;

#[derive(Debug, Default)]
pub struct InMemoryLockServicePointerRepository {
    pointers: RwLock<HashMap<CreatorPubky, LockServicePointer>>,
}

impl InMemoryLockServicePointerRepository {
    pub fn new() -> Self {
        Self::default()
    }
}

#[async_trait]
impl LockServicePointerRepository for InMemoryLockServicePointerRepository {
    async fn upsert_lock_service_pointer(
        &self,
        creator: CreatorPubky,
        pointer: LockServicePointer,
    ) -> Result<(), ApplicationError> {
        self.pointers.write().await.insert(creator, pointer);
        Ok(())
    }

    async fn get_lock_service_pointer(
        &self,
        creator: &CreatorPubky,
    ) -> Result<Option<LockServicePointer>, ApplicationError> {
        Ok(self.pointers.read().await.get(creator).cloned())
    }
}
