use std::{
    collections::HashSet,
    sync::{Arc, Mutex},
};

use async_trait::async_trait;
use uuid::Uuid;

use crate::{
    application::{
        errors::ApplicationError,
        ports::{
            ContentLockDeletionActionAcquireResult, ContentLockDeletionActionClaim,
            ContentLockDeletionActionGuard, ContentLockDeletionActionOwnership,
        },
    },
    infrastructure::memory::content_lock_deletions::InMemoryContentLockDeletionRepository,
};

/// Process-local parity adapter for per-job external action ownership.
#[derive(Debug, Clone)]
pub struct InMemoryContentLockDeletionActionOwnership {
    deletions: Arc<InMemoryContentLockDeletionRepository>,
    owned_jobs: Arc<Mutex<HashSet<Uuid>>>,
}

impl InMemoryContentLockDeletionActionOwnership {
    pub fn new(deletions: Arc<InMemoryContentLockDeletionRepository>) -> Self {
        Self {
            deletions,
            owned_jobs: Arc::new(Mutex::new(HashSet::new())),
        }
    }
}

#[async_trait]
impl ContentLockDeletionActionOwnership for InMemoryContentLockDeletionActionOwnership {
    async fn try_acquire(
        &self,
        claim: ContentLockDeletionActionClaim<'_>,
    ) -> Result<ContentLockDeletionActionAcquireResult, ApplicationError> {
        {
            let mut owned_jobs = self
                .owned_jobs
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if !owned_jobs.insert(claim.job_id) {
                return Ok(ContentLockDeletionActionAcquireResult::Busy);
            }
        }

        if !self.deletions.action_claim_is_live(claim).await {
            self.owned_jobs
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .remove(&claim.job_id);
            return Ok(ContentLockDeletionActionAcquireResult::ClaimLost);
        }

        Ok(ContentLockDeletionActionAcquireResult::Acquired(Box::new(
            InMemoryContentLockDeletionActionGuard {
                job_id: claim.job_id,
                owned_jobs: Arc::clone(&self.owned_jobs),
                released: false,
            },
        )))
    }
}

struct InMemoryContentLockDeletionActionGuard {
    job_id: Uuid,
    owned_jobs: Arc<Mutex<HashSet<Uuid>>>,
    released: bool,
}

impl InMemoryContentLockDeletionActionGuard {
    fn release_inner(&mut self) {
        if self.released {
            return;
        }
        self.owned_jobs
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .remove(&self.job_id);
        self.released = true;
    }
}

#[async_trait]
impl ContentLockDeletionActionGuard for InMemoryContentLockDeletionActionGuard {
    async fn release(mut self: Box<Self>) -> Result<(), ApplicationError> {
        self.release_inner();
        Ok(())
    }
}

impl Drop for InMemoryContentLockDeletionActionGuard {
    fn drop(&mut self) {
        self.release_inner();
    }
}
