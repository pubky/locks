use locks_core::ids::CreatorPubky;

use crate::application::errors::ApplicationError;
use crate::application::ports::GuardedResourceRepository;

/// Request to delete/unpublish the current guarded resource at a full private path.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeleteGuardedResourceRequest {
    pub creator: CreatorPubky,
    pub path: String,
}

/// Deletes/unpublishes the latest-state guarded resource for a creator/path.
pub struct DeleteGuardedResourceUseCase<'a> {
    guarded_resources: &'a dyn GuardedResourceRepository,
}

impl<'a> DeleteGuardedResourceUseCase<'a> {
    pub fn new(guarded_resources: &'a dyn GuardedResourceRepository) -> Self {
        Self { guarded_resources }
    }

    pub async fn execute(
        &self,
        request: DeleteGuardedResourceRequest,
    ) -> Result<(), ApplicationError> {
        if self
            .guarded_resources
            .delete_guarded_resource(&request.creator, &request.path)
            .await?
        {
            Ok(())
        } else {
            Err(ApplicationError::GuardedResourceUnavailable)
        }
    }
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use locks_core::ids::GuardedResourceHash;

    use super::*;
    use crate::application::models::GuardedResourceRecord;
    use crate::infrastructure::memory::guarded_resources::InMemoryGuardedResourceRepository;

    #[tokio::test]
    async fn delete_guarded_resource_deletes_existing_current_resource() {
        let repository = InMemoryGuardedResourceRepository::new();
        let creator = creator();
        repository
            .upsert_guarded_resource(GuardedResourceRecord {
                creator: creator.clone(),
                path: "/priv/locks.app/content/delete-me.txt".to_owned(),
                hash: GuardedResourceHash::from_bytes([7; 32]),
                content_type: "text/plain".to_owned(),
                size: 5,
                bytes: b"first".to_vec(),
            })
            .await
            .unwrap();

        DeleteGuardedResourceUseCase::new(&repository)
            .execute(DeleteGuardedResourceRequest {
                creator: creator.clone(),
                path: "/priv/locks.app/content/delete-me.txt".to_owned(),
            })
            .await
            .unwrap();

        assert_eq!(
            repository
                .get_current_guarded_resource(&creator, "/priv/locks.app/content/delete-me.txt")
                .await
                .unwrap(),
            None
        );
    }

    #[tokio::test]
    async fn delete_guarded_resource_returns_unavailable_for_missing_resource() {
        let repository = InMemoryGuardedResourceRepository::new();

        let error = DeleteGuardedResourceUseCase::new(&repository)
            .execute(DeleteGuardedResourceRequest {
                creator: creator(),
                path: "/priv/locks.app/content/missing.txt".to_owned(),
            })
            .await
            .unwrap_err();

        assert_eq!(error, ApplicationError::GuardedResourceUnavailable);
    }

    fn creator() -> CreatorPubky {
        CreatorPubky::from_str("pubkytkrq8zmwb8a3m9k15csu3q17qmfgqnp9dskbrg9uq1rydpyxp7qy").unwrap()
    }
}
