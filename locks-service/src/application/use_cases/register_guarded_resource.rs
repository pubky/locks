use locks_core::ids::{CreatorPubky, GuardedResourceHash};
use locks_core::lock_policy::GuardedResource;

use crate::application::errors::ApplicationError;
use crate::application::models::GuardedResourceRecord;
use crate::application::ports::GuardedResourceRepository;

/// Request to register local guarded resource bytes for creator publishing.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RegisterGuardedResourceRequest {
    /// Creator who owns the guarded resource path.
    pub creator: CreatorPubky,
    /// Creator-homeserver-relative guarded resource path.
    pub path: String,
    /// MIME content type for the guarded resource bytes.
    pub content_type: String,
    /// Guarded resource bytes to store locally.
    pub bytes: Vec<u8>,
}

/// Registered guarded resource descriptor returned to creator clients.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RegisteredGuardedResource {
    /// Creator who owns the registered guarded resource.
    pub creator: CreatorPubky,
    /// Descriptor for the stored guarded resource version.
    pub guarded_resource: GuardedResource,
}

/// Registers or replaces current local guarded resource bytes by creator/path.
pub struct RegisterGuardedResourceUseCase<'a> {
    guarded_resources: &'a dyn GuardedResourceRepository,
}

impl<'a> RegisterGuardedResourceUseCase<'a> {
    /// Creates a register-guarded-resource use case from its repository port.
    pub fn new(guarded_resources: &'a dyn GuardedResourceRepository) -> Self {
        Self { guarded_resources }
    }

    /// Registers guarded resource bytes and returns their descriptor.
    pub async fn execute(
        &self,
        request: RegisterGuardedResourceRequest,
    ) -> Result<RegisteredGuardedResource, ApplicationError> {
        let size = u64::try_from(request.bytes.len()).map_err(|_| {
            ApplicationError::InvalidGuardedResource {
                message: "guarded resource size exceeds u64".to_owned(),
            }
        })?;
        let hash = GuardedResourceHash::from_bytes(*blake3::hash(&request.bytes).as_bytes());
        let guarded_resource = GuardedResource::new(
            request.path.clone(),
            hash,
            request.content_type.clone(),
            size,
        )
        .map_err(|error| ApplicationError::InvalidGuardedResource {
            message: error.to_string(),
        })?;

        self.guarded_resources
            .upsert_guarded_resource(GuardedResourceRecord {
                creator: request.creator.clone(),
                path: guarded_resource.path.clone(),
                hash: guarded_resource.hash,
                content_type: guarded_resource.content_type.clone(),
                size: guarded_resource.size,
                bytes: request.bytes,
            })
            .await?;

        let stored = self
            .guarded_resources
            .get_guarded_resource(
                &request.creator,
                &guarded_resource.path,
                &guarded_resource.hash,
            )
            .await?
            .ok_or_else(|| ApplicationError::InvalidGuardedResource {
                message: "stored guarded resource did not match uploaded bytes".to_owned(),
            })?;
        let guarded_resource =
            GuardedResource::new(stored.path, stored.hash, stored.content_type, stored.size)
                .map_err(|error| ApplicationError::InvalidGuardedResource {
                    message: error.to_string(),
                })?;

        Ok(RegisteredGuardedResource {
            creator: request.creator,
            guarded_resource,
        })
    }
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use locks_core::ids::{CreatorPubky, GuardedResourceHash};

    use super::*;
    use crate::infrastructure::memory::guarded_resources::InMemoryGuardedResourceRepository;

    #[tokio::test]
    async fn register_guarded_resource_stores_bytes_and_returns_descriptor() {
        let repo = InMemoryGuardedResourceRepository::new();
        let use_case = RegisterGuardedResourceUseCase::new(&repo);
        let creator = creator();
        let bytes = b"hello guarded resource".to_vec();

        let result = use_case
            .execute(RegisterGuardedResourceRequest {
                creator: creator.clone(),
                path: "/priv/locks.app/content/hello.txt".to_owned(),
                content_type: "text/plain".to_owned(),
                bytes: bytes.clone(),
            })
            .await
            .unwrap();

        let expected_hash = GuardedResourceHash::from_bytes(*blake3::hash(&bytes).as_bytes());
        assert_eq!(result.creator, creator);
        assert_eq!(
            result.guarded_resource.path,
            "/priv/locks.app/content/hello.txt"
        );
        assert_eq!(result.guarded_resource.hash, expected_hash);
        assert_eq!(result.guarded_resource.content_type, "text/plain");
        assert_eq!(result.guarded_resource.size, bytes.len() as u64);

        let stored = repo
            .get_guarded_resource(
                &result.creator,
                &result.guarded_resource.path,
                &result.guarded_resource.hash,
            )
            .await
            .unwrap()
            .unwrap();
        assert_eq!(stored.creator, result.creator);
        assert_eq!(stored.path, result.guarded_resource.path);
        assert_eq!(stored.hash, result.guarded_resource.hash);
        assert_eq!(stored.content_type, result.guarded_resource.content_type);
        assert_eq!(stored.size, result.guarded_resource.size);
        assert_eq!(stored.bytes, bytes);
    }

    #[tokio::test]
    async fn register_guarded_resource_overwrites_current_path_and_old_hash_stops_reading() {
        let repo = InMemoryGuardedResourceRepository::new();
        let use_case = RegisterGuardedResourceUseCase::new(&repo);
        let creator = creator();

        let first = use_case
            .execute(RegisterGuardedResourceRequest {
                creator: creator.clone(),
                path: "/priv/locks.app/content/hello.txt".to_owned(),
                content_type: "text/plain".to_owned(),
                bytes: b"first bytes".to_vec(),
            })
            .await
            .unwrap();
        let second = use_case
            .execute(RegisterGuardedResourceRequest {
                creator: creator.clone(),
                path: "/priv/locks.app/content/hello.txt".to_owned(),
                content_type: "image/png".to_owned(),
                bytes: b"second bytes".to_vec(),
            })
            .await
            .unwrap();

        assert_ne!(first.guarded_resource.hash, second.guarded_resource.hash);
        assert_eq!(
            repo.get_guarded_resource(
                &creator,
                &first.guarded_resource.path,
                &first.guarded_resource.hash,
            )
            .await
            .unwrap(),
            None
        );
        let current = repo
            .get_current_guarded_resource(&creator, &second.guarded_resource.path)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(current.hash, second.guarded_resource.hash);
        assert_eq!(current.content_type, "image/png");
        assert_eq!(current.bytes, b"second bytes".to_vec());
    }

    #[tokio::test]
    async fn register_guarded_resource_rejects_invalid_mime_type() {
        let repo = InMemoryGuardedResourceRepository::new();
        let use_case = RegisterGuardedResourceUseCase::new(&repo);

        let result = use_case
            .execute(RegisterGuardedResourceRequest {
                creator: creator(),
                path: "/priv/locks.app/content/hello.txt".to_owned(),
                content_type: "not a mime".to_owned(),
                bytes: b"hello".to_vec(),
            })
            .await;

        assert!(matches!(
            result,
            Err(ApplicationError::InvalidGuardedResource { .. })
        ));
    }

    #[tokio::test]
    async fn register_guarded_resource_rejects_empty_bytes() {
        let repo = InMemoryGuardedResourceRepository::new();
        let use_case = RegisterGuardedResourceUseCase::new(&repo);

        let result = use_case
            .execute(RegisterGuardedResourceRequest {
                creator: creator(),
                path: "/priv/locks.app/content/empty.txt".to_owned(),
                content_type: "text/plain".to_owned(),
                bytes: Vec::new(),
            })
            .await;

        assert!(matches!(
            result,
            Err(ApplicationError::InvalidGuardedResource { .. })
        ));
    }

    fn creator() -> CreatorPubky {
        CreatorPubky::from_str("pubkytkrq8zmwb8a3m9k15csu3q17qmfgqnp9dskbrg9uq1rydpyxp7qy").unwrap()
    }
}
