use std::collections::BTreeMap;

use locks_core::ids::{ContentLockPath, CreatorPubky, LockId};
use locks_core::lock_policy::{
    AccessPolicy, CONTENT_LOCK_VERSION, ContentLock, Criterion, GuardedResource, LockLogic,
    LockServerConfig, SecondaryGuardedResource,
};
use uuid::Uuid;

use crate::application::errors::ApplicationError;
use crate::application::ports::{
    Clock, ContentLockDeletionRepository, ContentLockOwnershipRepository, ContentLockRepository,
    GuardedResourceRepository,
};

/// Request to create a local content lock for an already-registered guarded resource.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateContentLockRequest {
    /// Creator who owns the guarded resource and content lock.
    pub creator: CreatorPubky,
    /// Primary guarded resource descriptor the content lock will protect, if present.
    pub primary_resource: Option<GuardedResource>,
    /// Secondary guarded resources the content lock will protect, keyed by full guarded path.
    pub secondary_resources: BTreeMap<String, SecondaryGuardedResource>,
    /// Criteria required by the content lock.
    pub criteria: Vec<Criterion>,
    /// Logic over criterion IDs.
    pub lock_logic: LockLogic,
    /// Access credential policy requested by the creator.
    pub access_policy: AccessPolicy,
    /// Lock Server discovery settings for this lock.
    pub lock_server: LockServerConfig,
}

/// Created content lock plus its derived identifiers.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreatedContentLock {
    /// Lock ID derived from canonical content lock JSON.
    pub lock_id: LockId,
    /// Creator-relative public content lock path.
    pub content_lock_path: ContentLockPath,
    /// Stored content lock payload.
    pub content_lock: ContentLock,
}

/// Creates local content locks after verifying current guarded resource metadata.
pub struct CreateContentLockUseCase<'a> {
    content_locks: &'a dyn ContentLockRepository,
    content_lock_deletions: &'a dyn ContentLockDeletionRepository,
    content_lock_ownership: &'a dyn ContentLockOwnershipRepository,
    guarded_resources: &'a dyn GuardedResourceRepository,
    clock: &'a dyn Clock,
}

impl<'a> CreateContentLockUseCase<'a> {
    /// Creates a content-lock use case from its application ports.
    pub fn new(
        content_locks: &'a dyn ContentLockRepository,
        content_lock_deletions: &'a dyn ContentLockDeletionRepository,
        content_lock_ownership: &'a dyn ContentLockOwnershipRepository,
        guarded_resources: &'a dyn GuardedResourceRepository,
        clock: &'a dyn Clock,
    ) -> Self {
        Self {
            content_locks,
            content_lock_deletions,
            content_lock_ownership,
            guarded_resources,
            clock,
        }
    }

    /// Creates and stores a content lock for the current guarded resource descriptor.
    pub async fn execute(
        &self,
        request: CreateContentLockRequest,
    ) -> Result<CreatedContentLock, ApplicationError> {
        let content_lock = ContentLock {
            version: CONTENT_LOCK_VERSION,
            creator: request.creator.clone(),
            primary_resource: request.primary_resource,
            secondary_resources: request.secondary_resources,
            criteria: request.criteria,
            lock_logic: request.lock_logic,
            access_policy: request.access_policy,
            lock_server: request.lock_server,
            created_at: self.clock.now(),
        };
        content_lock.validate_resource_set().map_err(|error| {
            ApplicationError::InvalidGuardedResource {
                message: error.to_string(),
            }
        })?;
        validate_criterion_params(&content_lock)?;
        content_lock
            .validate_paykit_payment_v1_policy()
            .map_err(|error| ApplicationError::InvalidGuardedResource {
                message: error.to_string(),
            })?;

        for guarded_resource in resource_descriptors(&content_lock) {
            let current = self
                .guarded_resources
                .get_current_guarded_resource(&request.creator, &guarded_resource.path)
                .await?
                .ok_or(ApplicationError::GuardedResourceUnavailable)?;

            if current.hash != guarded_resource.hash
                || current.content_type != guarded_resource.content_type
                || current.size != guarded_resource.size
            {
                return Err(ApplicationError::InvalidGuardedResource {
                    message: "guarded resource descriptor does not match current stored resource"
                        .to_owned(),
                });
            }
        }
        let lock_id = content_lock.lock_id().map_err(|error| {
            ApplicationError::ContentLockCanonicalization {
                message: error.to_string(),
            }
        })?;
        let content_lock_path = content_lock.content_lock_path().map_err(|error| {
            ApplicationError::ContentLockCanonicalization {
                message: error.to_string(),
            }
        })?;
        let guarded_paths = resource_descriptors(&content_lock)
            .into_iter()
            .map(|resource| resource.path)
            .collect::<Vec<_>>();

        self.content_lock_ownership
            .reserve_paths(&request.creator, &guarded_paths, &lock_id)
            .await?;

        let publication_token = Uuid::new_v4();
        if let Err(error) = self
            .content_lock_deletions
            .begin_publication(&request.creator, &lock_id, publication_token)
            .await
        {
            let _ = self
                .content_lock_ownership
                .compensate_reserved_paths(&request.creator, &guarded_paths, &lock_id)
                .await;
            return Err(error);
        }

        if let Err(error) = self
            .content_locks
            .upsert_content_lock(
                request.creator.clone(),
                content_lock_path.clone(),
                content_lock.clone(),
            )
            .await
        {
            match self
                .content_locks
                .get_content_lock(&request.creator, &content_lock_path)
                .await
            {
                Ok(Some(published)) if published == content_lock => {
                    if self
                        .content_lock_ownership
                        .mark_paths_published(&request.creator, &guarded_paths, &lock_id)
                        .await
                        .is_ok()
                    {
                        let _ = self
                            .content_lock_deletions
                            .finish_publication(&request.creator, &lock_id, publication_token)
                            .await;
                    }
                }
                Ok(None) => {
                    if self
                        .content_lock_ownership
                        .compensate_reserved_paths(&request.creator, &guarded_paths, &lock_id)
                        .await
                        .is_ok()
                    {
                        let _ = self
                            .content_lock_deletions
                            .abandon_publication(&request.creator, &lock_id, publication_token)
                            .await;
                    }
                }
                Ok(Some(_)) | Err(_) => {}
            }
            return Err(error);
        }

        self.content_lock_ownership
            .mark_paths_published(&request.creator, &guarded_paths, &lock_id)
            .await?;
        if !self
            .content_lock_deletions
            .finish_publication(&request.creator, &lock_id, publication_token)
            .await?
        {
            return Err(ApplicationError::Storage {
                message: "content lock publication intent was lost".to_owned(),
            });
        }

        Ok(CreatedContentLock {
            lock_id,
            content_lock_path,
            content_lock,
        })
    }
}

fn validate_criterion_params(content_lock: &ContentLock) -> Result<(), ApplicationError> {
    for criterion in &content_lock.criteria {
        criterion
            .validate_params()
            .map_err(|error| ApplicationError::InvalidGuardedResource {
                message: format!(
                    "invalid params for {} criterion {}: {error}",
                    criterion.verifier_type, criterion.criterion_id
                ),
            })?;
    }
    Ok(())
}

fn resource_descriptors(content_lock: &ContentLock) -> Vec<GuardedResource> {
    let mut resources = Vec::with_capacity(content_lock.resource_count());
    if let Some(primary_resource) = &content_lock.primary_resource {
        resources.push(primary_resource.clone());
    }
    resources.extend(
        content_lock
            .secondary_resources
            .iter()
            .map(|(path, secondary)| GuardedResource {
                path: path.clone(),
                hash: secondary.hash,
                content_type: secondary.content_type.clone(),
                size: secondary.size,
            }),
    );
    resources
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use async_trait::async_trait;
    use serde_json::json;
    use time::OffsetDateTime;
    use time::macros::datetime;

    use locks_core::ids::{CreatorPubky, GuardedResourceHash, LockServerPubky};
    use locks_core::lock_policy::VerifierType;

    use super::*;
    use crate::application::models::{
        ContentLockOwnershipStatus, GuardedResourceRecord, PrepareForceDeletionResult,
    };
    use crate::application::ports::{
        Clock, ContentLockDeletionRepository, ContentLockOwnershipRepository,
        ContentLockRepository, GuardedResourceRepository,
    };
    use crate::infrastructure::memory::content_lock_deletions::InMemoryContentLockDeletionRepository;
    use crate::infrastructure::memory::content_lock_ownership::InMemoryContentLockOwnershipRepository;
    use crate::infrastructure::memory::content_locks::InMemoryContentLockRepository;
    use crate::infrastructure::memory::guarded_resources::InMemoryGuardedResourceRepository;

    #[tokio::test]
    async fn create_content_lock_stores_payload_under_derived_path() {
        let fixture = Fixture::seeded().await;
        let use_case = fixture.use_case();
        let request = content_lock_request(registered_guarded_resource());

        let result = use_case.execute(request.clone()).await.unwrap();

        assert_eq!(result.content_lock.version, CONTENT_LOCK_VERSION);
        assert_eq!(result.content_lock.creator, creator());
        assert_eq!(
            result.content_lock.primary_resource,
            request.primary_resource
        );
        assert_eq!(
            result.content_lock.secondary_resources,
            request.secondary_resources
        );
        assert_eq!(result.content_lock.criteria, request.criteria);
        assert_eq!(result.content_lock.lock_logic, request.lock_logic);
        assert_eq!(result.content_lock.access_policy, request.access_policy);
        assert_eq!(result.content_lock.lock_server, request.lock_server);
        assert_eq!(result.content_lock.created_at, fixture.clock.now());
        assert_eq!(result.lock_id, result.content_lock.lock_id().unwrap());
        assert_eq!(
            result.content_lock_path,
            result.content_lock.content_lock_path().unwrap()
        );
        assert_eq!(result.content_lock_path.lock_id(), &result.lock_id);
        assert_eq!(
            fixture
                .content_locks
                .get_content_lock(&creator(), &result.content_lock_path)
                .await
                .unwrap(),
            Some(result.content_lock)
        );
        let ownership = fixture
            .content_lock_ownership
            .get_path_ownership(&creator(), "/priv/locks.app/content/hello.txt")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(ownership.lock_id, result.lock_id);
        assert_eq!(ownership.status.as_str(), "published");
    }

    #[tokio::test]
    async fn permanent_force_receipt_blocks_canonical_lock_republication() {
        let fixture = Fixture::seeded().await;
        let request = content_lock_request(registered_guarded_resource());
        let content_lock = ContentLock {
            version: CONTENT_LOCK_VERSION,
            creator: request.creator.clone(),
            primary_resource: request.primary_resource.clone(),
            secondary_resources: request.secondary_resources.clone(),
            criteria: request.criteria.clone(),
            lock_logic: request.lock_logic.clone(),
            access_policy: request.access_policy.clone(),
            lock_server: request.lock_server.clone(),
            created_at: fixture.clock.now(),
        };
        let lock_id = content_lock.lock_id().unwrap();
        fixture
            .content_lock_deletions
            .prepare_force_deletion(&request.creator, &lock_id, fixture.clock.now())
            .await
            .unwrap();

        let result = fixture.use_case().execute(request).await;

        assert_eq!(result, Err(ApplicationError::ContentLockDeletionInProgress));
        assert_eq!(fixture.content_locks_len().await, 0);
        assert_eq!(
            fixture
                .content_lock_ownership
                .get_path_ownership(&creator(), "/priv/locks.app/content/hello.txt")
                .await
                .unwrap(),
            None
        );
    }

    #[tokio::test]
    async fn create_content_lock_rejects_missing_guarded_resource() {
        let fixture = Fixture::empty();
        let use_case = fixture.use_case();

        let result = use_case
            .execute(content_lock_request(registered_guarded_resource()))
            .await;

        assert_eq!(result, Err(ApplicationError::GuardedResourceUnavailable));
        assert_eq!(fixture.content_locks_len().await, 0);
    }

    #[tokio::test]
    async fn create_content_lock_rejects_stale_guarded_resource_hash() {
        let fixture = Fixture::seeded().await;
        let use_case = fixture.use_case();
        let mut guarded_resource = registered_guarded_resource();
        guarded_resource.hash = GuardedResourceHash::from_bytes([9; 32]);

        let result = use_case
            .execute(content_lock_request(guarded_resource))
            .await;

        assert!(matches!(
            result,
            Err(ApplicationError::InvalidGuardedResource { .. })
        ));
        assert_eq!(fixture.content_locks_len().await, 0);
    }

    #[tokio::test]
    async fn create_content_lock_rejects_stale_guarded_resource_content_type() {
        let fixture = Fixture::seeded().await;
        let use_case = fixture.use_case();
        let mut guarded_resource = registered_guarded_resource();
        guarded_resource.content_type = "image/png".to_owned();

        let result = use_case
            .execute(content_lock_request(guarded_resource))
            .await;

        assert!(matches!(
            result,
            Err(ApplicationError::InvalidGuardedResource { .. })
        ));
        assert_eq!(fixture.content_locks_len().await, 0);
    }

    #[tokio::test]
    async fn create_content_lock_rejects_stale_guarded_resource_size() {
        let fixture = Fixture::seeded().await;
        let use_case = fixture.use_case();
        let mut guarded_resource = registered_guarded_resource();
        guarded_resource.size += 1;

        let result = use_case
            .execute(content_lock_request(guarded_resource))
            .await;

        assert!(matches!(
            result,
            Err(ApplicationError::InvalidGuardedResource { .. })
        ));
        assert_eq!(fixture.content_locks_len().await, 0);
    }

    #[tokio::test]
    async fn identical_create_returns_same_lock_id_and_path() {
        let fixture = Fixture::seeded().await;
        let use_case = fixture.use_case();
        let request = content_lock_request(registered_guarded_resource());

        let first = use_case.execute(request.clone()).await.unwrap();
        let second = use_case.execute(request).await.unwrap();

        assert_eq!(second.lock_id, first.lock_id);
        assert_eq!(second.content_lock_path, first.content_lock_path);
        assert_eq!(second.content_lock, first.content_lock);
        assert_eq!(fixture.content_locks_len().await, 1);
    }

    #[tokio::test]
    async fn changed_criteria_rejects_path_owned_by_different_lock() {
        let fixture = Fixture::seeded().await;
        let use_case = fixture.use_case();
        let first_request = content_lock_request(registered_guarded_resource());
        let mut second_request = first_request.clone();
        second_request.criteria[0].params = json!({ "satisfied": false });

        let first = use_case.execute(first_request).await.unwrap();
        let second = use_case.execute(second_request).await;

        assert!(matches!(
            second,
            Err(ApplicationError::ContentLockPathConflict { ref guarded_path })
                if guarded_path == "/priv/locks.app/content/hello.txt"
        ));
        assert_eq!(fixture.content_locks_len().await, 1);
        let ownership = fixture
            .content_lock_ownership
            .get_path_ownership(&creator(), "/priv/locks.app/content/hello.txt")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(ownership.lock_id, first.lock_id);
    }

    #[tokio::test]
    async fn create_content_lock_rejects_invalid_paykit_payment_params() {
        let fixture = Fixture::seeded().await;
        let use_case = fixture.use_case();
        let mut request = content_lock_request(registered_guarded_resource());
        request.criteria[0].verifier_type = VerifierType::PaykitPayment;
        request.criteria[0].params = json!({
            "recipient_pubky": creator().to_string(),
            "amount": "0",
            "asset": "BTC",
            "payment_in": 24,
        });

        let result = use_case.execute(request).await;

        assert!(matches!(
            result,
            Err(ApplicationError::InvalidGuardedResource { message })
                if message.contains("paykit-payment") && message.contains("amount")
        ));
        assert_eq!(fixture.content_locks_len().await, 0);
    }

    #[tokio::test]
    async fn create_content_lock_rejects_mixed_paykit_payment_policy() {
        let fixture = Fixture::seeded().await;
        let use_case = fixture.use_case();
        let mut request = content_lock_request(registered_guarded_resource());
        request.criteria.push(Criterion {
            criterion_id: "payment".to_owned(),
            verifier_type: VerifierType::PaykitPayment,
            params: json!({
                "recipient_pubky": creator().to_string(),
                "amount": "50000",
                "asset": "BTC",
                "payment_in": 24
            }),
        });
        request.lock_logic = LockLogic::All {
            criteria: vec!["criterion-1".to_owned(), "payment".to_owned()],
        };

        let result = use_case.execute(request).await;

        assert!(matches!(
            result,
            Err(ApplicationError::InvalidGuardedResource { .. })
        ));
        assert_eq!(fixture.content_locks_len().await, 0);
    }

    #[tokio::test]
    async fn publication_failure_compensates_reserved_path_ownership() {
        let fixture = Fixture::seeded().await;
        let use_case = CreateContentLockUseCase::new(
            &FailingContentLockRepository,
            &fixture.content_lock_deletions,
            &fixture.content_lock_ownership,
            &fixture.guarded_resources,
            &fixture.clock,
        );

        let result = use_case
            .execute(content_lock_request(registered_guarded_resource()))
            .await;

        assert!(matches!(
            result,
            Err(ApplicationError::Storage { ref message }) if message == "publication failed"
        ));
        assert_eq!(
            fixture
                .content_lock_ownership
                .get_path_ownership(&creator(), "/priv/locks.app/content/hello.txt")
                .await
                .unwrap(),
            None
        );
    }

    #[tokio::test]
    async fn ambiguous_publication_error_reconciles_committed_lock_without_releasing_ownership() {
        let fixture = Fixture::seeded().await;
        let content_locks = AmbiguousContentLockRepository::default();
        let use_case = CreateContentLockUseCase::new(
            &content_locks,
            &fixture.content_lock_deletions,
            &fixture.content_lock_ownership,
            &fixture.guarded_resources,
            &fixture.clock,
        );
        let request = content_lock_request(registered_guarded_resource());
        let expected = ContentLock {
            version: CONTENT_LOCK_VERSION,
            creator: request.creator.clone(),
            primary_resource: request.primary_resource.clone(),
            secondary_resources: request.secondary_resources.clone(),
            criteria: request.criteria.clone(),
            lock_logic: request.lock_logic.clone(),
            access_policy: request.access_policy.clone(),
            lock_server: request.lock_server.clone(),
            created_at: fixture.clock.now(),
        };
        let lock_id = expected.lock_id().unwrap();
        let path = expected.content_lock_path().unwrap();

        let result = use_case.execute(request).await;

        assert!(matches!(
            result,
            Err(ApplicationError::Storage { ref message }) if message == "publication response lost"
        ));
        assert_eq!(
            content_locks
                .get_content_lock(&creator(), &path)
                .await
                .unwrap(),
            Some(expected)
        );
        assert_eq!(
            fixture
                .content_lock_ownership
                .get_path_ownership(&creator(), "/priv/locks.app/content/hello.txt")
                .await
                .unwrap()
                .unwrap()
                .status,
            ContentLockOwnershipStatus::Published
        );
        assert!(
            !fixture
                .content_lock_deletions
                .publication_in_progress(&creator(), &lock_id)
                .await
                .unwrap()
        );
    }

    #[tokio::test]
    async fn unreconciled_publication_error_retains_reserved_ownership_and_deletion_fence() {
        let fixture = Fixture::seeded().await;
        let content_locks = UnreconciledContentLockRepository;
        let use_case = CreateContentLockUseCase::new(
            &content_locks,
            &fixture.content_lock_deletions,
            &fixture.content_lock_ownership,
            &fixture.guarded_resources,
            &fixture.clock,
        );
        let request = content_lock_request(registered_guarded_resource());
        let lock_id = ContentLock {
            version: CONTENT_LOCK_VERSION,
            creator: request.creator.clone(),
            primary_resource: request.primary_resource.clone(),
            secondary_resources: request.secondary_resources.clone(),
            criteria: request.criteria.clone(),
            lock_logic: request.lock_logic.clone(),
            access_policy: request.access_policy.clone(),
            lock_server: request.lock_server.clone(),
            created_at: fixture.clock.now(),
        }
        .lock_id()
        .unwrap();

        let result = use_case.execute(request).await;

        assert!(matches!(
            result,
            Err(ApplicationError::Storage { ref message }) if message == "publication response lost"
        ));
        assert_eq!(
            fixture
                .content_lock_ownership
                .get_path_ownership(&creator(), "/priv/locks.app/content/hello.txt")
                .await
                .unwrap()
                .unwrap()
                .status,
            ContentLockOwnershipStatus::Reserved
        );
        assert!(
            fixture
                .content_lock_deletions
                .publication_in_progress(&creator(), &lock_id)
                .await
                .unwrap()
        );
        assert_eq!(
            fixture
                .content_lock_deletions
                .prepare_force_deletion(&creator(), &lock_id, fixture.clock.now())
                .await
                .unwrap(),
            PrepareForceDeletionResult::PublicationInProgress
        );
    }

    #[tokio::test]
    async fn publication_intent_fences_force_during_external_upsert() {
        let fixture = Fixture::seeded().await;
        let probe = PublicationRaceProbe {
            deletions: &fixture.content_lock_deletions,
            now: fixture.clock.now(),
        };
        let use_case = CreateContentLockUseCase::new(
            &probe,
            &fixture.content_lock_deletions,
            &fixture.content_lock_ownership,
            &fixture.guarded_resources,
            &fixture.clock,
        );

        let created = use_case
            .execute(content_lock_request(registered_guarded_resource()))
            .await
            .unwrap();

        assert_eq!(
            fixture
                .content_lock_deletions
                .prepare_force_deletion(&creator(), &created.lock_id, fixture.clock.now())
                .await
                .unwrap(),
            PrepareForceDeletionResult::Synchronous(None)
        );
    }

    struct PublicationRaceProbe<'a> {
        deletions: &'a InMemoryContentLockDeletionRepository,
        now: OffsetDateTime,
    }

    #[async_trait]
    impl ContentLockRepository for PublicationRaceProbe<'_> {
        async fn upsert_content_lock(
            &self,
            creator: CreatorPubky,
            _path: ContentLockPath,
            content_lock: ContentLock,
        ) -> Result<(), ApplicationError> {
            let lock_id = content_lock.lock_id().unwrap();
            assert_eq!(
                self.deletions
                    .prepare_force_deletion(&creator, &lock_id, self.now)
                    .await?,
                PrepareForceDeletionResult::PublicationInProgress
            );
            assert!(!self.deletions.has_force_receipt(&creator, &lock_id).await?);
            Ok(())
        }

        async fn get_content_lock(
            &self,
            _creator: &CreatorPubky,
            _path: &ContentLockPath,
        ) -> Result<Option<ContentLock>, ApplicationError> {
            Ok(None)
        }

        async fn delete_content_lock(
            &self,
            _creator: &CreatorPubky,
            _path: &ContentLockPath,
        ) -> Result<bool, ApplicationError> {
            unreachable!("creation must not delete content locks")
        }
    }

    #[derive(Default)]
    struct AmbiguousContentLockRepository {
        published: tokio::sync::RwLock<Option<(CreatorPubky, ContentLockPath, ContentLock)>>,
    }

    #[async_trait]
    impl ContentLockRepository for AmbiguousContentLockRepository {
        async fn upsert_content_lock(
            &self,
            creator: CreatorPubky,
            path: ContentLockPath,
            content_lock: ContentLock,
        ) -> Result<(), ApplicationError> {
            *self.published.write().await = Some((creator, path, content_lock));
            Err(ApplicationError::Storage {
                message: "publication response lost".to_owned(),
            })
        }

        async fn get_content_lock(
            &self,
            creator: &CreatorPubky,
            path: &ContentLockPath,
        ) -> Result<Option<ContentLock>, ApplicationError> {
            Ok(self
                .published
                .read()
                .await
                .as_ref()
                .filter(|(stored_creator, stored_path, _)| {
                    stored_creator == creator && stored_path == path
                })
                .map(|(_, _, content_lock)| content_lock.clone()))
        }

        async fn delete_content_lock(
            &self,
            _creator: &CreatorPubky,
            _path: &ContentLockPath,
        ) -> Result<bool, ApplicationError> {
            unreachable!("creation must not delete content locks")
        }
    }

    struct UnreconciledContentLockRepository;

    #[async_trait]
    impl ContentLockRepository for UnreconciledContentLockRepository {
        async fn upsert_content_lock(
            &self,
            _creator: CreatorPubky,
            _path: ContentLockPath,
            _content_lock: ContentLock,
        ) -> Result<(), ApplicationError> {
            Err(ApplicationError::Storage {
                message: "publication response lost".to_owned(),
            })
        }

        async fn get_content_lock(
            &self,
            _creator: &CreatorPubky,
            _path: &ContentLockPath,
        ) -> Result<Option<ContentLock>, ApplicationError> {
            Err(ApplicationError::Storage {
                message: "publication reconciliation failed".to_owned(),
            })
        }

        async fn delete_content_lock(
            &self,
            _creator: &CreatorPubky,
            _path: &ContentLockPath,
        ) -> Result<bool, ApplicationError> {
            unreachable!("creation must not delete content locks")
        }
    }

    struct FailingContentLockRepository;

    #[async_trait]
    impl ContentLockRepository for FailingContentLockRepository {
        async fn upsert_content_lock(
            &self,
            _creator: CreatorPubky,
            _path: ContentLockPath,
            _content_lock: ContentLock,
        ) -> Result<(), ApplicationError> {
            Err(ApplicationError::Storage {
                message: "publication failed".to_owned(),
            })
        }

        async fn get_content_lock(
            &self,
            _creator: &CreatorPubky,
            _path: &ContentLockPath,
        ) -> Result<Option<ContentLock>, ApplicationError> {
            Ok(None)
        }

        async fn delete_content_lock(
            &self,
            _creator: &CreatorPubky,
            _path: &ContentLockPath,
        ) -> Result<bool, ApplicationError> {
            unreachable!("creation must not delete content locks")
        }
    }

    struct Fixture {
        content_locks: InMemoryContentLockRepository,
        content_lock_deletions: InMemoryContentLockDeletionRepository,
        content_lock_ownership: InMemoryContentLockOwnershipRepository,
        guarded_resources: InMemoryGuardedResourceRepository,
        clock: FixedClock,
    }

    impl Fixture {
        fn empty() -> Self {
            Self {
                content_locks: InMemoryContentLockRepository::new(),
                content_lock_deletions: InMemoryContentLockDeletionRepository::new(),
                content_lock_ownership: InMemoryContentLockOwnershipRepository::new(),
                guarded_resources: InMemoryGuardedResourceRepository::new(),
                clock: FixedClock(datetime!(2026-06-03 12:00:00 UTC)),
            }
        }

        async fn seeded() -> Self {
            let fixture = Self::empty();
            let guarded_resource = registered_guarded_resource();
            fixture
                .guarded_resources
                .upsert_guarded_resource(GuardedResourceRecord {
                    creator: creator(),
                    path: guarded_resource.path.clone(),
                    hash: guarded_resource.hash,
                    content_type: guarded_resource.content_type.clone(),
                    size: guarded_resource.size,
                    bytes: b"guarded bytes".to_vec(),
                })
                .await
                .unwrap();
            fixture
        }

        fn use_case(&self) -> CreateContentLockUseCase<'_> {
            CreateContentLockUseCase::new(
                &self.content_locks,
                &self.content_lock_deletions,
                &self.content_lock_ownership,
                &self.guarded_resources,
                &self.clock,
            )
        }

        async fn content_locks_len(&self) -> usize {
            let request = content_lock_request(registered_guarded_resource());
            let maybe_path = ContentLock {
                version: CONTENT_LOCK_VERSION,
                creator: request.creator,
                primary_resource: request.primary_resource,
                secondary_resources: request.secondary_resources,
                criteria: request.criteria,
                lock_logic: request.lock_logic,
                access_policy: request.access_policy,
                lock_server: request.lock_server,
                created_at: self.clock.now(),
            }
            .content_lock_path()
            .unwrap();
            let first = self
                .content_locks
                .get_content_lock(&creator(), &maybe_path)
                .await
                .unwrap();
            let changed = content_lock_request_with_changed_criteria();
            let maybe_changed_path = ContentLock {
                version: CONTENT_LOCK_VERSION,
                creator: changed.creator,
                primary_resource: changed.primary_resource,
                secondary_resources: changed.secondary_resources,
                criteria: changed.criteria,
                lock_logic: changed.lock_logic,
                access_policy: changed.access_policy,
                lock_server: changed.lock_server,
                created_at: self.clock.now(),
            }
            .content_lock_path()
            .unwrap();
            let second = self
                .content_locks
                .get_content_lock(&creator(), &maybe_changed_path)
                .await
                .unwrap();
            usize::from(first.is_some()) + usize::from(second.is_some())
        }
    }

    fn content_lock_request(guarded_resource: GuardedResource) -> CreateContentLockRequest {
        CreateContentLockRequest {
            creator: creator(),
            primary_resource: Some(guarded_resource),
            secondary_resources: BTreeMap::new(),
            criteria: vec![Criterion {
                criterion_id: "criterion-1".to_owned(),
                verifier_type: VerifierType::DevStatic,
                params: json!({ "satisfied": true }),
            }],
            lock_logic: LockLogic::All {
                criteria: vec!["criterion-1".to_owned()],
            },
            access_policy: AccessPolicy {
                requested_credential_ttl_seconds: 900,
            },
            lock_server: LockServerConfig {
                override_: Some(server()),
            },
        }
    }

    fn content_lock_request_with_changed_criteria() -> CreateContentLockRequest {
        let mut request = content_lock_request(registered_guarded_resource());
        request.criteria[0].params = json!({ "satisfied": false });
        request
    }

    fn registered_guarded_resource() -> GuardedResource {
        GuardedResource {
            path: "/priv/locks.app/content/hello.txt".to_owned(),
            hash: GuardedResourceHash::from_bytes(*blake3::hash(b"guarded bytes").as_bytes()),
            content_type: "text/plain".to_owned(),
            size: 13,
        }
    }

    fn creator() -> CreatorPubky {
        CreatorPubky::from_str("pubkytkrq8zmwb8a3m9k15csu3q17qmfgqnp9dskbrg9uq1rydpyxp7qy").unwrap()
    }

    fn server() -> LockServerPubky {
        LockServerPubky::from_str("pubky7ir1ttte48bcp4zjychjyscicrwi1j34mtt91ptsafdbjmr8g9eo")
            .unwrap()
    }

    struct FixedClock(OffsetDateTime);

    impl Clock for FixedClock {
        fn now(&self) -> OffsetDateTime {
            self.0
        }
    }
}
