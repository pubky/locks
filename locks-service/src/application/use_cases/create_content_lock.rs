use std::collections::BTreeMap;

use locks_core::ids::{ContentLockPath, CreatorPubky, LockId};
use locks_core::lock_policy::{
    AccessPolicy, CONTENT_LOCK_VERSION, ContentLock, Criterion, GuardedResource, LockLogic,
    LockServerConfig, SecondaryGuardedResource,
};
use uuid::Uuid;

use crate::application::errors::ApplicationError;
use crate::application::models::ContentLockOwnershipStatus;
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
        let mut content_lock = ContentLock {
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
        let guarded_paths = resource_descriptors(&content_lock)
            .into_iter()
            .map(|resource| resource.path)
            .collect::<Vec<_>>();
        let mut already_published = false;
        if let Some((existing, ownership_status)) = self
            .resolve_owned_content_lock(&request.creator, &guarded_paths, &content_lock)
            .await?
        {
            content_lock = existing;
            already_published = ownership_status == ContentLockOwnershipStatus::Published;
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
        if already_published {
            // Reconciliation additionally requires the token owner to have durably recorded that
            // its external write returned. An exact replay never opens a new intent or republishes.
            self.content_lock_deletions
                .reconcile_publication(&request.creator, &lock_id, &content_lock, &guarded_paths)
                .await?;
            return Ok(CreatedContentLock {
                lock_id,
                content_lock_path,
                content_lock,
            });
        }
        if self
            .content_lock_deletions
            .publication_in_progress(&request.creator, &lock_id)
            .await?
        {
            let published_content_lock = self
                .content_locks
                .get_content_lock(&request.creator, &content_lock_path)
                .await?;
            let Some(published_content_lock) = published_content_lock.as_ref() else {
                return Err(ApplicationError::ContentLockPathConflict {
                    guarded_path: content_lock_path.to_string(),
                });
            };
            if published_content_lock != &content_lock {
                return Err(ApplicationError::ContentLockPathConflict {
                    guarded_path: content_lock_path.to_string(),
                });
            }
            self.content_lock_ownership
                .mark_paths_published(&request.creator, &guarded_paths, &lock_id)
                .await?;
            let reconciled = self
                .content_lock_deletions
                .reconcile_publication(
                    &request.creator,
                    &lock_id,
                    published_content_lock,
                    &guarded_paths,
                )
                .await?;
            if !reconciled
                && self
                    .content_lock_deletions
                    .publication_in_progress(&request.creator, &lock_id)
                    .await?
            {
                return Err(ApplicationError::Storage {
                    message: "content lock publication reconciliation was not committed".to_owned(),
                });
            }
            return Ok(CreatedContentLock {
                lock_id,
                content_lock_path,
                content_lock,
            });
        }

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

        // Any returned external-write error is ambiguous: an identical payload may predate this
        // attempt, while this PUT may still commit later. `?` deliberately retains reservation
        // and intent so deletion stays fenced until completion is proved out of band.
        self.content_locks
            .upsert_content_lock(
                request.creator.clone(),
                content_lock_path.clone(),
                content_lock.clone(),
            )
            .await?;

        if !self
            .content_lock_deletions
            .mark_publication_written(&request.creator, &lock_id, publication_token)
            .await?
        {
            return Err(ApplicationError::Storage {
                message: "content lock publication write completion was not committed".to_owned(),
            });
        }

        self.content_lock_ownership
            .mark_paths_published(&request.creator, &guarded_paths, &lock_id)
            .await?;
        // An exact replay may have reconciled this intent after the PUT and ownership commit.
        // Either outcome leaves this publisher's canonical write durably owned.
        self.content_lock_deletions
            .finish_publication(&request.creator, &lock_id, publication_token)
            .await?;

        Ok(CreatedContentLock {
            lock_id,
            content_lock_path,
            content_lock,
        })
    }

    async fn resolve_owned_content_lock(
        &self,
        creator: &CreatorPubky,
        guarded_paths: &[String],
        requested: &ContentLock,
    ) -> Result<Option<(ContentLock, ContentLockOwnershipStatus)>, ApplicationError> {
        // Reuse an earlier `created_at` only when every requested path has the
        // same owner and the published canonical payload matches every other
        // request field. Fully published ownership returns idempotently before
        // admission; uniformly reserved ownership still follows recovery below.
        let mut ownership = Vec::with_capacity(guarded_paths.len());
        for guarded_path in guarded_paths {
            ownership.push(
                self.content_lock_ownership
                    .get_path_ownership(creator, guarded_path)
                    .await?,
            );
        }
        let Some(existing_lock_id) = ownership
            .iter()
            .find_map(|record| record.as_ref().map(|record| record.lock_id.clone()))
        else {
            return Ok(None);
        };
        for (guarded_path, record) in guarded_paths.iter().zip(&ownership) {
            if !record
                .as_ref()
                .is_some_and(|record| record.lock_id == existing_lock_id)
            {
                return Err(ApplicationError::ContentLockPathConflict {
                    guarded_path: guarded_path.clone(),
                });
            }
        }
        let all_published = ownership.iter().all(|record| {
            record
                .as_ref()
                .is_some_and(|record| record.status == ContentLockOwnershipStatus::Published)
        });
        let all_reserved = ownership.iter().all(|record| {
            record
                .as_ref()
                .is_some_and(|record| record.status == ContentLockOwnershipStatus::Reserved)
        });
        if !all_published && !all_reserved {
            return Err(ApplicationError::ContentLockPathConflict {
                guarded_path: guarded_paths[0].clone(),
            });
        }

        let existing_path = ContentLockPath::from_lock_id(existing_lock_id.clone());
        let Some(existing) = self
            .content_locks
            .get_content_lock(creator, &existing_path)
            .await?
        else {
            return Err(ApplicationError::ContentLockPathConflict {
                guarded_path: guarded_paths[0].clone(),
            });
        };
        let canonical_lock_id =
            existing
                .lock_id()
                .map_err(|error| ApplicationError::ContentLockCanonicalization {
                    message: error.to_string(),
                })?;
        let mut expected = requested.clone();
        expected.created_at = existing.created_at;
        if canonical_lock_id != existing_lock_id || existing != expected {
            return Err(ApplicationError::ContentLockPathConflict {
                guarded_path: guarded_paths[0].clone(),
            });
        }
        let ownership_status = if all_published {
            ContentLockOwnershipStatus::Published
        } else {
            ContentLockOwnershipStatus::Reserved
        };
        Ok(Some((existing, ownership_status)))
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
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

    use async_trait::async_trait;
    use serde_json::json;
    use time::OffsetDateTime;
    use time::macros::datetime;
    use tokio::sync::Notify;

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
    use crate::infrastructure::postgres::{
        PostgresContentLockDeletionRepository, PostgresContentLockOwnershipRepository,
        testing::TestDatabase,
    };

    #[tokio::test]
    async fn create_content_lock_stores_payload_under_derived_path() {
        let fixture = Fixture::seeded().await;
        let use_case = fixture.use_case();
        let request = content_lock_request(registered_guarded_resource());
        let guarded_path = request
            .primary_resource
            .as_ref()
            .expect("fixture has a primary resource")
            .path
            .clone();

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
            .get_path_ownership(&creator(), &guarded_path)
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
            .prepare_force_deletion(
                &request.creator,
                &lock_id,
                Some(&content_lock),
                fixture.clock.now(),
            )
            .await
            .unwrap();

        let result = fixture.use_case().execute(request).await;

        assert_eq!(result, Err(ApplicationError::ContentLockDeletionInProgress));
        assert_eq!(fixture.content_locks_len().await, 0);
        assert_eq!(
            fixture
                .content_lock_ownership
                .get_path_ownership(&creator(), "/priv/app.locks/content/hello.txt")
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
        fixture.clock.advance(time::Duration::seconds(1));
        let second = use_case.execute(request).await.unwrap();

        assert_eq!(second.lock_id, first.lock_id);
        assert_eq!(second.content_lock_path, first.content_lock_path);
        assert_eq!(second.content_lock, first.content_lock);
        assert_eq!(
            fixture
                .content_locks
                .get_content_lock(&creator(), &first.content_lock_path)
                .await
                .unwrap(),
            Some(first.content_lock)
        );
    }

    #[tokio::test]
    async fn identical_create_does_not_republish_already_published_lock() {
        let fixture = Fixture::seeded().await;
        let content_locks = CountingContentLockRepository {
            inner: &fixture.content_locks,
            upsert_count: AtomicUsize::new(0),
        };
        let use_case = CreateContentLockUseCase::new(
            &content_locks,
            &fixture.content_lock_deletions,
            &fixture.content_lock_ownership,
            &fixture.guarded_resources,
            &fixture.clock,
        );
        let request = content_lock_request(registered_guarded_resource());

        let first = use_case.execute(request.clone()).await.unwrap();
        fixture.clock.advance(time::Duration::seconds(1));
        let replayed = use_case.execute(request).await.unwrap();

        assert_eq!(replayed, first);
        assert_eq!(content_locks.upsert_count.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn changed_criteria_rejects_path_owned_by_different_lock() {
        let fixture = Fixture::seeded().await;
        let use_case = fixture.use_case();
        let first_request = content_lock_request(registered_guarded_resource());
        let guarded_path = first_request
            .primary_resource
            .as_ref()
            .expect("fixture has a primary resource")
            .path
            .clone();
        let mut second_request = first_request.clone();
        second_request.criteria[0].params = json!({ "satisfied": false });

        let first = use_case.execute(first_request).await.unwrap();
        fixture.clock.advance(time::Duration::seconds(1));
        let second = use_case.execute(second_request).await;

        assert!(matches!(
            second,
            Err(ApplicationError::ContentLockPathConflict { guarded_path: ref conflicting_path })
                if conflicting_path == &guarded_path
        ));
        assert_eq!(
            fixture
                .content_locks
                .get_content_lock(&creator(), &first.content_lock_path)
                .await
                .unwrap(),
            Some(first.content_lock.clone())
        );
        let ownership = fixture
            .content_lock_ownership
            .get_path_ownership(&creator(), &guarded_path)
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
    async fn publication_failure_retains_reserved_path_ownership_and_intent() {
        let fixture = Fixture::seeded().await;
        let use_case = CreateContentLockUseCase::new(
            &FailingContentLockRepository,
            &fixture.content_lock_deletions,
            &fixture.content_lock_ownership,
            &fixture.guarded_resources,
            &fixture.clock,
        );
        let request = content_lock_request(registered_guarded_resource());
        let guarded_path = request
            .primary_resource
            .as_ref()
            .expect("fixture has a primary resource")
            .path
            .clone();

        let result = use_case.execute(request).await;

        assert!(matches!(
            result,
            Err(ApplicationError::Storage { ref message }) if message == "publication failed"
        ));
        let ownership = fixture
            .content_lock_ownership
            .get_path_ownership(&creator(), &guarded_path)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(ownership.status, ContentLockOwnershipStatus::Reserved);
        assert!(
            fixture
                .content_lock_deletions
                .publication_in_progress(&creator(), &ownership.lock_id)
                .await
                .unwrap()
        );
    }

    #[tokio::test]
    async fn ambiguous_publication_error_retains_fence_despite_exact_readback() {
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
                .get_path_ownership(&creator(), "/priv/app.locks/content/hello.txt")
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
    }

    #[tokio::test]
    async fn exact_retry_clears_retained_intent_only_after_publication_and_ownership_match() {
        let fixture = Fixture::seeded().await;
        let request = content_lock_request(registered_guarded_resource());
        let created = fixture.use_case().execute(request.clone()).await.unwrap();
        let retained_token = Uuid::new_v4();
        fixture
            .content_lock_deletions
            .begin_publication(&creator(), &created.lock_id, retained_token)
            .await
            .unwrap();
        assert!(
            fixture
                .content_lock_deletions
                .mark_publication_written(&creator(), &created.lock_id, retained_token)
                .await
                .unwrap()
        );

        let replayed = fixture.use_case().execute(request).await.unwrap();

        assert_eq!(replayed, created);
        assert!(
            !fixture
                .content_lock_deletions
                .publication_in_progress(&creator(), &created.lock_id)
                .await
                .unwrap()
        );
        assert_eq!(
            fixture
                .content_lock_ownership
                .get_path_ownership(&creator(), "/priv/app.locks/content/hello.txt")
                .await
                .unwrap()
                .unwrap()
                .status,
            ContentLockOwnershipStatus::Published
        );
    }

    #[tokio::test]
    async fn exact_published_retry_racing_force_does_not_republish_or_fail() {
        let fixture = Fixture::seeded().await;
        let request = content_lock_request(registered_guarded_resource());
        let created = fixture.use_case().execute(request.clone()).await.unwrap();
        let content_locks = BlockingReadContentLockRepository {
            inner: &fixture.content_locks,
            snapshot_taken: Notify::new(),
            release_read: Notify::new(),
            upsert_count: AtomicUsize::new(0),
        };
        let retry_use_case = CreateContentLockUseCase::new(
            &content_locks,
            &fixture.content_lock_deletions,
            &fixture.content_lock_ownership,
            &fixture.guarded_resources,
            &fixture.clock,
        );

        let retry = retry_use_case.execute(request);
        let force = async {
            content_locks.snapshot_taken.notified().await;
            let prepared = fixture
                .content_lock_deletions
                .prepare_force_deletion(
                    &creator(),
                    &created.lock_id,
                    Some(&created.content_lock),
                    fixture.clock.now(),
                )
                .await
                .unwrap();
            fixture
                .content_locks
                .delete_content_lock(&creator(), &created.content_lock_path)
                .await
                .unwrap();
            content_locks.release_read.notify_one();
            prepared
        };

        let (retried, forced) = tokio::join!(retry, force);

        assert_eq!(retried.unwrap(), created);
        assert!(matches!(
            forced,
            PrepareForceDeletionResult::Synchronous(receipt)
                if receipt.completed_at.is_none()
        ));
        assert_eq!(content_locks.upsert_count.load(Ordering::SeqCst), 0);
        assert!(
            fixture
                .content_locks
                .get_content_lock(&creator(), &created.content_lock_path)
                .await
                .unwrap()
                .is_none()
        );
    }

    #[tokio::test]
    async fn exact_retry_reconciliation_does_not_fail_original_publisher() {
        let fixture = Fixture::seeded().await;
        let content_locks = CountingContentLockRepository {
            inner: &fixture.content_locks,
            upsert_count: AtomicUsize::new(0),
        };
        let ownership = PauseAfterPublishOwnership {
            inner: &fixture.content_lock_ownership,
            published: Notify::new(),
            release: Notify::new(),
        };
        let use_case = CreateContentLockUseCase::new(
            &content_locks,
            &fixture.content_lock_deletions,
            &ownership,
            &fixture.guarded_resources,
            &fixture.clock,
        );
        let request = content_lock_request(registered_guarded_resource());

        let original = use_case.execute(request.clone());
        let replay = async {
            ownership.published.notified().await;
            let result = use_case.execute(request).await;
            ownership.release.notify_one();
            result
        };
        let (original, replayed) = tokio::join!(original, replay);

        let original = original.unwrap();
        assert_eq!(replayed.unwrap(), original);
        assert_eq!(content_locks.upsert_count.load(Ordering::SeqCst), 1);
        assert!(
            !fixture
                .content_lock_deletions
                .publication_in_progress(&creator(), &original.lock_id)
                .await
                .unwrap()
        );
    }

    #[tokio::test]
    async fn exact_retry_recovers_same_lock_reserved_ownership_after_publish_commit_failure() {
        let fixture = Fixture::seeded().await;
        let initial_time = datetime!(2026-06-03 12:00:00 UTC);
        let clock = MutableClock::new(initial_time);
        let ownership = FailFirstPublishOwnership {
            inner: &fixture.content_lock_ownership,
            fail_next_publish: AtomicBool::new(true),
        };
        let use_case = CreateContentLockUseCase::new(
            &fixture.content_locks,
            &fixture.content_lock_deletions,
            &ownership,
            &fixture.guarded_resources,
            &clock,
        );
        let request = content_lock_request(registered_guarded_resource());

        assert!(matches!(
            use_case.execute(request.clone()).await,
            Err(ApplicationError::Storage { ref message }) if message == "injected ownership commit failure"
        ));
        let original_lock_id = fixture
            .content_lock_ownership
            .get_path_ownership(&creator(), "/priv/app.locks/content/hello.txt")
            .await
            .unwrap()
            .unwrap()
            .lock_id;
        assert!(
            fixture
                .content_lock_deletions
                .publication_in_progress(&creator(), &original_lock_id)
                .await
                .unwrap()
        );
        clock.advance(time::Duration::seconds(1));
        let recovered = use_case.execute(request).await.unwrap();

        assert_eq!(recovered.lock_id, original_lock_id);
        assert_eq!(recovered.content_lock.created_at, initial_time);
        assert!(
            !fixture
                .content_lock_deletions
                .publication_in_progress(&creator(), &recovered.lock_id)
                .await
                .unwrap()
        );
        assert_eq!(
            fixture
                .content_lock_ownership
                .get_path_ownership(&creator(), "/priv/app.locks/content/hello.txt")
                .await
                .unwrap()
                .unwrap()
                .status,
            ContentLockOwnershipStatus::Published
        );
    }

    #[tokio::test]
    async fn changed_request_cannot_take_over_retained_publication_intent() {
        let fixture = Fixture::seeded().await;
        let ownership = FailFirstPublishOwnership {
            inner: &fixture.content_lock_ownership,
            fail_next_publish: AtomicBool::new(true),
        };
        let use_case = CreateContentLockUseCase::new(
            &fixture.content_locks,
            &fixture.content_lock_deletions,
            &ownership,
            &fixture.guarded_resources,
            &fixture.clock,
        );
        let request = content_lock_request(registered_guarded_resource());

        assert!(matches!(
            use_case.execute(request).await,
            Err(ApplicationError::Storage { ref message }) if message == "injected ownership commit failure"
        ));
        let original_ownership = fixture
            .content_lock_ownership
            .get_path_ownership(&creator(), "/priv/app.locks/content/hello.txt")
            .await
            .unwrap()
            .unwrap();
        fixture.clock.advance(time::Duration::seconds(1));

        assert!(matches!(
            use_case
                .execute(content_lock_request_with_changed_criteria())
                .await,
            Err(ApplicationError::ContentLockPathConflict { .. })
        ));
        assert_eq!(
            fixture
                .content_lock_ownership
                .get_path_ownership(&creator(), "/priv/app.locks/content/hello.txt")
                .await
                .unwrap()
                .unwrap(),
            original_ownership
        );
        assert!(
            fixture
                .content_lock_deletions
                .publication_in_progress(&creator(), &original_ownership.lock_id)
                .await
                .unwrap()
        );
    }

    #[tokio::test]
    async fn postgres_exact_retry_recovers_after_injected_ownership_commit_failure() {
        let database = TestDatabase::create().await;
        let content_locks = InMemoryContentLockRepository::new();
        let content_lock_deletions =
            PostgresContentLockDeletionRepository::new(database.pool().clone());
        let content_lock_ownership =
            PostgresContentLockOwnershipRepository::new(database.pool().clone());
        let ownership = FailFirstPublishOwnership {
            inner: &content_lock_ownership,
            fail_next_publish: AtomicBool::new(true),
        };
        let guarded_resources = InMemoryGuardedResourceRepository::new();
        let initial_time = datetime!(2026-06-03 12:00:00 UTC);
        let clock = MutableClock::new(initial_time);
        let guarded_resource = registered_guarded_resource();
        guarded_resources
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
        let use_case = CreateContentLockUseCase::new(
            &content_locks,
            &content_lock_deletions,
            &ownership,
            &guarded_resources,
            &clock,
        );
        let request = content_lock_request(guarded_resource);

        assert!(matches!(
            use_case.execute(request.clone()).await,
            Err(ApplicationError::Storage { ref message }) if message == "injected ownership commit failure"
        ));
        let original_lock_id = content_lock_ownership
            .get_path_ownership(&creator(), "/priv/app.locks/content/hello.txt")
            .await
            .unwrap()
            .unwrap()
            .lock_id;
        assert!(
            content_lock_deletions
                .publication_in_progress(&creator(), &original_lock_id)
                .await
                .unwrap()
        );
        clock.advance(time::Duration::seconds(1));
        let recovered = use_case.execute(request).await.unwrap();

        assert_eq!(recovered.lock_id, original_lock_id);
        assert_eq!(recovered.content_lock.created_at, initial_time);
        assert!(
            !content_lock_deletions
                .publication_in_progress(&creator(), &recovered.lock_id)
                .await
                .unwrap()
        );
        assert_eq!(
            content_lock_ownership
                .get_path_ownership(&creator(), "/priv/app.locks/content/hello.txt")
                .await
                .unwrap()
                .unwrap()
                .status,
            ContentLockOwnershipStatus::Published
        );
        database.cleanup().await;
    }

    #[tokio::test]
    async fn failed_memory_publisher_keeps_retry_and_force_fenced_from_old_exact_bytes() {
        let fixture = Fixture::seeded().await;

        assert_failed_publisher_keeps_retry_and_force_fenced(
            &fixture.content_lock_deletions,
            &fixture.content_lock_ownership,
            &fixture.guarded_resources,
            &fixture.clock,
        )
        .await;
    }

    #[tokio::test]
    async fn failed_postgres_publisher_keeps_retry_and_force_fenced_from_old_exact_bytes() {
        let database = TestDatabase::create().await;
        let content_lock_deletions =
            PostgresContentLockDeletionRepository::new(database.pool().clone());
        let content_lock_ownership =
            PostgresContentLockOwnershipRepository::new(database.pool().clone());
        let guarded_resources = InMemoryGuardedResourceRepository::new();
        let clock = MutableClock::new(datetime!(2026-06-03 12:00:00 UTC));
        let guarded_resource = registered_guarded_resource();
        guarded_resources
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

        assert_failed_publisher_keeps_retry_and_force_fenced(
            &content_lock_deletions,
            &content_lock_ownership,
            &guarded_resources,
            &clock,
        )
        .await;
        database.cleanup().await;
    }

    async fn assert_failed_publisher_keeps_retry_and_force_fenced(
        content_lock_deletions: &dyn ContentLockDeletionRepository,
        content_lock_ownership: &dyn ContentLockOwnershipRepository,
        guarded_resources: &dyn GuardedResourceRepository,
        clock: &dyn Clock,
    ) {
        let stored_content_locks = InMemoryContentLockRepository::new();
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
            created_at: clock.now(),
        };
        let lock_id = content_lock.lock_id().unwrap();
        let content_lock_path = content_lock.content_lock_path().unwrap();
        stored_content_locks
            .upsert_content_lock(
                request.creator.clone(),
                content_lock_path.clone(),
                content_lock.clone(),
            )
            .await
            .unwrap();
        let delayed_content_locks = DelayedFailedUpsertContentLockRepository {
            inner: &stored_content_locks,
            pending_write: Mutex::new(None),
        };
        let use_case = CreateContentLockUseCase::new(
            &delayed_content_locks,
            content_lock_deletions,
            content_lock_ownership,
            guarded_resources,
            clock,
        );

        let original = use_case.execute(request.clone()).await;

        assert!(matches!(
            original,
            Err(ApplicationError::Storage { ref message })
                if message == "publication response lost"
        ));
        assert!(delayed_content_locks.has_pending_write());
        assert_eq!(
            content_lock_ownership
                .get_path_ownership(&creator(), "/priv/app.locks/content/hello.txt")
                .await
                .unwrap()
                .unwrap()
                .status,
            ContentLockOwnershipStatus::Reserved
        );
        assert!(
            content_lock_deletions
                .publication_in_progress(&creator(), &lock_id)
                .await
                .unwrap()
        );

        let retry = use_case.execute(request).await;
        let force = content_lock_deletions
            .prepare_force_deletion(&creator(), &lock_id, Some(&content_lock), clock.now())
            .await
            .unwrap();

        assert!(matches!(
            retry,
            Err(ApplicationError::Storage { ref message })
                if message == "content lock publication reconciliation was not committed"
        ));
        assert_eq!(force, PrepareForceDeletionResult::PublicationInProgress);
        assert!(
            !content_lock_deletions
                .has_force_receipt(&creator(), &lock_id)
                .await
                .unwrap()
        );
        delayed_content_locks.land_delayed_write().await;
        assert_eq!(
            content_lock_deletions
                .prepare_force_deletion(&creator(), &lock_id, Some(&content_lock), clock.now())
                .await
                .unwrap(),
            PrepareForceDeletionResult::PublicationInProgress
        );
        assert!(
            !content_lock_deletions
                .has_force_receipt(&creator(), &lock_id)
                .await
                .unwrap()
        );
        assert_eq!(
            stored_content_locks
                .get_content_lock(&creator(), &content_lock_path)
                .await
                .unwrap(),
            Some(content_lock)
        );
    }

    #[tokio::test]
    async fn retry_keeps_retained_intent_when_public_canonical_bytes_do_not_match() {
        let fixture = Fixture::seeded().await;
        let request = content_lock_request(registered_guarded_resource());
        let created = fixture.use_case().execute(request.clone()).await.unwrap();
        fixture
            .content_lock_deletions
            .begin_publication(&creator(), &created.lock_id, Uuid::new_v4())
            .await
            .unwrap();
        let mut replacement = created.content_lock.clone();
        replacement.access_policy.requested_credential_ttl_seconds += 1;
        fixture
            .content_locks
            .upsert_content_lock(creator(), created.content_lock_path.clone(), replacement)
            .await
            .unwrap();
        fixture.clock.advance(time::Duration::seconds(1));

        assert!(matches!(
            fixture.use_case().execute(request).await,
            Err(ApplicationError::ContentLockPathConflict { .. })
        ));
        assert!(
            fixture
                .content_lock_deletions
                .publication_in_progress(&creator(), &created.lock_id)
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
                .get_path_ownership(&creator(), "/priv/app.locks/content/hello.txt")
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
                .prepare_force_deletion(&creator(), &lock_id, None, fixture.clock.now())
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

        assert!(matches!(
            fixture
                .content_lock_deletions
                .prepare_force_deletion(
                    &creator(),
                    &created.lock_id,
                    None,
                    fixture.clock.now(),
                )
                .await
                .unwrap(),
            PrepareForceDeletionResult::Synchronous(receipt)
                if receipt.frozen_content_lock.is_none() && receipt.completed_at.is_none()
        ));
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
                    .prepare_force_deletion(&creator, &lock_id, None, self.now)
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

    struct CountingContentLockRepository<'a> {
        inner: &'a dyn ContentLockRepository,
        upsert_count: AtomicUsize,
    }

    #[async_trait]
    impl ContentLockRepository for CountingContentLockRepository<'_> {
        async fn upsert_content_lock(
            &self,
            creator: CreatorPubky,
            path: ContentLockPath,
            content_lock: ContentLock,
        ) -> Result<(), ApplicationError> {
            self.upsert_count.fetch_add(1, Ordering::SeqCst);
            self.inner
                .upsert_content_lock(creator, path, content_lock)
                .await
        }

        async fn get_content_lock(
            &self,
            creator: &CreatorPubky,
            path: &ContentLockPath,
        ) -> Result<Option<ContentLock>, ApplicationError> {
            self.inner.get_content_lock(creator, path).await
        }

        async fn delete_content_lock(
            &self,
            creator: &CreatorPubky,
            path: &ContentLockPath,
        ) -> Result<bool, ApplicationError> {
            self.inner.delete_content_lock(creator, path).await
        }
    }

    struct BlockingReadContentLockRepository<'a> {
        inner: &'a dyn ContentLockRepository,
        snapshot_taken: Notify,
        release_read: Notify,
        upsert_count: AtomicUsize,
    }

    #[async_trait]
    impl ContentLockRepository for BlockingReadContentLockRepository<'_> {
        async fn upsert_content_lock(
            &self,
            creator: CreatorPubky,
            path: ContentLockPath,
            content_lock: ContentLock,
        ) -> Result<(), ApplicationError> {
            self.upsert_count.fetch_add(1, Ordering::SeqCst);
            self.inner
                .upsert_content_lock(creator, path, content_lock)
                .await
        }

        async fn get_content_lock(
            &self,
            creator: &CreatorPubky,
            path: &ContentLockPath,
        ) -> Result<Option<ContentLock>, ApplicationError> {
            let content_lock = self.inner.get_content_lock(creator, path).await?;
            self.snapshot_taken.notify_one();
            self.release_read.notified().await;
            Ok(content_lock)
        }

        async fn delete_content_lock(
            &self,
            creator: &CreatorPubky,
            path: &ContentLockPath,
        ) -> Result<bool, ApplicationError> {
            self.inner.delete_content_lock(creator, path).await
        }
    }

    struct DelayedFailedUpsertContentLockRepository<'a> {
        inner: &'a dyn ContentLockRepository,
        pending_write: Mutex<Option<(CreatorPubky, ContentLockPath, ContentLock)>>,
    }

    impl DelayedFailedUpsertContentLockRepository<'_> {
        fn has_pending_write(&self) -> bool {
            self.pending_write.lock().unwrap().is_some()
        }

        async fn land_delayed_write(&self) {
            let (creator, path, content_lock) = self.pending_write.lock().unwrap().take().unwrap();
            self.inner
                .upsert_content_lock(creator, path, content_lock)
                .await
                .unwrap();
        }
    }

    #[async_trait]
    impl ContentLockRepository for DelayedFailedUpsertContentLockRepository<'_> {
        async fn upsert_content_lock(
            &self,
            creator: CreatorPubky,
            path: ContentLockPath,
            content_lock: ContentLock,
        ) -> Result<(), ApplicationError> {
            *self.pending_write.lock().unwrap() = Some((creator, path, content_lock));
            Err(ApplicationError::Storage {
                message: "publication response lost".to_owned(),
            })
        }

        async fn get_content_lock(
            &self,
            creator: &CreatorPubky,
            path: &ContentLockPath,
        ) -> Result<Option<ContentLock>, ApplicationError> {
            self.inner.get_content_lock(creator, path).await
        }

        async fn delete_content_lock(
            &self,
            creator: &CreatorPubky,
            path: &ContentLockPath,
        ) -> Result<bool, ApplicationError> {
            self.inner.delete_content_lock(creator, path).await
        }
    }

    struct PauseAfterPublishOwnership<'a> {
        inner: &'a dyn ContentLockOwnershipRepository,
        published: Notify,
        release: Notify,
    }

    #[async_trait]
    impl ContentLockOwnershipRepository for PauseAfterPublishOwnership<'_> {
        async fn reserve_paths(
            &self,
            creator: &CreatorPubky,
            guarded_paths: &[String],
            lock_id: &LockId,
        ) -> Result<(), ApplicationError> {
            self.inner
                .reserve_paths(creator, guarded_paths, lock_id)
                .await
        }

        async fn mark_paths_published(
            &self,
            creator: &CreatorPubky,
            guarded_paths: &[String],
            lock_id: &LockId,
        ) -> Result<(), ApplicationError> {
            self.inner
                .mark_paths_published(creator, guarded_paths, lock_id)
                .await?;
            self.published.notify_one();
            self.release.notified().await;
            Ok(())
        }

        async fn compensate_reserved_paths(
            &self,
            creator: &CreatorPubky,
            guarded_paths: &[String],
            lock_id: &LockId,
        ) -> Result<(), ApplicationError> {
            self.inner
                .compensate_reserved_paths(creator, guarded_paths, lock_id)
                .await
        }

        async fn get_path_ownership(
            &self,
            creator: &CreatorPubky,
            guarded_path: &str,
        ) -> Result<Option<crate::application::models::ContentLockOwnership>, ApplicationError>
        {
            self.inner.get_path_ownership(creator, guarded_path).await
        }
    }

    struct FailFirstPublishOwnership<'a> {
        inner: &'a dyn ContentLockOwnershipRepository,
        fail_next_publish: AtomicBool,
    }

    #[async_trait]
    impl ContentLockOwnershipRepository for FailFirstPublishOwnership<'_> {
        async fn reserve_paths(
            &self,
            creator: &CreatorPubky,
            guarded_paths: &[String],
            lock_id: &LockId,
        ) -> Result<(), ApplicationError> {
            self.inner
                .reserve_paths(creator, guarded_paths, lock_id)
                .await
        }

        async fn mark_paths_published(
            &self,
            creator: &CreatorPubky,
            guarded_paths: &[String],
            lock_id: &LockId,
        ) -> Result<(), ApplicationError> {
            if self.fail_next_publish.swap(false, Ordering::SeqCst) {
                return Err(ApplicationError::Storage {
                    message: "injected ownership commit failure".to_owned(),
                });
            }
            self.inner
                .mark_paths_published(creator, guarded_paths, lock_id)
                .await
        }

        async fn compensate_reserved_paths(
            &self,
            creator: &CreatorPubky,
            guarded_paths: &[String],
            lock_id: &LockId,
        ) -> Result<(), ApplicationError> {
            self.inner
                .compensate_reserved_paths(creator, guarded_paths, lock_id)
                .await
        }

        async fn get_path_ownership(
            &self,
            creator: &CreatorPubky,
            guarded_path: &str,
        ) -> Result<Option<crate::application::models::ContentLockOwnership>, ApplicationError>
        {
            self.inner.get_path_ownership(creator, guarded_path).await
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
        clock: MutableClock,
    }

    impl Fixture {
        fn empty() -> Self {
            Self {
                content_locks: InMemoryContentLockRepository::new(),
                content_lock_deletions: InMemoryContentLockDeletionRepository::new(),
                content_lock_ownership: InMemoryContentLockOwnershipRepository::new(),
                guarded_resources: InMemoryGuardedResourceRepository::new(),
                clock: MutableClock::new(datetime!(2026-06-03 12:00:00 UTC)),
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
            path: "/priv/app.locks/content/hello.txt".to_owned(),
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

    struct MutableClock(Mutex<OffsetDateTime>);

    impl MutableClock {
        fn new(now: OffsetDateTime) -> Self {
            Self(Mutex::new(now))
        }

        fn advance(&self, duration: time::Duration) {
            let mut now = self.0.lock().unwrap();
            *now += duration;
        }
    }

    impl Clock for MutableClock {
        fn now(&self) -> OffsetDateTime {
            *self.0.lock().unwrap()
        }
    }
}
