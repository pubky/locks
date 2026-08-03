use std::collections::BTreeMap;

use locks_core::ids::{ContentLockPath, CreatorPubky, LockId};
use locks_core::lock_policy::{
    AccessPolicy, CONTENT_LOCK_VERSION, ContentLock, Criterion, GuardedResource, LockLogic,
    LockServerConfig, SecondaryGuardedResource,
};

use crate::application::errors::ApplicationError;
use crate::application::ports::{Clock, ContentLockRepository, GuardedResourceRepository};

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
    guarded_resources: &'a dyn GuardedResourceRepository,
    clock: &'a dyn Clock,
}

impl<'a> CreateContentLockUseCase<'a> {
    /// Creates a content-lock use case from its application ports.
    pub fn new(
        content_locks: &'a dyn ContentLockRepository,
        guarded_resources: &'a dyn GuardedResourceRepository,
        clock: &'a dyn Clock,
    ) -> Self {
        Self {
            content_locks,
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

        self.content_locks
            .upsert_content_lock(
                request.creator,
                content_lock_path.clone(),
                content_lock.clone(),
            )
            .await?;

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

    use serde_json::json;
    use time::OffsetDateTime;
    use time::macros::datetime;

    use locks_core::ids::{CreatorPubky, GuardedResourceHash, LockServerPubky};
    use locks_core::lock_policy::VerifierType;

    use super::*;
    use crate::application::models::GuardedResourceRecord;
    use crate::application::ports::{Clock, ContentLockRepository, GuardedResourceRepository};
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
    async fn changed_criteria_create_different_lock_id_and_path() {
        let fixture = Fixture::seeded().await;
        let use_case = fixture.use_case();
        let first_request = content_lock_request(registered_guarded_resource());
        let mut second_request = first_request.clone();
        second_request.criteria[0].params = json!({ "satisfied": false });

        let first = use_case.execute(first_request).await.unwrap();
        let second = use_case.execute(second_request).await.unwrap();

        assert_ne!(second.lock_id, first.lock_id);
        assert_ne!(second.content_lock_path, first.content_lock_path);
        assert_ne!(second.content_lock, first.content_lock);
        assert_eq!(fixture.content_locks_len().await, 2);
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
                "asset": "BTC"
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

    struct Fixture {
        content_locks: InMemoryContentLockRepository,
        guarded_resources: InMemoryGuardedResourceRepository,
        clock: FixedClock,
    }

    impl Fixture {
        fn empty() -> Self {
            Self {
                content_locks: InMemoryContentLockRepository::new(),
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
            CreateContentLockUseCase::new(&self.content_locks, &self.guarded_resources, &self.clock)
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
