use crate::application::errors::ApplicationError;
use crate::application::models::{AccessCredential, AccessCredentialLookupKey};
use crate::application::ports::{
    AccessCredentialStore, Clock, ContentLockRepository, EntitlementRepository,
    GuardedResourceRepository,
};
use crate::application::use_cases::entitlement_check::load_valid_entitlement;
use crate::application::use_cases::validate_access_credential::{
    ValidateAccessCredentialRequest, ValidateAccessCredentialUseCase,
};
use locks_core::ids::GuardedResourceHash;

/// Request to proxy-read a guarded resource using a presented access credential.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProxyReadGuardedResourceRequest {
    /// Raw bearer credential presented by the caller.
    pub credential: AccessCredential,
    /// Full guarded resource path requested by the caller.
    pub path: String,
}

/// Guarded resource bytes returned after successful credential and entitlement validation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProxiedGuardedResource {
    /// Creator-relative guarded resource path read through the proxy.
    pub path: String,
    /// MIME content type for the guarded resource.
    pub content_type: String,
    /// Verified guarded resource hash from the content-lock descriptor.
    pub hash: GuardedResourceHash,
    /// Guarded resource bytes.
    pub bytes: Vec<u8>,
    deletion_claim: Option<DeletionReadClaim>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct DeletionReadClaim {
    lookup_key: AccessCredentialLookupKey,
    path: String,
    claim_token: uuid::Uuid,
}

/// Validates an access credential and returns the currently guarded resource bytes.
pub struct ProxyReadGuardedResourceUseCase<'a> {
    credential_store: &'a dyn AccessCredentialStore,
    entitlements: &'a dyn EntitlementRepository,
    content_locks: &'a dyn ContentLockRepository,
    guarded_resources: &'a dyn GuardedResourceRepository,
    clock: &'a dyn Clock,
}

impl<'a> ProxyReadGuardedResourceUseCase<'a> {
    /// Creates a proxy-read use case from its ports.
    pub fn new(
        credential_store: &'a dyn AccessCredentialStore,
        entitlements: &'a dyn EntitlementRepository,
        content_locks: &'a dyn ContentLockRepository,
        guarded_resources: &'a dyn GuardedResourceRepository,
        clock: &'a dyn Clock,
    ) -> Self {
        Self {
            credential_store,
            entitlements,
            content_locks,
            guarded_resources,
            clock,
        }
    }

    /// Validates access and reads guarded resource bytes.
    pub async fn execute(
        &self,
        request: ProxyReadGuardedResourceRequest,
    ) -> Result<ProxiedGuardedResource, ApplicationError> {
        let now = self.clock.now();
        let lookup_key = AccessCredentialLookupKey::derive(&request.credential);
        if let Some(authorization) = self
            .credential_store
            .prepare_deletion_read(
                &lookup_key,
                &request.path,
                now,
                now + time::Duration::seconds(30),
            )
            .await?
        {
            let claim_token = authorization.claim_token;
            let guarded_resource = authorization.resource;
            let prepared_response = async {
                let guarded_record = self
                    .guarded_resources
                    .get_current_guarded_resource(&authorization.creator, &guarded_resource.path)
                    .await?
                    .ok_or(ApplicationError::GuardedResourceUnavailable)?;
                if guarded_record.hash != guarded_resource.hash
                    || guarded_record.content_type != guarded_resource.content_type
                    || guarded_record.size != guarded_resource.size
                {
                    return Err(ApplicationError::GuardedResourceUnavailable);
                }
                Ok(ProxiedGuardedResource {
                    path: guarded_resource.path,
                    content_type: guarded_record.content_type,
                    hash: guarded_resource.hash,
                    bytes: guarded_record.bytes,
                    deletion_claim: claim_token.map(|claim_token| DeletionReadClaim {
                        lookup_key: lookup_key.clone(),
                        path: request.path.clone(),
                        claim_token,
                    }),
                })
            }
            .await;
            match prepared_response {
                Ok(response) => return Ok(response),
                Err(error) => {
                    if let Some(claim_token) = claim_token {
                        self.credential_store
                            .release_deletion_read(
                                &lookup_key,
                                &request.path,
                                claim_token,
                                self.clock.now(),
                            )
                            .await?;
                    }
                    return Err(error);
                }
            }
        }
        if self
            .credential_store
            .deletion_credential_enrolled(&lookup_key)
            .await?
        {
            return Err(ApplicationError::GuardedResourceUnavailable);
        }
        let validation = ValidateAccessCredentialUseCase::new(
            self.credential_store,
            self.entitlements,
            self.content_locks,
            self.clock,
        );
        let validated = validation
            .execute(ValidateAccessCredentialRequest {
                credential: request.credential,
            })
            .await?;

        let entitlement = load_valid_entitlement(
            self.entitlements,
            self.content_locks,
            &validated.creator,
            &validated.bundle_id,
        )
        .await?;
        let guarded_resource = entitlement
            .content_lock
            .resource_for_path(&request.path)
            .ok_or(ApplicationError::GuardedResourceUnavailable)?;
        let guarded_record = self
            .guarded_resources
            .get_current_guarded_resource(&validated.creator, &guarded_resource.path)
            .await?
            .ok_or(ApplicationError::GuardedResourceUnavailable)?;

        if guarded_record.hash != guarded_resource.hash
            || guarded_record.content_type != guarded_resource.content_type
            || guarded_record.size != guarded_resource.size
        {
            return Err(ApplicationError::GuardedResourceUnavailable);
        }

        Ok(ProxiedGuardedResource {
            path: guarded_resource.path,
            content_type: guarded_record.content_type,
            hash: guarded_resource.hash,
            bytes: guarded_record.bytes,
            deletion_claim: None,
        })
    }

    /// Permanently consumes the exact final-read claim after a complete HTTP 200
    /// response has been constructed. A lost claim prevents response return.
    pub async fn consume_prepared_deletion_read(
        &self,
        response: &ProxiedGuardedResource,
    ) -> Result<(), ApplicationError> {
        let Some(claim) = &response.deletion_claim else {
            return Ok(());
        };
        if !self
            .credential_store
            .consume_deletion_read(
                &claim.lookup_key,
                &claim.path,
                claim.claim_token,
                self.clock.now(),
            )
            .await?
        {
            return Err(ApplicationError::InvalidAccessCredential);
        }
        Ok(())
    }

    /// Releases the exact final-read claim when HTTP response construction fails.
    pub async fn release_prepared_deletion_read(
        &self,
        response: &ProxiedGuardedResource,
    ) -> Result<(), ApplicationError> {
        let Some(claim) = &response.deletion_claim else {
            return Ok(());
        };
        self.credential_store
            .release_deletion_read(
                &claim.lookup_key,
                &claim.path,
                claim.claim_token,
                self.clock.now(),
            )
            .await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::str::FromStr;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use async_trait::async_trait;
    use serde_json::json;
    use time::OffsetDateTime;
    use time::macros::datetime;
    use uuid::Uuid;

    use locks_core::ids::{
        BundleId, CreatorPubky, GuardedResourceHash, LockServerPubky, PubkyLockResource,
    };
    use locks_core::lock_policy::{
        AccessPolicy, CONTENT_LOCK_VERSION, ContentLock, Criterion, GuardedResource, LockLogic,
        LockServerConfig, SecondaryGuardedResource, VerifierType,
    };
    use locks_core::verification::{
        CriterionVerificationResult, EntitlementLifetime, VERIFIED_PROOF_BUNDLE_VERSION,
        VerificationResult, VerifiedProofBundle,
    };

    use super::{ProxyReadGuardedResourceRequest, ProxyReadGuardedResourceUseCase};
    use crate::application::errors::ApplicationError;
    use crate::application::models::{
        AccessCredential, AccessCredentialLookupKey, AccessCredentialRecord,
        DeletionReadAuthorization, GuardedResourceRecord,
    };
    use crate::application::ports::{
        AccessCredentialStore, Clock, ContentLockRepository, EntitlementRepository,
        GuardedResourceRepository,
    };
    use crate::infrastructure::memory::access_credentials::InMemoryAccessCredentialStore;
    use crate::infrastructure::memory::content_locks::InMemoryContentLockRepository;
    use crate::infrastructure::memory::entitlements::InMemoryEntitlementRepository;
    use crate::infrastructure::memory::guarded_resources::InMemoryGuardedResourceRepository;

    const BUNDLE_ID: &str = "000G40R40M30E209185GR38E1W";

    #[tokio::test]
    async fn proxy_read_validates_credential_and_returns_guarded_resource_bytes() {
        let fixture = Fixture::seed().await;
        let use_case = ProxyReadGuardedResourceUseCase::new(
            &fixture.credentials,
            &fixture.entitlements,
            &fixture.content_locks,
            &fixture.guarded_resources,
            &fixture.clock,
        );

        let response = use_case
            .execute(ProxyReadGuardedResourceRequest {
                credential: fixture.credential.clone(),
                path: "/priv/locks.app/content/resource.txt".to_owned(),
            })
            .await
            .unwrap();

        assert_eq!(response.path, "/priv/locks.app/content/resource.txt");
        assert_eq!(response.content_type, "text/plain");
        assert_eq!(response.bytes, b"guarded bytes".to_vec());
    }

    #[tokio::test]
    async fn proxy_read_rejects_invalid_access_credential() {
        let fixture = Fixture::seed().await;
        let use_case = ProxyReadGuardedResourceUseCase::new(
            &fixture.credentials,
            &fixture.entitlements,
            &fixture.content_locks,
            &fixture.guarded_resources,
            &fixture.clock,
        );

        let result = use_case
            .execute(ProxyReadGuardedResourceRequest {
                credential: AccessCredential::new("wrong"),
                path: "/priv/locks.app/content/resource.txt".to_owned(),
            })
            .await;

        assert_eq!(result, Err(ApplicationError::InvalidAccessCredential));
    }

    #[tokio::test]
    async fn proxy_read_rejects_missing_guarded_resource() {
        let fixture = Fixture::seed_without_guarded_resource().await;
        let use_case = ProxyReadGuardedResourceUseCase::new(
            &fixture.credentials,
            &fixture.entitlements,
            &fixture.content_locks,
            &fixture.guarded_resources,
            &fixture.clock,
        );

        let result = use_case
            .execute(ProxyReadGuardedResourceRequest {
                credential: fixture.credential.clone(),
                path: "/priv/locks.app/content/resource.txt".to_owned(),
            })
            .await;

        assert_eq!(result, Err(ApplicationError::GuardedResourceUnavailable));
    }

    #[tokio::test]
    async fn proxy_read_secondary_resource_by_path_succeeds() {
        let fixture = Fixture::seed().await;
        let use_case = ProxyReadGuardedResourceUseCase::new(
            &fixture.credentials,
            &fixture.entitlements,
            &fixture.content_locks,
            &fixture.guarded_resources,
            &fixture.clock,
        );

        let response = use_case
            .execute(ProxyReadGuardedResourceRequest {
                credential: fixture.credential.clone(),
                path: "/priv/locks.app/content/secondary.txt".to_owned(),
            })
            .await
            .unwrap();

        assert_eq!(response.path, "/priv/locks.app/content/secondary.txt");
        assert_eq!(response.content_type, "text/plain");
        assert_eq!(response.bytes, b"secondary bytes".to_vec());
    }

    #[tokio::test]
    async fn proxy_read_path_outside_lock_resource_set_returns_unavailable() {
        let fixture = Fixture::seed().await;
        let use_case = ProxyReadGuardedResourceUseCase::new(
            &fixture.credentials,
            &fixture.entitlements,
            &fixture.content_locks,
            &fixture.guarded_resources,
            &fixture.clock,
        );

        let result = use_case
            .execute(ProxyReadGuardedResourceRequest {
                credential: fixture.credential.clone(),
                path: "/priv/locks.app/content/outside.txt".to_owned(),
            })
            .await;

        assert_eq!(result, Err(ApplicationError::GuardedResourceUnavailable));
    }

    #[tokio::test]
    async fn deletion_read_stays_claimed_until_response_boundary_consumes_it() {
        let fixture = Fixture::seed().await;
        let credentials = DeletionReadStore::new();
        let empty_public_locks = InMemoryContentLockRepository::new();
        let use_case = ProxyReadGuardedResourceUseCase::new(
            &credentials,
            &fixture.entitlements,
            &empty_public_locks,
            &fixture.guarded_resources,
            &fixture.clock,
        );

        let response = use_case
            .execute(ProxyReadGuardedResourceRequest {
                credential: fixture.credential,
                path: "/priv/locks.app/content/resource.txt".to_owned(),
            })
            .await
            .unwrap();

        assert_eq!(response.bytes, b"guarded bytes".to_vec());
        assert_eq!(credentials.consumes.load(Ordering::SeqCst), 0);
        use_case
            .consume_prepared_deletion_read(&response)
            .await
            .unwrap();
        assert_eq!(credentials.consumes.load(Ordering::SeqCst), 1);
        assert_eq!(credentials.releases.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn deletion_read_can_be_released_when_response_construction_fails() {
        let fixture = Fixture::seed().await;
        let credentials = DeletionReadStore::new();
        let use_case = ProxyReadGuardedResourceUseCase::new(
            &credentials,
            &fixture.entitlements,
            &fixture.content_locks,
            &fixture.guarded_resources,
            &fixture.clock,
        );
        let response = use_case
            .execute(ProxyReadGuardedResourceRequest {
                credential: fixture.credential,
                path: "/priv/locks.app/content/resource.txt".to_owned(),
            })
            .await
            .unwrap();

        use_case
            .release_prepared_deletion_read(&response)
            .await
            .unwrap();

        assert_eq!(credentials.releases.load(Ordering::SeqCst), 1);
        assert_eq!(credentials.consumes.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn deletion_read_releases_claim_when_upstream_resource_is_unavailable() {
        let fixture = Fixture::seed_without_guarded_resource().await;
        let credentials = DeletionReadStore::new();
        let use_case = ProxyReadGuardedResourceUseCase::new(
            &credentials,
            &fixture.entitlements,
            &fixture.content_locks,
            &fixture.guarded_resources,
            &fixture.clock,
        );

        let result = use_case
            .execute(ProxyReadGuardedResourceRequest {
                credential: fixture.credential,
                path: "/priv/locks.app/content/resource.txt".to_owned(),
            })
            .await;

        assert_eq!(result, Err(ApplicationError::GuardedResourceUnavailable));
        assert_eq!(credentials.releases.load(Ordering::SeqCst), 1);
        assert_eq!(credentials.consumes.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn deletion_credential_denied_path_does_not_fall_through_to_public_lock() {
        let fixture = Fixture::seed().await;
        let credentials = DeletionReadStore::new();
        let empty_public_locks = InMemoryContentLockRepository::new();
        let use_case = ProxyReadGuardedResourceUseCase::new(
            &credentials,
            &fixture.entitlements,
            &empty_public_locks,
            &fixture.guarded_resources,
            &fixture.clock,
        );

        let result = use_case
            .execute(ProxyReadGuardedResourceRequest {
                credential: fixture.credential,
                path: "/priv/locks.app/content/not-frozen.txt".to_owned(),
            })
            .await;

        assert_eq!(result, Err(ApplicationError::GuardedResourceUnavailable));
        assert_eq!(credentials.releases.load(Ordering::SeqCst), 0);
        assert_eq!(credentials.consumes.load(Ordering::SeqCst), 0);
    }

    struct DeletionReadStore {
        ordinary: InMemoryAccessCredentialStore,
        claim_token: Uuid,
        releases: AtomicUsize,
        consumes: AtomicUsize,
    }

    impl DeletionReadStore {
        fn new() -> Self {
            Self {
                ordinary: InMemoryAccessCredentialStore::new(),
                claim_token: Uuid::new_v4(),
                releases: AtomicUsize::new(0),
                consumes: AtomicUsize::new(0),
            }
        }
    }

    #[async_trait]
    impl AccessCredentialStore for DeletionReadStore {
        async fn insert_access_credential(
            &self,
            lock_id: &locks_core::ids::LockId,
            lookup_key: AccessCredentialLookupKey,
            record: AccessCredentialRecord,
        ) -> Result<(), ApplicationError> {
            self.ordinary
                .insert_access_credential(lock_id, lookup_key, record)
                .await
        }

        async fn get_access_credential(
            &self,
            lookup_key: &AccessCredentialLookupKey,
        ) -> Result<Option<AccessCredentialRecord>, ApplicationError> {
            self.ordinary.get_access_credential(lookup_key).await
        }

        async fn delete_access_credential(
            &self,
            lookup_key: &AccessCredentialLookupKey,
        ) -> Result<(), ApplicationError> {
            self.ordinary.delete_access_credential(lookup_key).await
        }

        async fn prepare_deletion_read(
            &self,
            _lookup_key: &AccessCredentialLookupKey,
            path: &str,
            _now: OffsetDateTime,
            _claim_expires_at: OffsetDateTime,
        ) -> Result<Option<DeletionReadAuthorization>, ApplicationError> {
            Ok((path == "/priv/locks.app/content/resource.txt").then(|| {
                DeletionReadAuthorization {
                    claim_token: Some(self.claim_token),
                    creator: creator(),
                    resource: content_lock_fixture().primary_resource.unwrap(),
                }
            }))
        }

        async fn deletion_credential_enrolled(
            &self,
            _lookup_key: &AccessCredentialLookupKey,
        ) -> Result<bool, ApplicationError> {
            Ok(true)
        }

        async fn release_deletion_read(
            &self,
            _lookup_key: &AccessCredentialLookupKey,
            _path: &str,
            claim_token: Uuid,
            _now: OffsetDateTime,
        ) -> Result<bool, ApplicationError> {
            assert_eq!(claim_token, self.claim_token);
            self.releases.fetch_add(1, Ordering::SeqCst);
            Ok(true)
        }

        async fn consume_deletion_read(
            &self,
            _lookup_key: &AccessCredentialLookupKey,
            _path: &str,
            claim_token: Uuid,
            _now: OffsetDateTime,
        ) -> Result<bool, ApplicationError> {
            assert_eq!(claim_token, self.claim_token);
            self.consumes.fetch_add(1, Ordering::SeqCst);
            Ok(true)
        }
    }

    struct Fixture {
        credentials: InMemoryAccessCredentialStore,
        entitlements: InMemoryEntitlementRepository,
        content_locks: InMemoryContentLockRepository,
        guarded_resources: InMemoryGuardedResourceRepository,
        clock: FixedClock,
        credential: AccessCredential,
    }

    impl Fixture {
        async fn seed() -> Self {
            Self::seed_with_guarded_resource(true).await
        }

        async fn seed_without_guarded_resource() -> Self {
            Self::seed_with_guarded_resource(false).await
        }

        async fn seed_with_guarded_resource(seed_guarded_resource: bool) -> Self {
            let credentials = InMemoryAccessCredentialStore::new();
            let entitlements = InMemoryEntitlementRepository::new();
            let content_locks = InMemoryContentLockRepository::new();
            let guarded_resources = InMemoryGuardedResourceRepository::new();
            let clock = FixedClock(datetime!(2026-05-29 12:00:00 UTC));
            let credential = AccessCredential::new("raw-bearer-credential");
            let content_lock = content_lock_fixture();
            let content_lock_path = content_lock.content_lock_path().unwrap();
            let pubky_lock_resource = PubkyLockResource::new(creator(), content_lock_path.clone());

            content_locks
                .upsert_content_lock(creator(), content_lock_path, content_lock.clone())
                .await
                .unwrap();
            entitlements
                .insert_verified_proof_bundle(VerifiedProofBundle {
                    version: VERIFIED_PROOF_BUNDLE_VERSION,
                    bundle_id: bundle_id(),
                    pubky_lock_resource,
                    verification_result: VerificationResult {
                        criteria: vec![CriterionVerificationResult {
                            criterion_id: "criterion-1".to_owned(),
                            satisfied: true,
                            verified_at: datetime!(2026-05-29 11:58:00 UTC),
                            verified_by: LockServerPubky::from_str(
                                "pubky7ir1ttte48bcp4zjychjyscicrwi1j34mtt91ptsafdbjmr8g9eo",
                            )
                            .unwrap(),
                            verifier_type: VerifierType::DevStatic,
                        }],
                    },
                    entitlement_lifetime: EntitlementLifetime::Unbounded,
                })
                .await
                .unwrap();
            credentials
                .insert_access_credential(
                    &content_lock.lock_id().unwrap(),
                    AccessCredentialLookupKey::derive(&credential),
                    AccessCredentialRecord {
                        creator: creator(),
                        bundle_id: bundle_id(),
                        expires_at: datetime!(2026-05-29 12:15:00 UTC),
                    },
                )
                .await
                .unwrap();
            if seed_guarded_resource {
                guarded_resources
                    .upsert_guarded_resource(GuardedResourceRecord {
                        creator: creator(),
                        path: "/priv/locks.app/content/resource.txt".to_owned(),
                        hash: GuardedResourceHash::from_bytes([7; 32]),
                        content_type: "text/plain".to_owned(),
                        size: 13,
                        bytes: b"guarded bytes".to_vec(),
                    })
                    .await
                    .unwrap();
                guarded_resources
                    .upsert_guarded_resource(GuardedResourceRecord {
                        creator: creator(),
                        path: "/priv/locks.app/content/secondary.txt".to_owned(),
                        hash: GuardedResourceHash::from_bytes([8; 32]),
                        content_type: "text/plain".to_owned(),
                        size: 15,
                        bytes: b"secondary bytes".to_vec(),
                    })
                    .await
                    .unwrap();
            }

            Self {
                credentials,
                entitlements,
                content_locks,
                guarded_resources,
                clock,
                credential,
            }
        }
    }

    fn content_lock_fixture() -> ContentLock {
        ContentLock {
            version: CONTENT_LOCK_VERSION,
            creator: creator(),
            primary_resource: Some(GuardedResource {
                path: "/priv/locks.app/content/resource.txt".to_owned(),
                hash: GuardedResourceHash::from_bytes([7; 32]),
                content_type: "text/plain".to_owned(),
                size: 13,
            }),
            secondary_resources: BTreeMap::from([(
                "/priv/locks.app/content/secondary.txt".to_owned(),
                SecondaryGuardedResource {
                    hash: GuardedResourceHash::from_bytes([8; 32]),
                    content_type: "text/plain".to_owned(),
                    size: 15,
                },
            )]),
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
            lock_server: LockServerConfig { override_: None },
            created_at: datetime!(2026-05-29 11:55:00 UTC),
        }
    }

    fn creator() -> CreatorPubky {
        CreatorPubky::from_str("pubkytkrq8zmwb8a3m9k15csu3q17qmfgqnp9dskbrg9uq1rydpyxp7qy").unwrap()
    }

    fn bundle_id() -> BundleId {
        BundleId::from_str(BUNDLE_ID).unwrap()
    }

    struct FixedClock(OffsetDateTime);

    impl Clock for FixedClock {
        fn now(&self) -> OffsetDateTime {
            self.0
        }
    }
}
