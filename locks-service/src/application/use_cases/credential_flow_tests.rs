use std::collections::VecDeque;
use std::str::FromStr;
use std::sync::Mutex;

use async_trait::async_trait;
use serde_json::json;
use time::OffsetDateTime;
use time::macros::datetime;

use locks_core::ids::{
    BundleId, ContentLockPath, CreatorPubky, GuardedResourceHash, LockId, LockServerPubky,
    PubkyLockResource, TaskId,
};
use locks_core::lock_policy::{
    AccessPolicy, CONTENT_LOCK_VERSION, ContentLock, Criterion, GuardedResource, LockLogic,
    LockServerConfig, VerifierType,
};
use locks_core::verification::{
    CriterionVerificationResult, EntitlementLifetime, VERIFIED_PROOF_BUNDLE_VERSION,
    VerificationResult, VerifiedProofBundle,
};

use crate::application::errors::ApplicationError;
use crate::application::models::{
    AccessCredential, AccessCredentialLookupKey, AccessCredentialPolicy, AccessCredentialRecord,
    GuardedResourceRecord, IssuedDeletionCredential, VerificationTaskRecord,
};
use crate::application::ports::{
    AccessCredentialGenerator, AccessCredentialStore, Clock, ContentLockRepository,
    EntitlementRepository, GuardedResourceRepository, VerificationTaskRepository,
};
use crate::application::use_cases::issue_access_credential::{
    IssueAccessCredentialRequest, IssueAccessCredentialUseCase,
};
use crate::application::use_cases::validate_access_credential::{
    ValidateAccessCredentialRequest, ValidateAccessCredentialUseCase,
};

const BUNDLE_ID: &str = "000G40R40M30E209185GR38E1W";

#[tokio::test]
async fn final_credential_winner_uses_fresh_time_after_generation() {
    let before_generation = datetime!(2026-05-29 12:00:00 UTC);
    let after_generation = datetime!(2026-05-29 12:00:01 UTC);
    let entitlements = FakeEntitlements::new(None);
    let content_locks = FakeContentLocks::new(None);
    let store = FakeAccessCredentialStore::with_final_credential();
    let generator = FakeGenerator::new(AccessCredential::new("final-bearer"));
    let clock = SequenceClock::new([before_generation, after_generation]);
    let use_case = IssueAccessCredentialUseCase::new(
        &entitlements,
        &content_locks,
        &store,
        &generator,
        &clock,
        AccessCredentialPolicy::new(3600),
    );

    let issued = use_case
        .execute(IssueAccessCredentialRequest {
            creator: creator(),
            bundle_id: bundle_id(),
        })
        .await
        .unwrap();

    assert_eq!(issued.credential.as_str(), "final-bearer");
    assert_eq!(store.final_issue_time(), Some(after_generation));
}

#[tokio::test]
async fn issue_access_credential_rechecks_entitlement_and_stores_lookup_key() {
    let content_lock = content_lock_fixture(900);
    let entitlement = verified_proof_bundle_fixture(&content_lock);
    let entitlements = FakeEntitlements::new(Some(entitlement));
    let content_locks = FakeContentLocks::new(Some(content_lock));
    let store = FakeAccessCredentialStore::default();
    let generator = FakeGenerator::new(AccessCredential::new("raw-bearer-credential"));
    let clock = FakeClock::new(datetime!(2026-05-29 12:00:00 UTC));
    let use_case = IssueAccessCredentialUseCase::new(
        &entitlements,
        &content_locks,
        &store,
        &generator,
        &clock,
        AccessCredentialPolicy::new(3600),
    );

    let issued = use_case
        .execute(IssueAccessCredentialRequest {
            creator: creator(),
            bundle_id: bundle_id(),
        })
        .await
        .unwrap();

    assert_eq!(issued.credential.as_str(), "raw-bearer-credential");
    assert_eq!(issued.expires_at, datetime!(2026-05-29 12:15:00 UTC));
    let (stored_lookup_key, stored_record) = store.stored_record();
    assert_eq!(
        stored_lookup_key,
        AccessCredentialLookupKey::derive(&issued.credential)
    );
    assert_eq!(stored_record.creator, creator());
    assert_eq!(stored_record.bundle_id, bundle_id());
    assert_eq!(stored_record.expires_at, issued.expires_at);
}

#[tokio::test]
async fn issue_access_credential_rejects_missing_entitlement() {
    let content_locks = FakeContentLocks::new(Some(content_lock_fixture(900)));
    let entitlements = FakeEntitlements::new(None);
    let store = FakeAccessCredentialStore::default();
    let generator = FakeGenerator::new(AccessCredential::new("raw-bearer-credential"));
    let clock = FakeClock::new(datetime!(2026-05-29 12:00:00 UTC));
    let use_case = IssueAccessCredentialUseCase::new(
        &entitlements,
        &content_locks,
        &store,
        &generator,
        &clock,
        AccessCredentialPolicy::new(3600),
    );

    let result = use_case
        .execute(IssueAccessCredentialRequest {
            creator: creator(),
            bundle_id: bundle_id(),
        })
        .await;

    assert_eq!(result, Err(ApplicationError::EntitlementNotFound));
    assert!(store.is_empty());
}

#[tokio::test]
async fn issue_access_credential_rejects_missing_content_lock() {
    let content_lock = content_lock_fixture(900);
    let entitlement = verified_proof_bundle_fixture(&content_lock);
    let entitlements = FakeEntitlements::new(Some(entitlement));
    let content_locks = FakeContentLocks::new(None);
    let store = FakeAccessCredentialStore::default();
    let generator = FakeGenerator::new(AccessCredential::new("raw-bearer-credential"));
    let clock = FakeClock::new(datetime!(2026-05-29 12:00:00 UTC));
    let use_case = IssueAccessCredentialUseCase::new(
        &entitlements,
        &content_locks,
        &store,
        &generator,
        &clock,
        AccessCredentialPolicy::new(3600),
    );

    let result = use_case
        .execute(IssueAccessCredentialRequest {
            creator: creator(),
            bundle_id: bundle_id(),
        })
        .await;

    assert_eq!(result, Err(ApplicationError::ContentLockUnavailable));
    assert!(store.is_empty());
}

#[tokio::test]
async fn issue_access_credential_rejects_unsatisfied_entitlement() {
    let content_lock = content_lock_fixture(900);
    let entitlement = verified_proof_bundle_fixture_with_satisfaction(&content_lock, false);
    let entitlements = FakeEntitlements::new(Some(entitlement));
    let content_locks = FakeContentLocks::new(Some(content_lock));
    let store = FakeAccessCredentialStore::default();
    let generator = FakeGenerator::new(AccessCredential::new("raw-bearer-credential"));
    let clock = FakeClock::new(datetime!(2026-05-29 12:00:00 UTC));
    let use_case = IssueAccessCredentialUseCase::new(
        &entitlements,
        &content_locks,
        &store,
        &generator,
        &clock,
        AccessCredentialPolicy::new(3600),
    );

    let result = use_case
        .execute(IssueAccessCredentialRequest {
            creator: creator(),
            bundle_id: bundle_id(),
        })
        .await;

    assert_eq!(result, Err(ApplicationError::EntitlementNotSatisfied));
    assert!(store.is_empty());
}

#[tokio::test]
async fn issue_access_credential_rejects_content_lock_hash_mismatch() {
    let original_content_lock = content_lock_fixture(900);
    let changed_content_lock = content_lock_fixture(901);
    let expected = original_content_lock
        .content_lock_path()
        .unwrap()
        .lock_id()
        .clone();
    let actual = changed_content_lock.lock_id().unwrap();
    let entitlement = verified_proof_bundle_fixture(&original_content_lock);
    let entitlements = FakeEntitlements::new(Some(entitlement));
    let content_locks = FakeContentLocks::new(Some(changed_content_lock));
    let store = FakeAccessCredentialStore::default();
    let generator = FakeGenerator::new(AccessCredential::new("raw-bearer-credential"));
    let clock = FakeClock::new(datetime!(2026-05-29 12:00:00 UTC));
    let use_case = IssueAccessCredentialUseCase::new(
        &entitlements,
        &content_locks,
        &store,
        &generator,
        &clock,
        AccessCredentialPolicy::new(3600),
    );

    let result = use_case
        .execute(IssueAccessCredentialRequest {
            creator: creator(),
            bundle_id: bundle_id(),
        })
        .await;

    assert_eq!(
        result,
        Err(ApplicationError::ContentLockHashMismatch { expected, actual })
    );
    assert!(store.is_empty());
}

#[tokio::test]
async fn issue_access_credential_rejects_unsupported_ttl_before_generating_or_storing() {
    let content_lock = content_lock_fixture(7200);
    let entitlement = verified_proof_bundle_fixture(&content_lock);
    let entitlements = FakeEntitlements::new(Some(entitlement));
    let content_locks = FakeContentLocks::new(Some(content_lock));
    let store = FakeAccessCredentialStore::default();
    let generator = FakeGenerator::new(AccessCredential::new("raw-bearer-credential"));
    let clock = FakeClock::new(datetime!(2026-05-29 12:00:00 UTC));
    let use_case = IssueAccessCredentialUseCase::new(
        &entitlements,
        &content_locks,
        &store,
        &generator,
        &clock,
        AccessCredentialPolicy::new(3600),
    );

    let result = use_case
        .execute(IssueAccessCredentialRequest {
            creator: creator(),
            bundle_id: bundle_id(),
        })
        .await;

    assert_eq!(
        result,
        Err(ApplicationError::UnsupportedCredentialTtl {
            requested_seconds: 7200,
            max_seconds: 3600,
        })
    );
    assert!(store.is_empty());
    assert_eq!(generator.generate_count(), 0);
}

#[tokio::test]
async fn validate_access_credential_returns_authorized_context_when_unexpired_and_entitled() {
    let credential = AccessCredential::new("raw-bearer-credential");
    let lookup_key = AccessCredentialLookupKey::derive(&credential);
    let store = FakeAccessCredentialStore::with_record(
        lookup_key,
        AccessCredentialRecord {
            creator: creator(),
            bundle_id: bundle_id(),
            expires_at: datetime!(2026-05-29 12:15:00 UTC),
        },
    );
    let content_lock = content_lock_fixture(900);
    let entitlement = verified_proof_bundle_fixture(&content_lock);
    let entitlements = FakeEntitlements::new(Some(entitlement));
    let content_locks = FakeContentLocks::new(Some(content_lock));
    let clock = FakeClock::new(datetime!(2026-05-29 12:00:00 UTC));
    let use_case =
        ValidateAccessCredentialUseCase::new(&store, &entitlements, &content_locks, &clock);

    let validated = use_case
        .execute(ValidateAccessCredentialRequest { credential })
        .await
        .unwrap();

    assert_eq!(validated.creator, creator());
    assert_eq!(validated.bundle_id, bundle_id());
    assert_eq!(validated.expires_at, datetime!(2026-05-29 12:15:00 UTC));
    assert_eq!(store.deleted_lookup_key(), None);
}

#[tokio::test]
async fn validate_access_credential_rejects_unknown_credential_without_delete() {
    let credential = AccessCredential::new("raw-bearer-credential");
    let store = FakeAccessCredentialStore::default();
    let entitlements = FakeEntitlements::new(None);
    let content_locks = FakeContentLocks::new(None);
    let clock = FakeClock::new(datetime!(2026-05-29 12:00:00 UTC));
    let use_case =
        ValidateAccessCredentialUseCase::new(&store, &entitlements, &content_locks, &clock);

    let result = use_case
        .execute(ValidateAccessCredentialRequest { credential })
        .await;

    assert_eq!(result, Err(ApplicationError::InvalidAccessCredential));
    assert_eq!(store.deleted_lookup_key(), None);
}

#[tokio::test]
async fn validate_access_credential_deletes_when_entitlement_no_longer_satisfies_lock() {
    let credential = AccessCredential::new("raw-bearer-credential");
    let lookup_key = AccessCredentialLookupKey::derive(&credential);
    let store = FakeAccessCredentialStore::with_record(
        lookup_key.clone(),
        AccessCredentialRecord {
            creator: creator(),
            bundle_id: bundle_id(),
            expires_at: datetime!(2026-05-29 12:15:00 UTC),
        },
    );
    let content_lock = content_lock_fixture(900);
    let entitlement = verified_proof_bundle_fixture_with_satisfaction(&content_lock, false);
    let entitlements = FakeEntitlements::new(Some(entitlement));
    let content_locks = FakeContentLocks::new(Some(content_lock));
    let clock = FakeClock::new(datetime!(2026-05-29 12:00:00 UTC));
    let use_case =
        ValidateAccessCredentialUseCase::new(&store, &entitlements, &content_locks, &clock);

    let result = use_case
        .execute(ValidateAccessCredentialRequest { credential })
        .await;

    assert_eq!(result, Err(ApplicationError::EntitlementNotSatisfied));
    assert_eq!(store.deleted_lookup_key(), Some(lookup_key));
}

#[tokio::test]
async fn validate_access_credential_deletes_expired_credential() {
    let credential = AccessCredential::new("raw-bearer-credential");
    let lookup_key = AccessCredentialLookupKey::derive(&credential);
    let store = FakeAccessCredentialStore::with_record(
        lookup_key.clone(),
        AccessCredentialRecord {
            creator: creator(),
            bundle_id: bundle_id(),
            expires_at: datetime!(2026-05-29 12:00:00 UTC),
        },
    );
    let entitlements = FakeEntitlements::new(None);
    let content_locks = FakeContentLocks::new(None);
    let clock = FakeClock::new(datetime!(2026-05-29 12:00:01 UTC));
    let use_case =
        ValidateAccessCredentialUseCase::new(&store, &entitlements, &content_locks, &clock);

    let result = use_case
        .execute(ValidateAccessCredentialRequest { credential })
        .await;

    assert_eq!(result, Err(ApplicationError::ExpiredAccessCredential));
    assert_eq!(store.deleted_lookup_key(), Some(lookup_key));
}

#[tokio::test]
async fn validate_access_credential_deletes_when_entitlement_is_revoked() {
    let credential = AccessCredential::new("raw-bearer-credential");
    let lookup_key = AccessCredentialLookupKey::derive(&credential);
    let store = FakeAccessCredentialStore::with_record(
        lookup_key.clone(),
        AccessCredentialRecord {
            creator: creator(),
            bundle_id: bundle_id(),
            expires_at: datetime!(2026-05-29 12:15:00 UTC),
        },
    );
    let entitlements = FakeEntitlements::new(None);
    let content_locks = FakeContentLocks::new(Some(content_lock_fixture(900)));
    let clock = FakeClock::new(datetime!(2026-05-29 12:00:00 UTC));
    let use_case =
        ValidateAccessCredentialUseCase::new(&store, &entitlements, &content_locks, &clock);

    let result = use_case
        .execute(ValidateAccessCredentialRequest { credential })
        .await;

    assert_eq!(result, Err(ApplicationError::EntitlementNotFound));
    assert_eq!(store.deleted_lookup_key(), Some(lookup_key));
}

#[tokio::test]
async fn validate_access_credential_deletes_when_content_lock_hash_mismatches_path() {
    let credential = AccessCredential::new("raw-bearer-credential");
    let lookup_key = AccessCredentialLookupKey::derive(&credential);
    let store = FakeAccessCredentialStore::with_record(
        lookup_key.clone(),
        AccessCredentialRecord {
            creator: creator(),
            bundle_id: bundle_id(),
            expires_at: datetime!(2026-05-29 12:15:00 UTC),
        },
    );
    let original_content_lock = content_lock_fixture(900);
    let changed_content_lock = content_lock_fixture(901);
    let expected = original_content_lock
        .content_lock_path()
        .unwrap()
        .lock_id()
        .clone();
    let actual = changed_content_lock.lock_id().unwrap();
    let entitlement = verified_proof_bundle_fixture(&original_content_lock);
    let entitlements = FakeEntitlements::new(Some(entitlement));
    let content_locks = FakeContentLocks::new(Some(changed_content_lock));
    let clock = FakeClock::new(datetime!(2026-05-29 12:00:00 UTC));
    let use_case =
        ValidateAccessCredentialUseCase::new(&store, &entitlements, &content_locks, &clock);

    let result = use_case
        .execute(ValidateAccessCredentialRequest { credential })
        .await;

    assert_eq!(
        result,
        Err(ApplicationError::ContentLockHashMismatch { expected, actual })
    );
    assert_eq!(store.deleted_lookup_key(), Some(lookup_key));
}

fn creator() -> CreatorPubky {
    CreatorPubky::from_str("pubkytkrq8zmwb8a3m9k15csu3q17qmfgqnp9dskbrg9uq1rydpyxp7qy").unwrap()
}

fn server() -> LockServerPubky {
    LockServerPubky::from_str("pubky7ir1ttte48bcp4zjychjyscicrwi1j34mtt91ptsafdbjmr8g9eo").unwrap()
}

fn bundle_id() -> BundleId {
    BundleId::from_str(BUNDLE_ID).unwrap()
}

fn content_lock_fixture(requested_ttl_seconds: u64) -> ContentLock {
    ContentLock {
        version: CONTENT_LOCK_VERSION,
        creator: creator(),
        primary_resource: Some(GuardedResource {
            path: "/priv/locks.app/content/resource.txt".to_owned(),
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
            requested_credential_ttl_seconds: requested_ttl_seconds,
        },
        lock_server: LockServerConfig {
            override_: Some(server()),
        },
        created_at: datetime!(2026-05-29 12:00:00 UTC),
    }
}

fn verified_proof_bundle_fixture(content_lock: &ContentLock) -> VerifiedProofBundle {
    verified_proof_bundle_fixture_with_satisfaction(content_lock, true)
}

fn verified_proof_bundle_fixture_with_satisfaction(
    content_lock: &ContentLock,
    satisfied: bool,
) -> VerifiedProofBundle {
    VerifiedProofBundle {
        version: VERIFIED_PROOF_BUNDLE_VERSION,
        bundle_id: bundle_id(),
        pubky_lock_resource: PubkyLockResource::new(
            creator(),
            content_lock.content_lock_path().unwrap(),
        ),
        verification_result: VerificationResult {
            criteria: vec![CriterionVerificationResult {
                criterion_id: "criterion-1".to_owned(),
                satisfied,
                verified_at: datetime!(2026-05-29 12:00:00 UTC),
                verified_by: server(),
                verifier_type: VerifierType::DevStatic,
            }],
        },
        entitlement_lifetime: EntitlementLifetime::Unbounded,
    }
}

struct FakeContentLocks {
    content_lock: Mutex<Option<ContentLock>>,
}

impl FakeContentLocks {
    fn new(content_lock: Option<ContentLock>) -> Self {
        Self {
            content_lock: Mutex::new(content_lock),
        }
    }
}

#[async_trait]
impl ContentLockRepository for FakeContentLocks {
    async fn upsert_content_lock(
        &self,
        _creator: CreatorPubky,
        _content_lock_path: ContentLockPath,
        content_lock: ContentLock,
    ) -> Result<(), ApplicationError> {
        *self.content_lock.lock().unwrap() = Some(content_lock);
        Ok(())
    }

    async fn get_content_lock(
        &self,
        _creator: &CreatorPubky,
        _content_lock_path: &ContentLockPath,
    ) -> Result<Option<ContentLock>, ApplicationError> {
        Ok(self.content_lock.lock().unwrap().clone())
    }

    async fn delete_content_lock(
        &self,
        _creator: &CreatorPubky,
        _content_lock_path: &ContentLockPath,
    ) -> Result<bool, ApplicationError> {
        unreachable!("credential flow must not delete content locks")
    }
}

struct FakeEntitlements {
    entitlement: Mutex<Option<VerifiedProofBundle>>,
}

impl FakeEntitlements {
    fn new(entitlement: Option<VerifiedProofBundle>) -> Self {
        Self {
            entitlement: Mutex::new(entitlement),
        }
    }
}

#[async_trait]
impl EntitlementRepository for FakeEntitlements {
    async fn insert_verified_proof_bundle(
        &self,
        verified_proof_bundle: VerifiedProofBundle,
    ) -> Result<(), ApplicationError> {
        *self.entitlement.lock().unwrap() = Some(verified_proof_bundle);
        Ok(())
    }

    async fn get_verified_proof_bundle(
        &self,
        _creator: &CreatorPubky,
        _bundle_id: &BundleId,
    ) -> Result<Option<VerifiedProofBundle>, ApplicationError> {
        Ok(self.entitlement.lock().unwrap().clone())
    }

    async fn delete_verified_proof_bundle(
        &self,
        _creator: &CreatorPubky,
        _bundle_id: &BundleId,
    ) -> Result<(), ApplicationError> {
        *self.entitlement.lock().unwrap() = None;
        Ok(())
    }
}

#[derive(Default)]
struct FakeAccessCredentialStore {
    record: Mutex<Option<(AccessCredentialLookupKey, AccessCredentialRecord)>>,
    deleted: Mutex<Option<AccessCredentialLookupKey>>,
    final_available: bool,
    final_issue_time: Mutex<Option<OffsetDateTime>>,
}

impl FakeAccessCredentialStore {
    fn with_record(lookup_key: AccessCredentialLookupKey, record: AccessCredentialRecord) -> Self {
        Self {
            record: Mutex::new(Some((lookup_key, record))),
            deleted: Mutex::new(None),
            final_available: false,
            final_issue_time: Mutex::new(None),
        }
    }

    fn with_final_credential() -> Self {
        Self {
            final_available: true,
            ..Self::default()
        }
    }

    fn final_issue_time(&self) -> Option<OffsetDateTime> {
        *self.final_issue_time.lock().unwrap()
    }

    fn stored_record(&self) -> (AccessCredentialLookupKey, AccessCredentialRecord) {
        self.record.lock().unwrap().clone().unwrap()
    }

    fn deleted_lookup_key(&self) -> Option<AccessCredentialLookupKey> {
        self.deleted.lock().unwrap().clone()
    }

    fn is_empty(&self) -> bool {
        self.record.lock().unwrap().is_none()
    }
}

#[async_trait]
impl AccessCredentialStore for FakeAccessCredentialStore {
    async fn insert_access_credential(
        &self,
        _lock_id: &LockId,
        lookup_key: AccessCredentialLookupKey,
        record: AccessCredentialRecord,
    ) -> Result<(), ApplicationError> {
        *self.record.lock().unwrap() = Some((lookup_key, record));
        Ok(())
    }

    async fn get_access_credential(
        &self,
        lookup_key: &AccessCredentialLookupKey,
    ) -> Result<Option<AccessCredentialRecord>, ApplicationError> {
        Ok(self
            .record
            .lock()
            .unwrap()
            .as_ref()
            .filter(|(stored_lookup_key, _)| stored_lookup_key == lookup_key)
            .map(|(_, record)| record.clone()))
    }

    async fn delete_access_credential(
        &self,
        lookup_key: &AccessCredentialLookupKey,
    ) -> Result<(), ApplicationError> {
        *self.deleted.lock().unwrap() = Some(lookup_key.clone());
        *self.record.lock().unwrap() = None;
        Ok(())
    }

    async fn final_credential_available(
        &self,
        _creator: &CreatorPubky,
        _bundle_id: &BundleId,
        _now: OffsetDateTime,
    ) -> Result<bool, ApplicationError> {
        Ok(self.final_available)
    }

    async fn issue_or_replay_final_credential(
        &self,
        _creator: &CreatorPubky,
        _bundle_id: &BundleId,
        now: OffsetDateTime,
        candidate: AccessCredential,
    ) -> Result<Option<IssuedDeletionCredential>, ApplicationError> {
        *self.final_issue_time.lock().unwrap() = Some(now);
        Ok(Some(IssuedDeletionCredential {
            credential: candidate,
            expires_at: now + time::Duration::minutes(1),
        }))
    }
}

struct FakeGenerator {
    credential: AccessCredential,
    generate_count: Mutex<usize>,
}

impl FakeGenerator {
    fn new(credential: AccessCredential) -> Self {
        Self {
            credential,
            generate_count: Mutex::new(0),
        }
    }

    fn generate_count(&self) -> usize {
        *self.generate_count.lock().unwrap()
    }
}

#[async_trait]
impl AccessCredentialGenerator for FakeGenerator {
    async fn generate_access_credential(&self) -> Result<AccessCredential, ApplicationError> {
        *self.generate_count.lock().unwrap() += 1;
        Ok(self.credential.clone())
    }
}

struct FakeClock {
    now: OffsetDateTime,
}

impl FakeClock {
    fn new(now: OffsetDateTime) -> Self {
        Self { now }
    }
}

impl Clock for FakeClock {
    fn now(&self) -> OffsetDateTime {
        self.now
    }
}

struct SequenceClock {
    values: Mutex<VecDeque<OffsetDateTime>>,
}

impl SequenceClock {
    fn new(values: impl IntoIterator<Item = OffsetDateTime>) -> Self {
        Self {
            values: Mutex::new(values.into_iter().collect()),
        }
    }
}

impl Clock for SequenceClock {
    fn now(&self) -> OffsetDateTime {
        self.values
            .lock()
            .unwrap()
            .pop_front()
            .expect("sequence clock exhausted")
    }
}

#[allow(dead_code)]
struct UnusedPortImplementations;

#[async_trait]
impl GuardedResourceRepository for UnusedPortImplementations {
    async fn upsert_guarded_resource(
        &self,
        _guarded_resource: GuardedResourceRecord,
    ) -> Result<(), ApplicationError> {
        unreachable!()
    }

    async fn get_guarded_resource(
        &self,
        _creator: &CreatorPubky,
        _path: &str,
        _hash: &GuardedResourceHash,
    ) -> Result<Option<GuardedResourceRecord>, ApplicationError> {
        unreachable!()
    }

    async fn get_current_guarded_resource(
        &self,
        _creator: &CreatorPubky,
        _path: &str,
    ) -> Result<Option<GuardedResourceRecord>, ApplicationError> {
        unreachable!()
    }

    async fn delete_guarded_resource(
        &self,
        _creator: &CreatorPubky,
        _path: &str,
    ) -> Result<bool, ApplicationError> {
        unreachable!()
    }
}

#[async_trait]
impl VerificationTaskRepository for UnusedPortImplementations {
    async fn insert_verification_task(
        &self,
        _task: VerificationTaskRecord,
    ) -> Result<(), ApplicationError> {
        unreachable!()
    }

    async fn update_verification_task(
        &self,
        _task: VerificationTaskRecord,
    ) -> Result<(), ApplicationError> {
        unreachable!()
    }

    async fn get_verification_task(
        &self,
        _task_id: &TaskId,
    ) -> Result<Option<VerificationTaskRecord>, ApplicationError> {
        unreachable!()
    }

    async fn delete_verification_task(&self, _task_id: &TaskId) -> Result<(), ApplicationError> {
        unreachable!()
    }
}
