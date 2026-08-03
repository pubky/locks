use std::str::FromStr;

use async_trait::async_trait;
use serde_json::json;
use time::OffsetDateTime;
use time::macros::datetime;

use locks_core::ids::{
    BundleId, CreatorPubky, GuardedResourceHash, LockServerPubky, PubkyLockResource, TaskId,
};
use locks_core::lock_policy::{
    AccessPolicy, CONTENT_LOCK_VERSION, ContentLock, Criterion, GuardedResource, LockLogic,
    LockServerConfig, VerifierType,
};
use locks_core::verification::{Proof, SUBMITTED_PROOF_BUNDLE_VERSION, SubmittedProofBundle};

use crate::application::errors::ApplicationError;
use crate::application::models::{
    AccessCredential, AccessCredentialPolicy, GuardedResourceRecord, VerificationTaskStatus,
};
use crate::application::ports::{
    AccessCredentialGenerator, Clock, ContentLockRepository, GuardedResourceRepository,
    VerificationTaskIdGenerator,
};
use crate::application::use_cases::complete_verification_task::{
    CompleteVerificationTaskRequest, CompleteVerificationTaskUseCase,
};
use crate::application::use_cases::get_verification_task::{
    GetVerificationTaskRequest, GetVerificationTaskUseCase,
};
use crate::application::use_cases::issue_access_credential::{
    IssueAccessCredentialRequest, IssueAccessCredentialUseCase,
};
use crate::application::use_cases::proxy_read_guarded_resource::{
    ProxyReadGuardedResourceRequest, ProxyReadGuardedResourceUseCase,
};
use crate::application::use_cases::submit_proof_bundle::{
    SubmitProofBundleRequest, SubmitProofBundleUseCase,
};
use crate::application::use_cases::validate_access_credential::{
    ValidateAccessCredentialRequest, ValidateAccessCredentialUseCase,
};
use crate::infrastructure::memory::access_credentials::InMemoryAccessCredentialStore;
use crate::infrastructure::memory::content_locks::InMemoryContentLockRepository;
use crate::infrastructure::memory::entitlements::InMemoryEntitlementRepository;
use crate::infrastructure::memory::guarded_resources::InMemoryGuardedResourceRepository;
use crate::infrastructure::memory::verification_tasks::InMemoryVerificationTaskRepository;
use crate::infrastructure::verifiers::dev_static::DevStaticVerifier;
use crate::infrastructure::verifiers::registry::StaticCriterionVerifierRegistry;

const BUNDLE_ID: &str = "000G40R40M30E209185GR38E1W";
const TASK_ID: &str = "018fc6ec-2f3d-4f7e-8b7d-6f5c4b3a2d10";
const RAW_ACCESS_CREDENTIAL: &str = "test-access-credential-32-byte-value";

#[tokio::test]
async fn retrieval_access_slice_submits_verifies_issues_validates_and_proxy_reads() {
    let content_locks = InMemoryContentLockRepository::new();
    let guarded_resources = InMemoryGuardedResourceRepository::new();
    let verification_tasks = InMemoryVerificationTaskRepository::new();
    let entitlements = InMemoryEntitlementRepository::new();
    let access_credentials = InMemoryAccessCredentialStore::new();
    let clock = FixedClock(datetime!(2026-05-29 12:00:00 UTC));
    let task_ids = FixedTaskIdGenerator(task_id());
    let credential_generator =
        FixedAccessCredentialGenerator(AccessCredential::new(RAW_ACCESS_CREDENTIAL));
    let verifier = DevStaticVerifier;
    let registry = StaticCriterionVerifierRegistry::new().with_dev_static(&verifier);
    let content_lock = content_lock_fixture();
    let content_lock_path = content_lock.content_lock_path().unwrap();
    let pubky_lock_resource = PubkyLockResource::new(creator(), content_lock_path.clone());

    content_locks
        .upsert_content_lock(creator(), content_lock_path, content_lock.clone())
        .await
        .unwrap();
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

    let submitted_task = SubmitProofBundleUseCase::new(&task_ids, &verification_tasks, &clock)
        .execute(SubmitProofBundleRequest {
            submitted_proof_bundle: SubmittedProofBundle {
                version: SUBMITTED_PROOF_BUNDLE_VERSION,
                bundle_id: bundle_id(),
                pubky_lock_resource: pubky_lock_resource.clone(),
                reader_public_key: None,
                proofs: vec![Proof {
                    criterion_id: "criterion-1".to_owned(),
                    verifier_type: VerifierType::DevStatic,
                    payload: json!({}),
                }],
            },
        })
        .await
        .unwrap();

    assert_eq!(submitted_task.creator, creator());
    assert_eq!(submitted_task.bundle_id, bundle_id());
    assert_eq!(submitted_task.status, VerificationTaskStatus::Pending);
    assert_eq!(
        GetVerificationTaskUseCase::new(&verification_tasks)
            .execute(GetVerificationTaskRequest { task_id: task_id() })
            .await
            .unwrap()
            .status,
        VerificationTaskStatus::Pending
    );

    let completed = CompleteVerificationTaskUseCase::new(
        &verification_tasks,
        &content_locks,
        &entitlements,
        &registry,
        &clock,
        lock_server(),
    )
    .execute(CompleteVerificationTaskRequest { task_id: task_id() })
    .await
    .unwrap();

    assert_eq!(completed.status, VerificationTaskStatus::Completed);
    assert_eq!(
        GetVerificationTaskUseCase::new(&verification_tasks)
            .execute(GetVerificationTaskRequest { task_id: task_id() })
            .await
            .unwrap()
            .status,
        VerificationTaskStatus::Completed
    );

    let issued = IssueAccessCredentialUseCase::new(
        &entitlements,
        &content_locks,
        &access_credentials,
        &credential_generator,
        &clock,
        AccessCredentialPolicy::new(900),
    )
    .execute(IssueAccessCredentialRequest {
        creator: creator(),
        bundle_id: bundle_id(),
    })
    .await
    .unwrap();

    assert_eq!(issued.credential.as_str(), RAW_ACCESS_CREDENTIAL);
    assert_eq!(issued.expires_at, datetime!(2026-05-29 12:15:00 UTC));

    let validated = ValidateAccessCredentialUseCase::new(
        &access_credentials,
        &entitlements,
        &content_locks,
        &clock,
    )
    .execute(ValidateAccessCredentialRequest {
        credential: issued.credential.clone(),
    })
    .await
    .unwrap();

    assert_eq!(validated.creator, creator());
    assert_eq!(validated.bundle_id, bundle_id());

    let proxied = ProxyReadGuardedResourceUseCase::new(
        &access_credentials,
        &entitlements,
        &content_locks,
        &guarded_resources,
        &clock,
    )
    .execute(ProxyReadGuardedResourceRequest {
        credential: issued.credential,
        path: "/priv/locks.app/content/resource.txt".to_owned(),
    })
    .await
    .unwrap();

    assert_eq!(proxied.path, "/priv/locks.app/content/resource.txt");
    assert_eq!(proxied.bytes, b"guarded bytes".to_vec());
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
            requested_credential_ttl_seconds: 900,
        },
        lock_server: LockServerConfig { override_: None },
        created_at: datetime!(2026-05-29 11:55:00 UTC),
    }
}

fn creator() -> CreatorPubky {
    CreatorPubky::from_str("pubkytkrq8zmwb8a3m9k15csu3q17qmfgqnp9dskbrg9uq1rydpyxp7qy").unwrap()
}

fn lock_server() -> LockServerPubky {
    LockServerPubky::from_str("pubky7ir1ttte48bcp4zjychjyscicrwi1j34mtt91ptsafdbjmr8g9eo").unwrap()
}

fn bundle_id() -> BundleId {
    BundleId::from_str(BUNDLE_ID).unwrap()
}

fn task_id() -> TaskId {
    TaskId::from_str(TASK_ID).unwrap()
}

struct FixedClock(OffsetDateTime);

impl Clock for FixedClock {
    fn now(&self) -> OffsetDateTime {
        self.0
    }
}

struct FixedTaskIdGenerator(TaskId);

#[async_trait]
impl VerificationTaskIdGenerator for FixedTaskIdGenerator {
    async fn generate_task_id(&self) -> Result<TaskId, ApplicationError> {
        Ok(self.0)
    }
}

struct FixedAccessCredentialGenerator(AccessCredential);

#[async_trait]
impl AccessCredentialGenerator for FixedAccessCredentialGenerator {
    async fn generate_access_credential(&self) -> Result<AccessCredential, ApplicationError> {
        Ok(self.0.clone())
    }
}
