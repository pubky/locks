use locks_core::ids::{BundleId, CreatorPubky};
use locks_core::verification::SubmittedProofBundle;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::creator::encode_content_path;
use crate::{LocksSdkError, Result};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ViewerLocks;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SdkViewerRequest {
    pub method: &'static str,
    pub path: String,
    pub authorization: Option<String>,
    pub body: Value,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct VerificationTaskHandleRequest {
    pub creator: CreatorPubky,
    pub bundle_id: BundleId,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReadLockedResourceRequest {
    pub credential: String,
    pub path: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
struct SubmitProofBundleRequest {
    submitted_proof_bundle: SubmittedProofBundle,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum VerificationTaskStatus {
    Pending,
    InProgress,
    Completed,
    Failed,
    Expired,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct VerificationTaskLifecycleResponse {
    pub creator: CreatorPubky,
    pub bundle_id: BundleId,
    pub status: VerificationTaskStatus,
    #[serde(with = "time::serde::rfc3339")]
    pub submitted_at: time::OffsetDateTime,
    #[serde(with = "time::serde::rfc3339::option")]
    pub started_at: Option<time::OffsetDateTime>,
    #[serde(with = "time::serde::rfc3339::option")]
    pub completed_at: Option<time::OffsetDateTime>,
    pub failure_message: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AccessCredentialResponse {
    pub credential: String,
    #[serde(with = "time::serde::rfc3339")]
    pub expires_at: time::OffsetDateTime,
}

impl ViewerLocks {
    pub fn new() -> Self {
        Self
    }

    pub fn submit_proof_bundle(
        &self,
        submitted_proof_bundle: SubmittedProofBundle,
    ) -> SdkViewerRequest {
        self.post_json(
            "/proof-bundles",
            SubmitProofBundleRequest {
                submitted_proof_bundle,
            },
        )
    }

    pub fn lookup_verification_task(
        &self,
        request: VerificationTaskHandleRequest,
    ) -> SdkViewerRequest {
        self.post_json("/verification-task-lookups", request)
    }

    pub fn issue_access_credential(
        &self,
        request: VerificationTaskHandleRequest,
    ) -> SdkViewerRequest {
        self.post_json("/access-credentials", request)
    }

    pub fn complete_verification_task(
        &self,
        request: VerificationTaskHandleRequest,
    ) -> SdkViewerRequest {
        self.post_json("/verification-task-completions", request)
    }

    pub fn read_locked_resource(&self, request: ReadLockedResourceRequest) -> SdkViewerRequest {
        SdkViewerRequest {
            method: "GET",
            path: format!(
                "/priv-resources/content/{}",
                encode_content_path(&request.path)
            ),
            authorization: Some(format!("Bearer {}", request.credential)),
            body: Value::Null,
        }
    }

    pub fn proxy_read_guarded_resource(
        &self,
        access_credential: impl AsRef<str>,
        path: impl Into<String>,
    ) -> SdkViewerRequest {
        self.read_locked_resource(ReadLockedResourceRequest {
            credential: access_credential.as_ref().to_owned(),
            path: path.into(),
        })
    }

    pub fn parse_lifecycle_response(value: Value) -> Result<VerificationTaskLifecycleResponse> {
        serde_json::from_value(value).map_err(|err| LocksSdkError::InvalidResponse(err.to_string()))
    }

    pub fn parse_access_credential_response(value: Value) -> Result<AccessCredentialResponse> {
        serde_json::from_value(value).map_err(|err| LocksSdkError::InvalidResponse(err.to_string()))
    }

    fn post_json(&self, path: &'static str, body: impl Serialize) -> SdkViewerRequest {
        SdkViewerRequest {
            method: "POST",
            path: path.to_owned(),
            authorization: None,
            body: serde_json::to_value(body).expect("SDK viewer request bodies serialize to JSON"),
        }
    }
}

impl Default for ViewerLocks {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use locks_core::ids::{BundleId, PubkyLockResource};
    use locks_core::lock_policy::VerifierType;
    use locks_core::verification::{Proof, SUBMITTED_PROOF_BUNDLE_VERSION, SubmittedProofBundle};
    use serde_json::json;

    use super::*;

    const BUNDLE_ID: &str = "000G40R40M30E209185GR38E1W";
    const CREATOR: &str = "pubkytkrq8zmwb8a3m9k15csu3q17qmfgqnp9dskbrg9uq1rydpyxp7qy";
    const LOCK_ID: &str = "000G40R40M30E209185GR38E1W8124GK2GAHC5RR34D1P70X3RFG";

    #[test]
    fn submit_proof_bundle_request_uses_public_envelope_without_auth_or_task_id() {
        let viewer = ViewerLocks::new();
        let submitted = submitted_proof_bundle();

        let request = viewer.submit_proof_bundle(submitted.clone());

        assert_eq!(request.method, "POST");
        assert_eq!(request.path, "/proof-bundles");
        assert_eq!(request.authorization, None);
        assert_eq!(request.body, json!({ "submitted_proof_bundle": submitted }));
        assert!(request.body.get("task_id").is_none());
    }

    #[test]
    fn verification_task_lookup_request_keeps_bundle_id_in_json_body() {
        let viewer = ViewerLocks::new();
        let request = viewer.lookup_verification_task(VerificationTaskHandleRequest {
            creator: CREATOR.parse().unwrap(),
            bundle_id: BundleId::from_str(BUNDLE_ID).unwrap(),
        });

        assert_eq!(request.method, "POST");
        assert_eq!(request.path, "/verification-task-lookups");
        assert_eq!(request.authorization, None);
        assert_eq!(
            request.body,
            json!({ "creator": CREATOR, "bundle_id": BUNDLE_ID })
        );
        assert!(!request.path.contains(BUNDLE_ID));
    }

    #[test]
    fn issue_access_credential_request_uses_handle_body_without_task_id() {
        let viewer = ViewerLocks::new();
        let request = viewer.issue_access_credential(VerificationTaskHandleRequest {
            creator: CREATOR.parse().unwrap(),
            bundle_id: BundleId::from_str(BUNDLE_ID).unwrap(),
        });

        assert_eq!(request.method, "POST");
        assert_eq!(request.path, "/access-credentials");
        assert_eq!(request.authorization, None);
        assert_eq!(
            request.body,
            json!({ "creator": CREATOR, "bundle_id": BUNDLE_ID })
        );
        assert!(request.body.get("task_id").is_none());
    }

    #[test]
    fn complete_verification_task_request_uses_handle_body_without_task_id() {
        let viewer = ViewerLocks::new();
        let request = viewer.complete_verification_task(VerificationTaskHandleRequest {
            creator: CREATOR.parse().unwrap(),
            bundle_id: BundleId::from_str(BUNDLE_ID).unwrap(),
        });

        assert_eq!(request.method, "POST");
        assert_eq!(request.path, "/verification-task-completions");
        assert_eq!(request.authorization, None);
        assert_eq!(
            request.body,
            json!({ "creator": CREATOR, "bundle_id": BUNDLE_ID })
        );
        assert!(request.body.get("task_id").is_none());
    }

    #[test]
    fn proxy_read_guarded_resource_request_puts_credential_only_in_authorization_header() {
        let viewer = ViewerLocks::new();
        let request = viewer.read_locked_resource(ReadLockedResourceRequest {
            credential: "raw-access-credential".to_owned(),
            path: "nested/example file.txt".to_owned(),
        });

        assert_eq!(request.method, "GET");
        assert_eq!(
            request.path,
            "/priv-resources/content/nested/example%20file.txt"
        );
        assert_eq!(
            request.authorization,
            Some("Bearer raw-access-credential".to_owned())
        );
        assert_eq!(request.body, serde_json::Value::Null);
        assert!(!request.path.contains("raw-access-credential"));
        assert!(!request.path.contains("credential"));
    }

    #[test]
    fn lifecycle_response_parses_public_view_without_task_id_or_credentials() {
        let response = ViewerLocks::parse_lifecycle_response(json!({
            "creator": CREATOR,
            "bundle_id": BUNDLE_ID,
            "status": "completed",
            "submitted_at": "2026-06-01T12:00:00Z",
            "started_at": "2026-06-01T12:00:01Z",
            "completed_at": "2026-06-01T12:00:02Z",
            "failure_message": null
        }))
        .unwrap();

        assert_eq!(response.creator.to_string(), CREATOR);
        assert_eq!(response.bundle_id.to_string(), BUNDLE_ID);
        assert_eq!(response.status, VerificationTaskStatus::Completed);
        assert!(response.failure_message.is_none());
    }

    #[test]
    fn lifecycle_response_rejects_internal_task_id_and_credentials() {
        let result = ViewerLocks::parse_lifecycle_response(json!({
            "creator": CREATOR,
            "bundle_id": BUNDLE_ID,
            "status": "pending",
            "submitted_at": "2026-06-01T12:00:00Z",
            "started_at": null,
            "completed_at": null,
            "failure_message": null,
            "task_id": "018fc6ec-2f3d-4f7e-8b7d-6f5c4b3a2d10",
            "credential": "raw-secret"
        }));

        assert!(result.is_err());
    }

    #[test]
    fn access_credential_response_parses_raw_credential_once() {
        let response = ViewerLocks::parse_access_credential_response(json!({
            "credential": "raw-access-credential",
            "expires_at": "2026-06-01T12:15:00Z"
        }))
        .unwrap();

        assert_eq!(response.credential, "raw-access-credential");
        assert_eq!(
            response.expires_at,
            time::OffsetDateTime::parse(
                "2026-06-01T12:15:00Z",
                &time::format_description::well_known::Rfc3339
            )
            .unwrap()
        );
    }

    #[test]
    fn access_credential_response_rejects_extra_entitlement_or_proof_material() {
        let result = ViewerLocks::parse_access_credential_response(json!({
            "credential": "raw-access-credential",
            "expires_at": "2026-06-01T12:15:00Z",
            "submitted_proof_bundle": { "not": "viewer safe" }
        }));

        assert!(result.is_err());
    }

    fn submitted_proof_bundle() -> SubmittedProofBundle {
        SubmittedProofBundle {
            version: SUBMITTED_PROOF_BUNDLE_VERSION,
            bundle_id: BundleId::from_str(BUNDLE_ID).unwrap(),
            pubky_lock_resource: PubkyLockResource::from_str(&format!(
                "{CREATOR}/pub/locks.app/{LOCK_ID}.json"
            ))
            .unwrap(),
            reader_public_key: None,
            proofs: vec![Proof {
                criterion_id: "criterion-1".to_owned(),
                verifier_type: VerifierType::DevStatic,
                payload: json!({ "satisfied": true }),
            }],
        }
    }
}
