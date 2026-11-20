pub use locks_core::creator_publishing::{
    CreateContentLockPayload, CreateContentLockRequest, CreateContentLockV2Request,
    SetLockServicePointerRequest,
};
use locks_core::ids::{BundleId, CreatorPubky, LockId};
pub use locks_core::lock_policy::PaykitPaymentAsset;
use percent_encoding::{AsciiSet, CONTROLS, utf8_percent_encode};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::session::LocksSession;

const PATH_SEGMENT_ENCODE_SET: &AsciiSet = &CONTROLS
    .add(b' ')
    .add(b'"')
    .add(b'#')
    .add(b'%')
    .add(b'<')
    .add(b'>')
    .add(b'?')
    .add(b'`')
    .add(b'{')
    .add(b'}')
    .add(b'[')
    .add(b']')
    .add(b'\\')
    .add(b'^')
    .add(b'|')
    .add(b'/');

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreatorLocks {
    session: LocksSession,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SdkRequest {
    pub method: &'static str,
    pub path: String,
    pub authorization: String,
    pub content_type: String,
    pub body: SdkRequestBody,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SdkRequestBody {
    Json(Value),
    Bytes(Vec<u8>),
    Empty,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PaykitSetupStatusKind {
    Ready,
    SetupRequired,
    Unavailable,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PaykitSetupStatus {
    pub status: PaykitSetupStatusKind,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CreatorAuthorityAuthKind {
    LegacyCookie,
    Grant,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CreatorAuthorityStatus {
    pub creator: CreatorPubky,
    pub authorized: bool,
    pub auth_kind: Option<CreatorAuthorityAuthKind>,
    pub granted_scopes: Vec<String>,
    #[serde(with = "time::serde::rfc3339::option")]
    pub session_expires_at: Option<time::OffsetDateTime>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RegisterGuardedResourceRequest {
    pub path: String,
    pub content_type: String,
    pub bytes: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeleteGuardedResourceRequest {
    pub path: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DeleteContentLockMode {
    DefaultGraceful,
    ExplicitGraceful,
    Force,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeleteContentLockRequest {
    pub lock_id: LockId,
    pub mode: DeleteContentLockMode,
}

impl CreatorLocks {
    pub fn new(session: LocksSession) -> Self {
        Self { session }
    }

    pub fn register_guarded_resource_request(
        &self,
        request: RegisterGuardedResourceRequest,
    ) -> SdkRequest {
        SdkRequest {
            method: "PUT",
            path: format!(
                "/creator/priv-resources/content/{}",
                encode_content_path(&request.path)
            ),
            authorization: self.session.authorization_header_value(),
            content_type: request.content_type,
            body: SdkRequestBody::Bytes(request.bytes),
        }
    }

    pub fn register_guarded_resource(&self, request: RegisterGuardedResourceRequest) -> SdkRequest {
        self.register_guarded_resource_request(request)
    }

    pub fn delete_guarded_resource_request(
        &self,
        request: DeleteGuardedResourceRequest,
    ) -> SdkRequest {
        SdkRequest {
            method: "DELETE",
            path: format!(
                "/creator/priv-resources/content/{}",
                encode_content_path(&request.path)
            ),
            authorization: self.session.authorization_header_value(),
            content_type: String::new(),
            body: SdkRequestBody::Bytes(Vec::new()),
        }
    }

    pub fn delete_guarded_resource(&self, request: DeleteGuardedResourceRequest) -> SdkRequest {
        self.delete_guarded_resource_request(request)
    }

    pub fn delete_content_lock(&self, request: DeleteContentLockRequest) -> SdkRequest {
        let suffix = match request.mode {
            DeleteContentLockMode::DefaultGraceful => "",
            DeleteContentLockMode::ExplicitGraceful => "?graceful=true",
            DeleteContentLockMode::Force => "?force=true",
        };
        self.empty_request(
            "DELETE",
            format!("/creator/content-locks/{}{suffix}", request.lock_id),
        )
    }

    pub fn get_content_lock_deletion(&self, lock_id: LockId) -> SdkRequest {
        SdkRequest {
            method: "GET",
            path: format!("/creator/content-locks/{lock_id}/deletion"),
            authorization: self.session.authorization_header_value(),
            content_type: String::new(),
            body: SdkRequestBody::Empty,
        }
    }

    pub fn create_content_lock_request(&self, request: CreateContentLockRequest) -> SdkRequest {
        self.post_json("/creator/content-locks", request)
    }

    pub fn create_content_lock(&self, request: impl Into<CreateContentLockPayload>) -> SdkRequest {
        self.post_json("/creator/content-locks", request.into())
    }

    pub fn set_lock_service_pointer_request(
        &self,
        request: SetLockServicePointerRequest,
    ) -> SdkRequest {
        self.post_json("/creator/lock-service-config", request)
    }

    pub fn set_lock_service_pointer(&self, request: SetLockServicePointerRequest) -> SdkRequest {
        self.set_lock_service_pointer_request(request)
    }

    pub fn paykit_setup_status_request(&self, asset: PaykitPaymentAsset) -> SdkRequest {
        SdkRequest {
            method: "GET",
            path: format!("/creator/paykit/setup-status?asset={}", asset.as_str()),
            authorization: self.session.authorization_header_value(),
            content_type: String::new(),
            body: SdkRequestBody::Empty,
        }
    }

    pub fn paykit_setup_status(&self, asset: PaykitPaymentAsset) -> SdkRequest {
        self.paykit_setup_status_request(asset)
    }

    pub fn authority_status_request(&self) -> SdkRequest {
        SdkRequest {
            method: "GET",
            path: "/creator/authority-status".to_owned(),
            authorization: self.session.authorization_header_value(),
            content_type: String::new(),
            body: SdkRequestBody::Empty,
        }
    }

    pub fn authority_status(&self) -> SdkRequest {
        self.authority_status_request()
    }

    pub fn verification_evidence(&self, bundle_id: BundleId) -> SdkRequest {
        self.post_json(
            "/creator/verification-evidence",
            serde_json::json!({ "bundle_id": bundle_id }),
        )
    }

    pub fn parse_verification_evidence_response(
        value: Value,
    ) -> crate::Result<crate::CreatorVerificationEvidenceResponse> {
        crate::graph::parse_creator_verification_evidence_response(value)
    }

    pub fn parse_mixed_verification_evidence_response(
        value: Value,
    ) -> crate::Result<crate::CreatorMixedVerificationEvidenceResponse> {
        crate::mixed::parse_creator_mixed_verification_evidence_response(value)
    }

    pub fn parse_paykit_setup_status_response(value: Value) -> crate::Result<PaykitSetupStatus> {
        serde_json::from_value(value).map_err(|_| {
            crate::LocksSdkError::InvalidResponse("invalid paykit setup status response".to_owned())
        })
    }

    pub fn parse_authority_status_response(value: Value) -> crate::Result<CreatorAuthorityStatus> {
        let status: CreatorAuthorityStatus = serde_json::from_value(value).map_err(|_| {
            crate::LocksSdkError::InvalidResponse(
                "invalid creator authority status response".to_owned(),
            )
        })?;
        let canonical = if status.authorized {
            status.auth_kind.is_some()
        } else {
            status.auth_kind.is_none()
                && status.granted_scopes.is_empty()
                && status.session_expires_at.is_none()
        };
        if !canonical {
            return Err(crate::LocksSdkError::InvalidResponse(
                "invalid creator authority status response".to_owned(),
            ));
        }
        Ok(status)
    }

    fn post_json(&self, path: &'static str, body: impl Serialize) -> SdkRequest {
        SdkRequest {
            method: "POST",
            path: path.to_owned(),
            authorization: self.session.authorization_header_value(),
            content_type: "application/json".to_owned(),
            body: SdkRequestBody::Json(
                serde_json::to_value(body).expect("SDK request bodies serialize to JSON"),
            ),
        }
    }

    fn empty_request(&self, method: &'static str, path: String) -> SdkRequest {
        SdkRequest {
            method,
            path,
            authorization: self.session.authorization_header_value(),
            content_type: String::new(),
            body: SdkRequestBody::Bytes(Vec::new()),
        }
    }
}

pub(crate) fn encode_content_path(path: &str) -> String {
    path.split('/')
        .map(|segment| utf8_percent_encode(segment, PATH_SEGMENT_ENCODE_SET).to_string())
        .collect::<Vec<_>>()
        .join("/")
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::str::FromStr;

    use locks_core::ids::{GuardedResourceHash, LockServerPubky};
    use locks_core::lock_policy::{
        AccessPolicy, CONTENT_LOCK_V2_VERSION, GraphParticipantV2, GraphPolicyV2, GraphPredicateV2,
        GuardedResource, LockLogic, LockServerConfig, SecondaryGuardedResource,
    };
    use serde_json::json;

    use super::*;

    #[test]
    fn register_guarded_resource_request_builds_raw_put_without_creator() {
        let creator = LocksSession::new("frontend-session-secret").creator();

        let request = creator.register_guarded_resource(RegisterGuardedResourceRequest {
            path: "images/example file.txt".to_owned(),
            content_type: "text/plain".to_owned(),
            bytes: b"guarded bytes".to_vec(),
        });

        assert_eq!(request.method, "PUT");
        assert_eq!(
            request.path,
            "/creator/priv-resources/content/images/example%20file.txt"
        );
        assert_eq!(request.authorization, "Bearer frontend-session-secret");
        assert_eq!(request.content_type, "text/plain");
        assert_eq!(
            request.body,
            SdkRequestBody::Bytes(b"guarded bytes".to_vec())
        );
    }

    #[test]
    fn delete_guarded_resource_request_uses_path_and_delete_method() {
        let creator = LocksSession::new("frontend-session-secret").creator();

        let request = creator.delete_guarded_resource(DeleteGuardedResourceRequest {
            path: "images/example file.txt".to_owned(),
        });

        assert_eq!(request.method, "DELETE");
        assert_eq!(
            request.path,
            "/creator/priv-resources/content/images/example%20file.txt"
        );
        assert_eq!(request.authorization, "Bearer frontend-session-secret");
        assert_eq!(request.body, SdkRequestBody::Bytes(Vec::new()));
    }

    #[test]
    fn create_content_lock_request_serializes_without_creator_and_with_bearer() {
        let creator = LocksSession::new("frontend-session-secret").creator();
        let guarded_resource = guarded_resource();
        let secondary_resource = SecondaryGuardedResource {
            hash: guarded_resource.hash,
            content_type: guarded_resource.content_type.clone(),
            size: guarded_resource.size,
        };
        let mut secondary_resources = BTreeMap::new();
        secondary_resources.insert(
            "/priv/app.locks/content/attachments/example.txt".to_owned(),
            secondary_resource.clone(),
        );

        let request = creator.create_content_lock(CreateContentLockRequest {
            primary_resource: Some(guarded_resource.clone()),
            secondary_resources: secondary_resources.clone(),
            criteria: vec![],
            lock_logic: LockLogic::All { criteria: vec![] },
            access_policy: AccessPolicy {
                requested_credential_ttl_seconds: 900,
            },
            lock_server: LockServerConfig { override_: None },
        });

        assert_eq!(request.method, "POST");
        assert_eq!(request.path, "/creator/content-locks");
        assert_eq!(request.authorization, "Bearer frontend-session-secret");
        assert_eq!(request.content_type, "application/json");
        let SdkRequestBody::Json(body) = request.body else {
            panic!("create content lock should be JSON");
        };
        assert_eq!(
            body["primary_resource"],
            serde_json::to_value(guarded_resource).unwrap()
        );
        assert_eq!(
            body["secondary_resources"],
            serde_json::to_value(secondary_resources).unwrap()
        );
        assert_eq!(body["criteria"], json!([]));
        assert_eq!(body["lock_logic"], json!({ "type": "all", "criteria": [] }));
        assert_eq!(
            body["access_policy"],
            json!({ "requested_credential_ttl_seconds": 900 })
        );
        assert_eq!(body["lock_server"], json!({ "override": null }));
        assert!(body.get("creator").is_none());
    }

    #[test]
    fn create_content_lock_request_omits_empty_secondary_resources() {
        let creator = LocksSession::new("frontend-session-secret").creator();

        let request = creator.create_content_lock(CreateContentLockRequest {
            primary_resource: Some(guarded_resource()),
            secondary_resources: BTreeMap::new(),
            criteria: vec![],
            lock_logic: LockLogic::All { criteria: vec![] },
            access_policy: AccessPolicy {
                requested_credential_ttl_seconds: 900,
            },
            lock_server: LockServerConfig { override_: None },
        });

        let SdkRequestBody::Json(body) = request.body else {
            panic!("create content lock should be JSON");
        };
        assert!(body.get("primary_resource").is_some());
        assert!(body.get("secondary_resources").is_none());
        assert!(body.get("guarded_resource").is_none());
    }

    #[test]
    fn create_content_lock_accepts_explicit_v2_request_on_common_surface() {
        let creator = LocksSession::new("frontend-session-secret").creator();
        let request = creator.create_content_lock(CreateContentLockV2Request {
            version: CONTENT_LOCK_V2_VERSION,
            primary_resource: Some(guarded_resource()),
            secondary_resources: BTreeMap::new(),
            payment: None,
            graph: Some(GraphPolicyV2 {
                predicates: vec![GraphPredicateV2::Follows {
                    follower: GraphParticipantV2::Reader,
                    followed: GraphParticipantV2::Creator,
                }],
                operator: None,
            }),
            verification_window_seconds: 300,
            access_policy: AccessPolicy {
                requested_credential_ttl_seconds: 900,
            },
            lock_server: LockServerConfig { override_: None },
        });

        let SdkRequestBody::Json(body) = request.body else {
            panic!("create content lock should be JSON");
        };
        assert_eq!(body["version"], 2);
        assert!(body.get("criteria").is_none());
        assert!(body.get("graph").is_some());
    }

    #[test]
    fn set_lock_service_pointer_request_serializes_without_creator_and_with_bearer() {
        let creator = LocksSession::new("frontend-session-secret").creator();
        let lock_server = lock_server();

        let request = creator.set_lock_service_pointer(SetLockServicePointerRequest {
            default_lock_server: lock_server.clone(),
        });

        assert_eq!(request.method, "POST");
        assert_eq!(request.path, "/creator/lock-service-config");
        assert_eq!(request.authorization, "Bearer frontend-session-secret");
        assert_eq!(request.content_type, "application/json");
        assert_eq!(
            request.body,
            SdkRequestBody::Json(json!({ "default_lock_server": lock_server }))
        );
    }

    #[test]
    fn creator_authority_status_request_uses_authenticated_empty_get() {
        let creator = LocksSession::new("frontend-session-secret").creator();

        let request = creator.authority_status_request();

        assert_eq!(request.method, "GET");
        assert_eq!(request.path, "/creator/authority-status");
        assert_eq!(request.authorization, "Bearer frontend-session-secret");
        assert_eq!(request.content_type, "");
        assert_eq!(request.body, SdkRequestBody::Empty);
    }

    #[test]
    fn creator_authority_status_response_accepts_only_canonical_shapes() {
        let creator = "pubkytkrq8zmwb8a3m9k15csu3q17qmfgqnp9dskbrg9uq1rydpyxp7qy";
        let authorized = CreatorLocks::parse_authority_status_response(json!({
            "creator": creator,
            "authorized": true,
            "auth_kind": "grant",
            "granted_scopes": ["/pub/app.locks/:rw"],
            "session_expires_at": "2026-11-03T12:00:00Z"
        }))
        .unwrap();
        assert!(authorized.authorized);
        assert_eq!(authorized.auth_kind, Some(CreatorAuthorityAuthKind::Grant));

        let unavailable = CreatorLocks::parse_authority_status_response(json!({
            "creator": creator,
            "authorized": false,
            "auth_kind": null,
            "granted_scopes": [],
            "session_expires_at": null
        }))
        .unwrap();
        assert!(!unavailable.authorized);

        for invalid in [
            json!({"creator": creator, "authorized": false, "auth_kind": "grant", "granted_scopes": [], "session_expires_at": null}),
            json!({"creator": creator, "authorized": false, "auth_kind": null, "granted_scopes": ["/pub/app.locks/:rw"], "session_expires_at": null}),
            json!({"creator": creator, "authorized": true, "auth_kind": "future", "granted_scopes": [], "session_expires_at": null}),
            json!({"creator": creator, "authorized": false, "auth_kind": null, "granted_scopes": [], "session_expires_at": null, "secret": "rejected"}),
        ] {
            assert!(CreatorLocks::parse_authority_status_response(invalid).is_err());
        }
    }

    #[test]
    fn paykit_setup_status_request_uses_authenticated_get_without_creator_input() {
        let creator = LocksSession::new("frontend-session-secret").creator();

        let request = creator.paykit_setup_status_request(super::PaykitPaymentAsset::Usd);

        assert_eq!(request.method, "GET");
        assert_eq!(request.path, "/creator/paykit/setup-status?asset=USD");
        assert_eq!(request.authorization, "Bearer frontend-session-secret");
        assert_eq!(request.content_type, "");
        assert_eq!(request.body, SdkRequestBody::Empty);
    }

    #[test]
    fn paykit_setup_status_response_accepts_only_closed_statuses() {
        for (wire_status, expected) in [
            ("ready", PaykitSetupStatusKind::Ready),
            ("setup_required", PaykitSetupStatusKind::SetupRequired),
            ("unavailable", PaykitSetupStatusKind::Unavailable),
        ] {
            assert_eq!(
                CreatorLocks::parse_paykit_setup_status_response(json!({
                    "status": wire_status
                }))
                .unwrap(),
                PaykitSetupStatus { status: expected }
            );
        }

        for invalid in [
            json!({ "status": "future" }),
            json!({ "status": "ready", "extra": true }),
            json!({}),
            json!("ready"),
        ] {
            assert!(CreatorLocks::parse_paykit_setup_status_response(invalid).is_err());
        }
    }

    fn guarded_resource() -> GuardedResource {
        GuardedResource::new(
            "/priv/app.locks/content/example.txt",
            GuardedResourceHash::from_bytes([7; 32]),
            "text/plain",
            13,
        )
        .unwrap()
    }

    fn lock_server() -> LockServerPubky {
        LockServerPubky::from_str("pubky7ir1ttte48bcp4zjychjyscicrwi1j34mtt91ptsafdbjmr8g9eo")
            .unwrap()
    }
}
