pub use locks_core::creator_publishing::{CreateContentLockRequest, SetLockServicePointerRequest};
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

    pub fn create_content_lock_request(&self, request: CreateContentLockRequest) -> SdkRequest {
        self.post_json("/creator/content-locks", request)
    }

    pub fn create_content_lock(&self, request: CreateContentLockRequest) -> SdkRequest {
        self.create_content_lock_request(request)
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

    pub fn paykit_setup_status_request(&self) -> SdkRequest {
        SdkRequest {
            method: "GET",
            path: "/creator/paykit/setup-status".to_owned(),
            authorization: self.session.authorization_header_value(),
            content_type: String::new(),
            body: SdkRequestBody::Empty,
        }
    }

    pub fn paykit_setup_status(&self) -> SdkRequest {
        self.paykit_setup_status_request()
    }

    pub fn parse_paykit_setup_status_response(value: Value) -> crate::Result<PaykitSetupStatus> {
        serde_json::from_value(value).map_err(|_| {
            crate::LocksSdkError::InvalidResponse("invalid paykit setup status response".to_owned())
        })
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
        AccessPolicy, GuardedResource, LockLogic, LockServerConfig, SecondaryGuardedResource,
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
            "/priv/locks.app/content/attachments/example.txt".to_owned(),
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
    fn paykit_setup_status_request_uses_authenticated_get_without_creator_input() {
        let creator = LocksSession::new("frontend-session-secret").creator();

        let request = creator.paykit_setup_status_request();

        assert_eq!(request.method, "GET");
        assert_eq!(request.path, "/creator/paykit/setup-status");
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
            "/priv/locks.app/content/example.txt",
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
