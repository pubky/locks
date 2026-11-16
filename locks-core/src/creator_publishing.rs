use std::collections::BTreeMap;

use serde::{Deserialize, Deserializer, Serialize};

use crate::ids::LockServerPubky;
use crate::lock_policy::{
    AccessPolicy, ContentLockV2ValidationError, Criterion, GraphPolicyV2, GuardedResource,
    LockLogic, LockServerConfig, PaymentPolicyV2, SecondaryGuardedResource, validate_v2_shape,
};

/// Closed creator-publishing payload accepted by the common content-lock route.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum CreateContentLockPayload {
    /// Explicitly versioned v2 creator request.
    V2(CreateContentLockV2Request),
    /// Existing unversioned v1 creator request.
    V1(CreateContentLockRequest),
}

impl From<CreateContentLockRequest> for CreateContentLockPayload {
    fn from(value: CreateContentLockRequest) -> Self {
        Self::V1(value)
    }
}

impl From<CreateContentLockV2Request> for CreateContentLockPayload {
    fn from(value: CreateContentLockV2Request) -> Self {
        Self::V2(value)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CreateContentLockRequest {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub primary_resource: Option<GuardedResource>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub secondary_resources: BTreeMap<String, SecondaryGuardedResource>,
    pub criteria: Vec<Criterion>,
    pub lock_logic: LockLogic,
    pub access_policy: AccessPolicy,
    pub lock_server: LockServerConfig,
}

/// Creator-free v2 request. Creator identity comes only from frontend session auth.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct CreateContentLockV2Request {
    pub version: u16,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub primary_resource: Option<GuardedResource>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub secondary_resources: BTreeMap<String, SecondaryGuardedResource>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub payment: Option<PaymentPolicyV2>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub graph: Option<GraphPolicyV2>,
    pub verification_window_seconds: u64,
    pub access_policy: AccessPolicy,
    pub lock_server: LockServerConfig,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct CreateContentLockV2RequestWire {
    version: u16,
    #[serde(default)]
    primary_resource: Option<GuardedResource>,
    #[serde(default)]
    secondary_resources: BTreeMap<String, SecondaryGuardedResource>,
    #[serde(default)]
    payment: Option<PaymentPolicyV2>,
    #[serde(default)]
    graph: Option<GraphPolicyV2>,
    verification_window_seconds: u64,
    access_policy: AccessPolicy,
    lock_server: LockServerConfig,
}

impl From<CreateContentLockV2RequestWire> for CreateContentLockV2Request {
    fn from(value: CreateContentLockV2RequestWire) -> Self {
        Self {
            version: value.version,
            primary_resource: value.primary_resource,
            secondary_resources: value.secondary_resources,
            payment: value.payment,
            graph: value.graph,
            verification_window_seconds: value.verification_window_seconds,
            access_policy: value.access_policy,
            lock_server: value.lock_server,
        }
    }
}

impl CreateContentLockV2Request {
    /// Validates closed v2 fields that do not depend on authenticated creator identity.
    pub fn validate_shape(&self) -> Result<(), ContentLockV2ValidationError> {
        validate_v2_shape(
            self.version,
            &self.primary_resource,
            &self.secondary_resources,
            &self.payment,
            &self.graph,
            self.verification_window_seconds,
        )
    }
}

impl<'de> Deserialize<'de> for CreateContentLockV2Request {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let request = Self::from(CreateContentLockV2RequestWire::deserialize(deserializer)?);
        request.validate_shape().map_err(serde::de::Error::custom)?;
        Ok(request)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SetLockServicePointerRequest {
    pub default_lock_server: LockServerPubky,
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::str::FromStr;

    use serde_json::json;

    use super::*;
    use crate::ids::GuardedResourceHash;

    #[test]
    fn create_content_lock_request_serializes_creator_free_json_shape() {
        let primary_resource = guarded_resource(
            "/priv/app.locks/content/post.json",
            7,
            "application/json",
            5,
        );
        let secondary_resource = SecondaryGuardedResource {
            hash: guarded_hash(8),
            content_type: "image/png".to_owned(),
            size: 9,
        };
        let mut secondary_resources = BTreeMap::new();
        secondary_resources.insert(
            "/priv/app.locks/content/attachments/image.png".to_owned(),
            secondary_resource.clone(),
        );
        let request = CreateContentLockRequest {
            primary_resource: Some(primary_resource.clone()),
            secondary_resources: secondary_resources.clone(),
            criteria: vec![],
            lock_logic: LockLogic::All { criteria: vec![] },
            access_policy: AccessPolicy {
                requested_credential_ttl_seconds: 900,
            },
            lock_server: LockServerConfig { override_: None },
        };

        let serialized = serde_json::to_value(&request).unwrap();

        assert!(serialized.get("creator").is_none());
        assert_eq!(
            serialized,
            json!({
                "primary_resource": primary_resource,
                "secondary_resources": {
                    "/priv/app.locks/content/attachments/image.png": secondary_resource,
                },
                "criteria": [],
                "lock_logic": { "type": "all", "criteria": [] },
                "access_policy": { "requested_credential_ttl_seconds": 900 },
                "lock_server": { "override": null },
            })
        );
        serde_json::from_value::<CreateContentLockRequest>(serialized).unwrap();
    }

    #[test]
    fn create_content_lock_request_omits_empty_optional_resource_sets() {
        let request = CreateContentLockRequest {
            primary_resource: None,
            secondary_resources: BTreeMap::new(),
            criteria: vec![],
            lock_logic: LockLogic::Any { criteria: vec![] },
            access_policy: AccessPolicy {
                requested_credential_ttl_seconds: 900,
            },
            lock_server: LockServerConfig { override_: None },
        };

        let serialized = serde_json::to_value(&request).unwrap();

        assert!(serialized.get("creator").is_none());
        assert!(serialized.get("primary_resource").is_none());
        assert!(serialized.get("secondary_resources").is_none());
        serde_json::from_value::<CreateContentLockRequest>(serialized).unwrap();
    }

    #[test]
    fn create_content_lock_request_rejects_unknown_fields() {
        let mut value = json!({
            "criteria": [],
            "lock_logic": { "type": "all", "criteria": [] },
            "access_policy": { "requested_credential_ttl_seconds": 900 },
            "lock_server": {},
        });
        value["creator"] = json!("pubkytkrq8zmwb8a3m9k15csu3q17qmfgqnp9dskbrg9uq1rydpyxp7qy");

        assert!(serde_json::from_value::<CreateContentLockRequest>(value).is_err());
    }

    #[test]
    fn create_content_lock_payload_accepts_closed_v2_shape() {
        let value = json!({
            "version": 2,
            "primary_resource": guarded_resource(
                "/priv/app.locks/content/post.json",
                7,
                "application/json",
                5,
            ),
            "payment": {
                "recipient_pubky": "pubkytkrq8zmwb8a3m9k15csu3q17qmfgqnp9dskbrg9uq1rydpyxp7qy",
                "amount": "50000",
                "asset": "BTC",
                "minimum_confirmations": 1
            },
            "graph": {
                "predicates": [{
                    "type": "follows",
                    "follower": "reader",
                    "followed": "creator"
                }]
            },
            "verification_window_seconds": 300,
            "access_policy": { "requested_credential_ttl_seconds": 900 },
            "lock_server": { "override": null }
        });

        let request = serde_json::from_value::<CreateContentLockPayload>(value.clone()).unwrap();

        assert!(matches!(request, CreateContentLockPayload::V2(_)));
        assert_eq!(serde_json::to_value(request).unwrap(), value);
    }

    #[test]
    fn create_content_lock_payload_rejects_mixed_v1_v2_fields() {
        let value = json!({
            "version": 2,
            "primary_resource": guarded_resource(
                "/priv/app.locks/content/post.json",
                7,
                "application/json",
                5,
            ),
            "payment": null,
            "graph": {
                "predicates": [{
                    "type": "follows",
                    "follower": "reader",
                    "followed": "creator"
                }]
            },
            "verification_window_seconds": 300,
            "criteria": [],
            "lock_logic": { "type": "all", "criteria": [] },
            "access_policy": { "requested_credential_ttl_seconds": 900 },
            "lock_server": { "override": null }
        });

        assert!(serde_json::from_value::<CreateContentLockPayload>(value).is_err());
    }

    #[test]
    fn create_content_lock_v2_request_rejects_policy_outside_closed_v2_contract() {
        let valid = json!({
            "version": 2,
            "primary_resource": guarded_resource(
                "/priv/app.locks/content/post.json",
                7,
                "application/json",
                5,
            ),
            "payment": {
                "recipient_pubky": "pubkytkrq8zmwb8a3m9k15csu3q17qmfgqnp9dskbrg9uq1rydpyxp7qy",
                "amount": "50000",
                "asset": "BTC",
                "minimum_confirmations": 1
            },
            "verification_window_seconds": 300,
            "access_policy": { "requested_credential_ttl_seconds": 900 },
            "lock_server": { "override": null }
        });

        let mut valid_usdt = valid.clone();
        valid_usdt["payment"]["asset"] = json!("USDT");
        valid_usdt["payment"]["amount"] = json!("5000001");
        serde_json::from_value::<CreateContentLockV2Request>(valid_usdt).unwrap();

        for invalid in [
            {
                let mut value = valid.clone();
                value["payment"]["asset"] = json!("USD");
                value
            },
            {
                let mut value = valid.clone();
                value["payment"]["amount"] = json!("0");
                value
            },
            {
                let mut value = valid.clone();
                value["payment"]["minimum_confirmations"] = json!(7);
                value
            },
            {
                let mut value = valid.clone();
                value["verification_window_seconds"] = json!(59);
                value
            },
            {
                let mut value = valid.clone();
                value["version"] = json!(3);
                value
            },
            {
                let mut value = valid.clone();
                value["payment"]["unknown"] = json!(true);
                value
            },
            {
                let mut value = valid.clone();
                value.as_object_mut().unwrap().remove("payment");
                value["graph"] = json!({
                    "predicates": [{
                        "type": "follows",
                        "follower": "reader",
                        "followed": "reader"
                    }]
                });
                value
            },
            {
                let mut value = valid.clone();
                value.as_object_mut().unwrap().remove("payment");
                value["graph"] = json!({
                    "predicates": [{
                        "type": "follows",
                        "follower": "reader",
                        "followed": "creator",
                        "unknown": true
                    }]
                });
                value
            },
        ] {
            assert!(
                serde_json::from_value::<CreateContentLockV2Request>(invalid).is_err(),
                "invalid v2 creator policy must fail closed"
            );
        }
    }

    #[test]
    fn set_lock_service_pointer_request_serializes_creator_free_json_shape() {
        let request = SetLockServicePointerRequest {
            default_lock_server: LockServerPubky::from_str(
                "pubky7ir1ttte48bcp4zjychjyscicrwi1j34mtt91ptsafdbjmr8g9eo",
            )
            .unwrap(),
        };

        let serialized = serde_json::to_value(&request).unwrap();

        assert!(serialized.get("creator").is_none());
        assert_eq!(
            serialized["default_lock_server"],
            request.default_lock_server.to_string()
        );
        serde_json::from_value::<SetLockServicePointerRequest>(serialized).unwrap();
    }

    fn guarded_resource(
        path: &str,
        hash_seed: u8,
        content_type: &str,
        size: u64,
    ) -> GuardedResource {
        GuardedResource::new(
            path.to_owned(),
            guarded_hash(hash_seed),
            content_type.to_owned(),
            size,
        )
        .unwrap()
    }

    fn guarded_hash(seed: u8) -> GuardedResourceHash {
        GuardedResourceHash::from_bytes([seed; 32])
    }
}
