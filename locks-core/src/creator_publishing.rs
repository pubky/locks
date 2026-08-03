use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::ids::LockServerPubky;
use crate::lock_policy::{
    AccessPolicy, Criterion, GuardedResource, LockLogic, LockServerConfig, SecondaryGuardedResource,
};

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
            "/priv/locks.app/content/post.json",
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
            "/priv/locks.app/content/attachments/image.png".to_owned(),
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
                    "/priv/locks.app/content/attachments/image.png": secondary_resource,
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
