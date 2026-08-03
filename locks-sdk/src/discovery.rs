use std::fmt::Display;

use locks_core::ids::{CreatorPubky, LockServerPubky, PubkyLockResource};
use locks_core::lock_policy::ContentLock;
use locks_core::lock_service_pointer::{LOCK_SERVICE_POINTER_VERSION, LockServicePointer};
use serde::Deserialize;

use crate::error::{LocksSdkError, Result};
use url::Url;

pub const LOCKS_SERVER_SERVICE: &str = "pubky-locks-server";
pub const SUPPORTED_API_VERSION: &str = "0.1";

pub fn creator_lock_service_pointer_url(creator: &CreatorPubky) -> Result<Url> {
    Url::parse(&format!(
        "https://_pubky.{}{}",
        raw_pubky_z32(creator),
        locks_core::lock_service_pointer::LOCK_SERVICE_POINTER_PATH
    ))
    .map_err(|_| LocksSdkError::InvalidTransportUrl)
}

pub fn content_lock_resource_url(resource: &PubkyLockResource) -> Result<Url> {
    Url::parse(&format!(
        "https://_pubky.{}{}",
        raw_pubky_z32(resource.creator()),
        resource.content_lock_path()
    ))
    .map_err(|_| LocksSdkError::InvalidTransportUrl)
}

fn raw_pubky_z32(pubky: &impl Display) -> String {
    let value = pubky.to_string();
    value.strip_prefix("pubky").unwrap_or(&value).to_owned()
}

pub fn validate_content_lock_value(
    value: serde_json::Value,
    expected_resource: &PubkyLockResource,
) -> Result<ContentLock> {
    let content_lock: ContentLock =
        serde_json::from_value(value).map_err(|_| LocksSdkError::InvalidDiscoveryResponse)?;
    if content_lock.creator != *expected_resource.creator() {
        return Err(LocksSdkError::ContentLockCreatorMismatch);
    }
    let actual_path = content_lock
        .content_lock_path()
        .map_err(|err| LocksSdkError::InvalidResponse(err.to_string()))?;
    if &actual_path != expected_resource.content_lock_path() {
        return Err(LocksSdkError::ContentLockPathMismatch);
    }
    Ok(content_lock)
}

pub fn lock_server_for_content_lock(
    content_lock: &ContentLock,
    creator_pointer: Option<&CreatorLockServicePointer>,
) -> Result<LockServerPubky> {
    if let Some(lock_server) = &content_lock.lock_server.override_ {
        return Ok(lock_server.clone());
    }
    creator_pointer
        .map(|pointer| pointer.default_lock_server().clone())
        .ok_or(LocksSdkError::MissingCreatorLockServicePointer)
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct WellKnownLocksServer {
    pub service: String,
    pub api_version: String,
    pub lock_server: LockServerPubky,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreatorLockServicePointer {
    inner: LockServicePointer,
}

impl CreatorLockServicePointer {
    pub fn validate_value(value: serde_json::Value) -> Result<Self> {
        let inner: LockServicePointer =
            serde_json::from_value(value).map_err(|_| LocksSdkError::InvalidDiscoveryResponse)?;
        if inner.version != LOCK_SERVICE_POINTER_VERSION {
            return Err(LocksSdkError::UnsupportedLockServicePointerVersion(
                inner.version,
            ));
        }
        Ok(Self { inner })
    }

    pub fn default_lock_server(&self) -> &LockServerPubky {
        &self.inner.default_lock_server
    }

    pub fn into_inner(self) -> LockServicePointer {
        self.inner
    }
}

impl WellKnownLocksServer {
    pub fn validate_value(
        value: serde_json::Value,
        expected_lock_server: &LockServerPubky,
    ) -> Result<Self> {
        let discovery: Self =
            serde_json::from_value(value).map_err(|_| LocksSdkError::InvalidDiscoveryResponse)?;
        discovery.validate(expected_lock_server)?;
        Ok(discovery)
    }

    pub fn validate(&self, expected_lock_server: &LockServerPubky) -> Result<()> {
        if self.service != LOCKS_SERVER_SERVICE {
            return Err(LocksSdkError::UnexpectedDiscoveryService(
                self.service.clone(),
            ));
        }
        if self.api_version != SUPPORTED_API_VERSION {
            return Err(LocksSdkError::UnsupportedApiVersion(
                self.api_version.clone(),
            ));
        }
        if &self.lock_server != expected_lock_server {
            return Err(LocksSdkError::LockServerMismatch);
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use serde_json::json;
    use time::macros::datetime;

    use locks_core::ids::GuardedResourceHash;
    use locks_core::lock_policy::{
        AccessPolicy, CONTENT_LOCK_VERSION, GuardedResource, LockLogic, LockServerConfig,
    };

    use super::*;

    #[test]
    fn discovery_validation_accepts_expected_service_version_and_lock_server() {
        let lock_server = lock_server();
        let body = json!({
            "service": "pubky-locks-server",
            "api_version": "0.1",
            "lock_server": lock_server,
        });

        let discovery = WellKnownLocksServer::validate_value(body, &lock_server).unwrap();

        assert_eq!(discovery.service, "pubky-locks-server");
        assert_eq!(discovery.api_version, "0.1");
        assert_eq!(discovery.lock_server, lock_server);
    }

    #[test]
    fn discovery_validation_rejects_wrong_service() {
        let lock_server = lock_server();
        let body = json!({
            "service": "other-service",
            "api_version": "0.1",
            "lock_server": lock_server,
        });

        assert!(WellKnownLocksServer::validate_value(body, &lock_server).is_err());
    }

    #[test]
    fn discovery_validation_rejects_wrong_api_version() {
        let lock_server = lock_server();
        let body = json!({
            "service": "pubky-locks-server",
            "api_version": "9.9",
            "lock_server": lock_server,
        });

        assert!(WellKnownLocksServer::validate_value(body, &lock_server).is_err());
    }

    #[test]
    fn discovery_validation_rejects_wrong_lock_server() {
        let expected = lock_server();
        let body = json!({
            "service": "pubky-locks-server",
            "api_version": "0.1",
            "lock_server": "pubkytkrq8zmwb8a3m9k15csu3q17qmfgqnp9dskbrg9uq1rydpyxp7qy",
        });

        assert!(WellKnownLocksServer::validate_value(body, &expected).is_err());
    }

    #[test]
    fn creator_lock_service_pointer_accepts_v1_default_lock_server() {
        let body = json!({
            "version": 1,
            "default_lock_server": lock_server(),
            "created_at": "2026-06-03T00:00:00Z"
        });

        let pointer = CreatorLockServicePointer::validate_value(body).unwrap();

        assert_eq!(pointer.default_lock_server(), &lock_server());
    }

    #[test]
    fn creator_lock_service_pointer_rejects_unsupported_version() {
        let body = json!({
            "version": 2,
            "default_lock_server": lock_server(),
            "created_at": "2026-06-03T00:00:00Z"
        });

        assert!(CreatorLockServicePointer::validate_value(body).is_err());
    }

    #[test]
    fn creator_lock_service_pointer_rejects_unknown_fields_and_url_lock_server() {
        let unknown_field = json!({
            "version": 1,
            "default_lock_server": lock_server(),
            "created_at": "2026-06-03T00:00:00Z",
            "base_url": "https://locks.example"
        });
        assert!(CreatorLockServicePointer::validate_value(unknown_field).is_err());

        let url_lock_server = json!({
            "version": 1,
            "default_lock_server": "https://locks.example",
            "created_at": "2026-06-03T00:00:00Z"
        });
        assert!(CreatorLockServicePointer::validate_value(url_lock_server).is_err());
    }

    #[test]
    fn creator_lock_service_pointer_url_uses_canonical_public_pointer_path() {
        let creator =
            CreatorPubky::from_str("pubkytkrq8zmwb8a3m9k15csu3q17qmfgqnp9dskbrg9uq1rydpyxp7qy")
                .unwrap();

        let url = creator_lock_service_pointer_url(&creator).unwrap();

        assert_eq!(url.scheme(), "https");
        assert_eq!(
            url.host_str(),
            Some("_pubky.tkrq8zmwb8a3m9k15csu3q17qmfgqnp9dskbrg9uq1rydpyxp7qy")
        );
        assert_eq!(url.path(), "/pub/locks.app/config.json");
        assert_eq!(url.query(), None);
    }

    #[test]
    fn content_lock_resource_url_uses_canonical_public_lock_path() {
        let resource = PubkyLockResource::from_str(
            "pubkytkrq8zmwb8a3m9k15csu3q17qmfgqnp9dskbrg9uq1rydpyxp7qy/pub/locks.app/000G40R40M30E209185GR38E1W8124GK2GAHC5RR34D1P70X3RFG.json",
        )
        .unwrap();

        let url = content_lock_resource_url(&resource).unwrap();

        assert_eq!(url.scheme(), "https");
        assert_eq!(
            url.host_str(),
            Some("_pubky.tkrq8zmwb8a3m9k15csu3q17qmfgqnp9dskbrg9uq1rydpyxp7qy")
        );
        assert_eq!(
            url.path(),
            "/pub/locks.app/000G40R40M30E209185GR38E1W8124GK2GAHC5RR34D1P70X3RFG.json"
        );
        assert_eq!(url.query(), None);
    }

    #[test]
    fn content_lock_response_validation_rejects_creator_mismatch() {
        let expected = PubkyLockResource::from_str(
            "pubkytkrq8zmwb8a3m9k15csu3q17qmfgqnp9dskbrg9uq1rydpyxp7qy/pub/locks.app/000G40R40M30E209185GR38E1W8124GK2GAHC5RR34D1P70X3RFG.json",
        )
        .unwrap();
        let mismatched = json!({
            "version": 1,
            "creator": "pubky7ir1ttte48bcp4zjychjyscicrwi1j34mtt91ptsafdbjmr8g9eo",
            "primary_resource": {
                "path": "/priv/locks.app/content/demo.txt",
                "hash": "0W3GE1R70W3GE1R70W3GE1R70W3GE1R70W3G",
                "content_type": "text/plain",
                "size": 12
            },
            "secondary_resources": {},
            "criteria": [],
            "lock_logic": { "type": "all", "criteria": [] },
            "access_policy": { "requested_credential_ttl_seconds": 3600 },
            "lock_server": { "override": null },
            "created_at": "2026-06-03T00:00:00Z"
        });

        assert!(validate_content_lock_value(mismatched, &expected).is_err());
    }

    #[test]
    fn lock_server_for_content_lock_prefers_per_lock_override() {
        let override_lock_server = lock_server();
        let pointer_lock_server =
            LockServerPubky::from_str("pubkytkrq8zmwb8a3m9k15csu3q17qmfgqnp9dskbrg9uq1rydpyxp7qy")
                .unwrap();
        let content_lock = content_lock_json(Some(override_lock_server.clone()));
        let pointer = CreatorLockServicePointer::validate_value(json!({
            "version": 1,
            "default_lock_server": pointer_lock_server,
            "created_at": "2026-06-03T00:00:00Z"
        }))
        .unwrap();

        let selected = lock_server_for_content_lock(&content_lock, Some(&pointer)).unwrap();

        assert_eq!(selected, override_lock_server);
    }

    #[test]
    fn lock_server_for_content_lock_falls_back_to_creator_pointer() {
        let content_lock = content_lock_json(None);
        let pointer = CreatorLockServicePointer::validate_value(json!({
            "version": 1,
            "default_lock_server": lock_server(),
            "created_at": "2026-06-03T00:00:00Z"
        }))
        .unwrap();

        let selected = lock_server_for_content_lock(&content_lock, Some(&pointer)).unwrap();

        assert_eq!(selected, lock_server());
        assert!(lock_server_for_content_lock(&content_lock, None).is_err());
    }

    fn content_lock_json(override_lock_server: Option<LockServerPubky>) -> ContentLock {
        ContentLock {
            version: CONTENT_LOCK_VERSION,
            creator: CreatorPubky::from_str(
                "pubkytkrq8zmwb8a3m9k15csu3q17qmfgqnp9dskbrg9uq1rydpyxp7qy",
            )
            .unwrap(),
            primary_resource: Some(GuardedResource {
                path: "/priv/locks.app/content/demo.txt".to_owned(),
                hash: GuardedResourceHash::from_bytes([7; 32]),
                content_type: "text/plain".to_owned(),
                size: 12,
            }),
            secondary_resources: Default::default(),
            criteria: vec![],
            lock_logic: LockLogic::All { criteria: vec![] },
            access_policy: AccessPolicy {
                requested_credential_ttl_seconds: 3600,
            },
            lock_server: LockServerConfig {
                override_: override_lock_server,
            },
            created_at: datetime!(2026-06-03 00:00:00 UTC),
        }
    }

    fn lock_server() -> LockServerPubky {
        LockServerPubky::from_str("pubky7ir1ttte48bcp4zjychjyscicrwi1j34mtt91ptsafdbjmr8g9eo")
            .unwrap()
    }
}
