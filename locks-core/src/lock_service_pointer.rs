use serde::{Deserialize, Serialize};
use time::OffsetDateTime;

use crate::ids::LockServerPubky;

pub const LOCK_SERVICE_POINTER_VERSION: u16 = 1;
pub const LOCK_SERVICE_POINTER_PATH: &str = "/pub/locks.app/config.json";

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub struct LockServicePointer {
    pub version: u16,
    pub default_lock_server: LockServerPubky,
    #[serde(with = "time::serde::rfc3339")]
    pub created_at: OffsetDateTime,
}

pub fn lock_service_pointer_path() -> &'static str {
    LOCK_SERVICE_POINTER_PATH
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use serde_json::json;
    use time::macros::datetime;

    use crate::ids::LockServerPubky;
    use crate::lock_service_pointer::{
        LOCK_SERVICE_POINTER_PATH, LOCK_SERVICE_POINTER_VERSION, LockServicePointer,
        lock_service_pointer_path,
    };

    fn test_pubky_identity() -> String {
        pubky::Keypair::random().public_key().to_string()
    }

    fn pointer_fixture() -> LockServicePointer {
        LockServicePointer {
            version: LOCK_SERVICE_POINTER_VERSION,
            default_lock_server: LockServerPubky::from_str(&test_pubky_identity()).unwrap(),
            created_at: datetime!(2026-06-03 00:00:00 UTC),
        }
    }

    #[test]
    fn lock_service_pointer_serializes_exact_json_shape() {
        let pointer = pointer_fixture();

        let serialized = serde_json::to_value(&pointer).unwrap();
        let expected_lock_server = pointer.default_lock_server.to_string();

        assert_eq!(
            serialized,
            json!({
                "version": 1,
                "default_lock_server": expected_lock_server,
                "created_at": "2026-06-03T00:00:00Z"
            })
        );
    }

    #[test]
    fn lock_service_pointer_requires_version() {
        let mut value = serde_json::to_value(pointer_fixture()).unwrap();
        value.as_object_mut().unwrap().remove("version");

        let result = serde_json::from_value::<LockServicePointer>(value);

        assert!(result.is_err());
    }

    #[test]
    fn lock_service_pointer_requires_default_lock_server() {
        let mut value = serde_json::to_value(pointer_fixture()).unwrap();
        value.as_object_mut().unwrap().remove("default_lock_server");

        let result = serde_json::from_value::<LockServicePointer>(value);

        assert!(result.is_err());
    }

    #[test]
    fn lock_service_pointer_requires_created_at() {
        let mut value = serde_json::to_value(pointer_fixture()).unwrap();
        value.as_object_mut().unwrap().remove("created_at");

        let result = serde_json::from_value::<LockServicePointer>(value);

        assert!(result.is_err());
    }

    #[test]
    fn lock_service_pointer_rejects_unknown_top_level_fields() {
        let mut value = serde_json::to_value(pointer_fixture()).unwrap();
        value["extra"] = json!("nope");

        let result = serde_json::from_value::<LockServicePointer>(value);

        assert!(result.is_err());
    }

    #[test]
    fn lock_service_pointer_path_is_canonical_pubky_path() {
        assert_eq!(LOCK_SERVICE_POINTER_PATH, "/pub/locks.app/config.json");
        assert_eq!(lock_service_pointer_path(), "/pub/locks.app/config.json");
    }

    #[test]
    fn lock_service_pointer_rejects_invalid_default_lock_server() {
        let mut value = serde_json::to_value(pointer_fixture()).unwrap();
        value["default_lock_server"] = json!("http://example.test/server");

        let result = serde_json::from_value::<LockServicePointer>(value);

        assert!(result.is_err());
    }
}
