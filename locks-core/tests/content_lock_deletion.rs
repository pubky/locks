use std::str::FromStr;

use locks_core::content_lock_deletion::{
    CONTENT_LOCK_DELETION_TOMBSTONE_VERSION, ContentLockDeletionTombstone,
};
use locks_core::ids::LockId;
use serde_json::json;
use time::macros::datetime;

const LOCK_ID: &str = "000G40R40M30E209185GR38E1W8124GK2GAHC5RR34D1P70X3RFG";

#[test]
fn tombstone_serializes_the_exact_closed_protocol_shape() {
    let tombstone = ContentLockDeletionTombstone::new(
        LockId::from_str(LOCK_ID).unwrap(),
        datetime!(2026-08-12 05:00:00 UTC),
    );

    assert_eq!(
        serde_json::to_value(&tombstone).unwrap(),
        json!({
            "version": CONTENT_LOCK_DELETION_TOMBSTONE_VERSION,
            "type": "content_lock_deletion",
            "lock_id": LOCK_ID,
            "deletion_started_at": "2026-08-12T05:00:00Z",
        })
    );
}

#[test]
fn tombstone_rejects_unknown_version_type_fields_and_non_utc_time() {
    let valid = json!({
        "version": 1,
        "type": "content_lock_deletion",
        "lock_id": LOCK_ID,
        "deletion_started_at": "2026-08-12T05:00:00Z",
    });
    assert!(serde_json::from_value::<ContentLockDeletionTombstone>(valid.clone()).is_ok());

    for invalid in [
        {
            let mut value = valid.clone();
            value["version"] = json!(2);
            value
        },
        {
            let mut value = valid.clone();
            value["type"] = json!("content_lock");
            value
        },
        {
            let mut value = valid.clone();
            value["extra"] = json!(true);
            value
        },
        {
            let mut value = valid;
            value["deletion_started_at"] = json!("2026-08-12T06:00:00+01:00");
            value
        },
    ] {
        assert!(serde_json::from_value::<ContentLockDeletionTombstone>(invalid).is_err());
    }
}

#[test]
fn tombstone_constructor_normalizes_offsets_to_utc() {
    let tombstone = ContentLockDeletionTombstone::new(
        LockId::from_str(LOCK_ID).unwrap(),
        datetime!(2026-08-12 06:00:00 +01:00),
    );

    assert_eq!(
        serde_json::to_value(tombstone).unwrap()["deletion_started_at"],
        "2026-08-12T05:00:00Z"
    );
}
