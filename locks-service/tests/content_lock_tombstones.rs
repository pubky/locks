use std::str::FromStr;
use std::sync::Mutex;

use async_trait::async_trait;
use locks_core::content_lock_deletion::ContentLockDeletionTombstone;
use locks_core::ids::{ContentLockPath, CreatorPubky, LockId};
use locks_service::application::errors::ApplicationError;
use locks_service::application::ports::{
    ContentLockRepository, ContentLockTombstoneRepository, TombstoneReadback,
};
use locks_service::infrastructure::memory::{
    content_lock_tombstones::InMemoryContentLockTombstoneRepository,
    content_locks::InMemoryContentLockRepository,
    public_content_locks::InMemoryPublicContentLockStore,
};
use locks_service::infrastructure::pubky::{
    PubkyBytesResource, PubkyContentLockTombstoneRepository, PubkyHomeserverStorageClient,
};
use time::macros::datetime;

const LOCK_ID: &str = "000G40R40M30E209185GR38E1W8124GK2GAHC5RR34D1P70X3RFG";

#[tokio::test]
async fn memory_content_lock_and_tombstone_adapters_share_one_canonical_public_path() {
    let store = InMemoryPublicContentLockStore::new();
    let content_locks = InMemoryContentLockRepository::with_public_store(store.clone());
    let tombstones = InMemoryContentLockTombstoneRepository::with_public_store(store);
    let creator = creator();
    let path = path();
    let original: locks_core::lock_policy::ContentLock =
        serde_json::from_value(serde_json::json!({
            "version": 1,
            "creator": creator,
            "primary_resource": null,
            "secondary_resources": {},
            "criteria": [],
            "lock_logic": { "type": "all", "criteria": [] },
            "access_policy": { "requested_credential_ttl_seconds": 900 },
            "lock_server": { "override": null },
            "created_at": "2026-08-12T04:00:00Z"
        }))
        .unwrap();
    content_locks
        .upsert_content_lock(creator.clone(), path.clone(), original.clone())
        .await
        .unwrap();

    let tombstone = tombstone_at_five();
    assert_eq!(
        tombstones
            .withdraw_content_lock(creator.clone(), path.clone(), &original, &tombstone)
            .await
            .unwrap(),
        TombstoneReadback::Exact
    );
    assert!(
        content_locks
            .get_content_lock(&creator, &path)
            .await
            .is_err()
    );

    assert_eq!(
        tombstones
            .read_tombstone(&creator, &path, &tombstone)
            .await
            .unwrap(),
        TombstoneReadback::Exact
    );
    assert!(
        content_locks
            .get_content_lock(&creator, &path)
            .await
            .is_err()
    );
}

#[tokio::test]
async fn memory_withdrawal_is_exact_and_replacement_is_classified_without_parsing() {
    let store = InMemoryPublicContentLockStore::new();
    let content_locks = InMemoryContentLockRepository::with_public_store(store.clone());
    let repository = InMemoryContentLockTombstoneRepository::with_public_store(store);
    let creator = creator();
    let path = path();
    let original = original_content_lock();
    let expected = tombstone_at_five();

    assert_eq!(
        repository
            .read_tombstone(&creator, &path, &expected)
            .await
            .unwrap(),
        TombstoneReadback::Missing
    );
    assert_eq!(
        repository
            .withdraw_content_lock(creator.clone(), path.clone(), &original, &expected)
            .await
            .unwrap(),
        TombstoneReadback::Missing
    );

    content_locks
        .upsert_content_lock(creator.clone(), path.clone(), original.clone())
        .await
        .unwrap();
    assert_eq!(
        repository
            .withdraw_content_lock(creator.clone(), path.clone(), &original, &expected)
            .await
            .unwrap(),
        TombstoneReadback::Exact
    );

    let mut replacement = original.clone();
    replacement.created_at = datetime!(2026-08-12 05:00:01 UTC);
    content_locks
        .upsert_content_lock(creator.clone(), path.clone(), replacement.clone())
        .await
        .unwrap();

    assert_eq!(
        repository
            .read_tombstone(&creator, &path, &expected)
            .await
            .unwrap(),
        TombstoneReadback::Replaced
    );
    assert_eq!(
        content_locks
            .get_content_lock(&creator, &path)
            .await
            .unwrap(),
        Some(replacement)
    );
}

#[tokio::test]
async fn memory_force_delete_removes_tombstone_and_missing_retry_succeeds() {
    let store = InMemoryPublicContentLockStore::new();
    let content_locks = InMemoryContentLockRepository::with_public_store(store.clone());
    let repository = InMemoryContentLockTombstoneRepository::with_public_store(store);
    let creator = creator();
    let path = path();
    let original = original_content_lock();
    let expected = tombstone_at_five();
    content_locks
        .upsert_content_lock(creator.clone(), path.clone(), original.clone())
        .await
        .unwrap();
    repository
        .withdraw_content_lock(creator.clone(), path.clone(), &original, &expected)
        .await
        .unwrap();

    repository
        .force_delete_content_lock_and_verify_absent(&creator, &path)
        .await
        .unwrap();
    repository
        .force_delete_content_lock_and_verify_absent(&creator, &path)
        .await
        .unwrap();
    assert_eq!(
        repository
            .read_tombstone(&creator, &path, &expected)
            .await
            .unwrap(),
        TombstoneReadback::Missing
    );
}

#[tokio::test]
async fn pubky_withdrawal_writes_canonical_bytes_to_exact_path_and_reads_them_back() {
    let repository = PubkyContentLockTombstoneRepository::new(FakePubkyStorage::default());
    let creator = creator();
    let path = path();
    let original = original_content_lock();
    let expected = tombstone_at_five();
    repository
        .client()
        .replace_bytes(original.canonical_json_bytes().unwrap());

    assert_eq!(
        repository
            .withdraw_content_lock(creator.clone(), path.clone(), &original, &expected)
            .await
            .unwrap(),
        TombstoneReadback::Exact
    );
    assert_eq!(
        repository.client().written_bytes(),
        serde_json::to_vec(&expected).unwrap()
    );
    assert_eq!(
        repository.client().operations(),
        vec![
            format!("get_bytes {creator} {path}"),
            format!("put_bytes {creator} {path} application/json"),
            format!("get_bytes {creator} {path}"),
        ]
    );

    assert_eq!(
        repository
            .withdraw_content_lock(creator.clone(), path.clone(), &original, &expected)
            .await
            .unwrap(),
        TombstoneReadback::Exact
    );
    assert_eq!(
        repository
            .client()
            .operations()
            .iter()
            .filter(|operation| operation.starts_with("put_bytes "))
            .count(),
        1
    );
}

#[tokio::test]
async fn pubky_readback_classifies_missing_and_non_tombstone_replacement_as_raw_bytes() {
    let repository = PubkyContentLockTombstoneRepository::new(FakePubkyStorage::default());
    let creator = creator();
    let path = path();
    let expected = tombstone_at_five();

    assert_eq!(
        repository
            .read_tombstone(&creator, &path, &expected)
            .await
            .unwrap(),
        TombstoneReadback::Missing
    );

    repository
        .client()
        .replace_bytes(br#"{"version":1,"creator":"not-a-tombstone"}"#.to_vec());
    assert_eq!(
        repository
            .read_tombstone(&creator, &path, &expected)
            .await
            .unwrap(),
        TombstoneReadback::Replaced
    );

    assert_eq!(
        repository
            .withdraw_content_lock(
                creator.clone(),
                path.clone(),
                &original_content_lock(),
                &expected,
            )
            .await
            .unwrap(),
        TombstoneReadback::Replaced
    );
    assert!(
        repository
            .client()
            .operations()
            .iter()
            .all(|operation| !operation.starts_with("put_bytes "))
    );
}

#[tokio::test]
async fn pubky_force_delete_removes_original_bytes_tombstone_and_replacement_with_missing_retries()
{
    let repository = PubkyContentLockTombstoneRepository::new(FakePubkyStorage::default());
    let creator = creator();
    let path = path();

    for bytes in [
        br#"{"version":1,"creator":"original-content-lock"}"#.to_vec(),
        serde_json::to_vec(&tombstone_at_five()).unwrap(),
        br#"replacement bytes that are not json"#.to_vec(),
    ] {
        repository.client().replace_bytes(bytes);
        repository
            .force_delete_content_lock_and_verify_absent(&creator, &path)
            .await
            .unwrap();
    }

    repository
        .force_delete_content_lock_and_verify_absent(&creator, &path)
        .await
        .unwrap();
    let expected_last = format!("get_bytes {creator} {path}");
    assert_eq!(
        repository.client().operations().last(),
        Some(&expected_last)
    );
}

#[derive(Debug, Default)]
struct FakePubkyStorage {
    bytes: Mutex<Option<Vec<u8>>>,
    written_bytes: Mutex<Option<Vec<u8>>>,
    operations: Mutex<Vec<String>>,
}

impl FakePubkyStorage {
    fn replace_bytes(&self, bytes: Vec<u8>) {
        *self.bytes.lock().unwrap() = Some(bytes);
    }

    fn written_bytes(&self) -> Vec<u8> {
        self.written_bytes.lock().unwrap().clone().unwrap()
    }

    fn operations(&self) -> Vec<String> {
        self.operations.lock().unwrap().clone()
    }
}

#[async_trait]
impl PubkyHomeserverStorageClient for FakePubkyStorage {
    async fn put_json_value_as_creator(
        &self,
        _creator: &CreatorPubky,
        _path: &str,
        _body: serde_json::Value,
    ) -> Result<(), ApplicationError> {
        unreachable!("tombstones must use raw canonical bytes")
    }

    async fn get_json_value_as_creator(
        &self,
        _creator: &CreatorPubky,
        _path: &str,
    ) -> Result<Option<serde_json::Value>, ApplicationError> {
        unreachable!("tombstones must not be parsed as JSON or ContentLock")
    }

    async fn put_bytes_as_creator(
        &self,
        creator: &CreatorPubky,
        path: &str,
        bytes: Vec<u8>,
        content_type: &str,
    ) -> Result<(), ApplicationError> {
        self.operations
            .lock()
            .unwrap()
            .push(format!("put_bytes {creator} {path} {content_type}"));
        *self.written_bytes.lock().unwrap() = Some(bytes.clone());
        *self.bytes.lock().unwrap() = Some(bytes);
        Ok(())
    }

    async fn get_bytes_as_creator(
        &self,
        creator: &CreatorPubky,
        path: &str,
    ) -> Result<Option<PubkyBytesResource>, ApplicationError> {
        self.operations
            .lock()
            .unwrap()
            .push(format!("get_bytes {creator} {path}"));
        Ok(self
            .bytes
            .lock()
            .unwrap()
            .clone()
            .map(|bytes| PubkyBytesResource {
                bytes,
                content_type: Some("application/json".to_owned()),
            }))
    }

    async fn delete_as_creator(
        &self,
        creator: &CreatorPubky,
        path: &str,
    ) -> Result<(), ApplicationError> {
        self.operations
            .lock()
            .unwrap()
            .push(format!("delete {creator} {path}"));
        *self.bytes.lock().unwrap() = None;
        Ok(())
    }
}

fn creator() -> CreatorPubky {
    CreatorPubky::from_str("pubkytkrq8zmwb8a3m9k15csu3q17qmfgqnp9dskbrg9uq1rydpyxp7qy").unwrap()
}

fn path() -> ContentLockPath {
    ContentLockPath::from_lock_id(LockId::from_str(LOCK_ID).unwrap())
}

fn tombstone_at_five() -> ContentLockDeletionTombstone {
    ContentLockDeletionTombstone::new(
        LockId::from_str(LOCK_ID).unwrap(),
        datetime!(2026-08-12 05:00:00 UTC),
    )
}

fn original_content_lock() -> locks_core::lock_policy::ContentLock {
    serde_json::from_value(serde_json::json!({
        "version": 1,
        "creator": creator(),
        "primary_resource": null,
        "secondary_resources": {},
        "criteria": [],
        "lock_logic": { "type": "all", "criteria": [] },
        "access_policy": { "requested_credential_ttl_seconds": 900 },
        "lock_server": { "override": null },
        "created_at": "2026-08-12T04:00:00Z"
    }))
    .unwrap()
}
