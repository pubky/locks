use std::str::FromStr;

use locks_core::ids::{BundleId, CreatorPubky, LockServerPubky, PubkyLockResource};
use locks_sdk::{
    AccessCredentialResponse, CreatorLockServicePointer, DeleteGuardedResourceRequest, LocksClient,
    LocksSession, ReadLockedResourceRequest, RegisterGuardedResourceRequest,
    VerificationTaskHandleRequest, VerificationTaskLifecycleResponse, VerificationTaskStatus,
    ViewerLocks, content_lock_resource_url, creator_lock_service_pointer_url,
    lock_server_for_content_lock,
};

#[test]
fn crate_root_exports_foundation_sdk_types() {
    let lock_server =
        LockServerPubky::from_str("pubky7ir1ttte48bcp4zjychjyscicrwi1j34mtt91ptsafdbjmr8g9eo")
            .unwrap();
    let client = LocksClient::for_server(lock_server);
    let session = client.restore_session("frontend-session-secret");
    let creator = session.creator();

    let request = creator.register_guarded_resource(RegisterGuardedResourceRequest {
        path: "example.txt".to_owned(),
        content_type: "text/plain".to_owned(),
        bytes: b"guarded bytes".to_vec(),
    });

    assert_eq!(request.path, "/creator/priv-resources/content/example.txt");

    let delete_request = creator.delete_guarded_resource(DeleteGuardedResourceRequest {
        path: "example.txt".to_owned(),
    });
    assert_eq!(delete_request.method, "DELETE");
    assert_eq!(
        delete_request.path,
        "/creator/priv-resources/content/example.txt"
    );

    let viewer = ViewerLocks::new();
    let viewer_request = viewer.issue_access_credential(VerificationTaskHandleRequest {
        creator: CreatorPubky::from_str(
            "pubkytkrq8zmwb8a3m9k15csu3q17qmfgqnp9dskbrg9uq1rydpyxp7qy",
        )
        .unwrap(),
        bundle_id: BundleId::from_str("000G40R40M30E209185GR38E1W").unwrap(),
    });
    assert_eq!(viewer_request.path, "/access-credentials");

    let read_request = viewer.read_locked_resource(ReadLockedResourceRequest {
        credential: "raw-access-credential".to_owned(),
        path: "example.txt".to_owned(),
    });
    assert_eq!(read_request.method, "GET");
    assert_eq!(read_request.path, "/priv-resources/content/example.txt");

    let lifecycle: VerificationTaskLifecycleResponse =
        ViewerLocks::parse_lifecycle_response(serde_json::json!({
            "creator": "pubkytkrq8zmwb8a3m9k15csu3q17qmfgqnp9dskbrg9uq1rydpyxp7qy",
            "bundle_id": "000G40R40M30E209185GR38E1W",
            "status": "pending",
            "submitted_at": "2026-06-01T12:00:00Z",
            "started_at": null,
            "completed_at": null,
            "failure_message": null
        }))
        .unwrap();
    assert_eq!(lifecycle.status, VerificationTaskStatus::Pending);

    let issued: AccessCredentialResponse =
        ViewerLocks::parse_access_credential_response(serde_json::json!({
            "credential": "raw-access-credential",
            "expires_at": "2026-06-01T12:15:00Z"
        }))
        .unwrap();
    assert_eq!(issued.credential, "raw-access-credential");

    let pointer = CreatorLockServicePointer::validate_value(serde_json::json!({
        "version": 1,
        "default_lock_server": "pubky7ir1ttte48bcp4zjychjyscicrwi1j34mtt91ptsafdbjmr8g9eo",
        "created_at": "2026-06-03T00:00:00Z"
    }))
    .unwrap();
    assert_eq!(
        LocksClient::for_creator_pointer(pointer)
            .lock_server()
            .to_string(),
        "pubky7ir1ttte48bcp4zjychjyscicrwi1j34mtt91ptsafdbjmr8g9eo"
    );

    let creator_pointer_url = creator_lock_service_pointer_url(
        &CreatorPubky::from_str("pubkytkrq8zmwb8a3m9k15csu3q17qmfgqnp9dskbrg9uq1rydpyxp7qy")
            .unwrap(),
    )
    .unwrap();
    assert_eq!(
        creator_pointer_url.as_str(),
        "https://_pubky.tkrq8zmwb8a3m9k15csu3q17qmfgqnp9dskbrg9uq1rydpyxp7qy/pub/locks.app/config.json"
    );

    let content_lock_resource = PubkyLockResource::from_str(
        "pubkytkrq8zmwb8a3m9k15csu3q17qmfgqnp9dskbrg9uq1rydpyxp7qy/pub/locks.app/000G40R40M30E209185GR38E1W8124GK2GAHC5RR34D1P70X3RFG.json",
    )
    .unwrap();
    assert_eq!(
        content_lock_resource_url(&content_lock_resource)
            .unwrap()
            .as_str(),
        "https://_pubky.tkrq8zmwb8a3m9k15csu3q17qmfgqnp9dskbrg9uq1rydpyxp7qy/pub/locks.app/000G40R40M30E209185GR38E1W8124GK2GAHC5RR34D1P70X3RFG.json"
    );
    let _selector: fn(
        &locks_core::lock_policy::ContentLock,
        Option<&CreatorLockServicePointer>,
    ) -> locks_sdk::Result<LockServerPubky> = lock_server_for_content_lock;
    let _paykit_data_probe = locks_sdk::has_paykit_data;

    assert_eq!(LocksSession::new("another").export_secret(), "another");
}
