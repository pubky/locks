use std::str::FromStr;
use std::sync::Arc;

use locks_core::ids::{BundleId, CreatorPubky, LockServerPubky, PubkyLockResource};
use locks_sdk::{
    AccessCredentialResponse, CreatorLockServicePointer, DeleteContentLockMode,
    DeleteContentLockRequest, DeleteGuardedResourceRequest, LocksClient, LocksSdkError,
    LocksSession, ReadLockedResourceRequest, RegisterGuardedResourceRequest, SdkRequestBody,
    VerificationTaskHandleRequest, VerificationTaskLifecycleResponse, VerificationTaskStatus,
    ViewerLocks, content_lock_resource_url, creator_lock_service_pointer_url,
    lock_server_for_content_lock,
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

#[tokio::test]
async fn readiness_resolves_the_lock_server_packet_at_the_bare_key_apex() {
    let keypair = pubky_common::crypto::Keypair::from_secret(&[9_u8; 32]);
    let (readiness_port, readiness_task) = spawn_readiness_server(&keypair).await;
    let packet = locks_server::pkdns::create_signed_packet(
        &locks_server::config::PkdnsConfig {
            public_ip: std::net::Ipv4Addr::LOCALHOST.into(),
            public_pubky_tls_port: Some(readiness_port),
            public_icann_http_port: None,
            icann_domain: None,
            key_republisher_interval_seconds: 3600,
        },
        &keypair,
    )
    .unwrap();
    let relay_url = spawn_packet_relay(packet).await;
    let lock_server =
        LockServerPubky::from_str(&format!("pubky{}", keypair.public_key().to_z32())).unwrap();
    let client = LocksClient::for_server(lock_server);
    let mut builder = pubky::PubkyHttpClient::builder();
    builder.request_timeout(std::time::Duration::from_secs(2));
    builder.pkarr(|pkarr| {
        pkarr
            .no_default_network()
            .relays(&[relay_url.as_str()])
            .expect("local relay URL")
    });
    let http = builder.build().unwrap();

    assert!(client.is_ready(&http).await.unwrap());
    readiness_task.await.unwrap();
}

async fn spawn_packet_relay(packet: pkarr::SignedPacket) -> String {
    let listener = tokio::net::TcpListener::bind((std::net::Ipv4Addr::LOCALHOST, 0))
        .await
        .unwrap();
    let address = listener.local_addr().unwrap();
    tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        let mut request = [0_u8; 2048];
        let read = stream.read(&mut request).await.unwrap();
        assert!(request[..read].starts_with(b"GET /"));
        let payload = packet.to_relay_payload();
        let response = format!(
            "HTTP/1.1 200 OK\r\ncontent-type: application/octet-stream\r\ncontent-length: {}\r\nconnection: close\r\n\r\n",
            payload.len()
        );
        stream.write_all(response.as_bytes()).await.unwrap();
        stream.write_all(&payload).await.unwrap();
    });
    format!("http://{address}")
}

async fn spawn_readiness_server(
    keypair: &pubky_common::crypto::Keypair,
) -> (u16, tokio::task::JoinHandle<()>) {
    let listener = tokio::net::TcpListener::bind((std::net::Ipv4Addr::LOCALHOST, 0))
        .await
        .unwrap();
    let port = listener.local_addr().unwrap().port();
    let mut tls_config = keypair.to_rpk_rustls_server_config();
    tls_config.alpn_protocols = vec![b"http/1.1".to_vec()];
    let acceptor = tokio_rustls::TlsAcceptor::from(Arc::new(tls_config));
    let task = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let mut stream = acceptor.accept(stream).await.unwrap();
        let mut request = [0_u8; 2048];
        let read = stream.read(&mut request).await.unwrap();
        assert!(request[..read].starts_with(b"GET /readyz HTTP/1.1\r\n"));
        stream
            .write_all(b"HTTP/1.1 200 OK\r\ncontent-length: 0\r\nconnection: close\r\n\r\n")
            .await
            .unwrap();
    });
    (port, task)
}

#[tokio::test]
async fn readiness_missing_pkarr_record_uses_public_request_failure_contract() {
    let lock_server =
        LockServerPubky::from_str("pubky7ir1ttte48bcp4zjychjyscicrwi1j34mtt91ptsafdbjmr8g9eo")
            .unwrap();
    let client = LocksClient::for_server(lock_server);
    let mut builder = pubky::PubkyHttpClient::builder();
    builder.request_timeout(std::time::Duration::from_millis(250));
    builder.pkarr(|pkarr| {
        pkarr
            .no_default_network()
            .relays(&["http://127.0.0.1:9"])
            .expect("local relay URL")
    });
    let http = builder.build().unwrap();

    let error = client.is_ready(&http).await.unwrap_err();

    assert!(matches!(
        error,
        LocksSdkError::LockServerReadinessRequestFailed(_)
    ));
}

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
            "failure_message": null,
            "status_message": "Reader wallet setup needed"
        }))
        .unwrap();
    assert_eq!(lifecycle.status, VerificationTaskStatus::Pending);
    assert_eq!(
        lifecycle.status_message.as_deref(),
        Some("Reader wallet setup needed")
    );

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
        "https://_pubky.tkrq8zmwb8a3m9k15csu3q17qmfgqnp9dskbrg9uq1rydpyxp7qy/pub/app.locks/config.json"
    );

    let content_lock_resource = PubkyLockResource::from_str(
        "pubkytkrq8zmwb8a3m9k15csu3q17qmfgqnp9dskbrg9uq1rydpyxp7qy/pub/app.locks/000G40R40M30E209185GR38E1W8124GK2GAHC5RR34D1P70X3RFG.json",
    )
    .unwrap();
    assert_eq!(
        content_lock_resource_url(&content_lock_resource)
            .unwrap()
            .as_str(),
        "https://_pubky.tkrq8zmwb8a3m9k15csu3q17qmfgqnp9dskbrg9uq1rydpyxp7qy/pub/app.locks/000G40R40M30E209185GR38E1W8124GK2GAHC5RR34D1P70X3RFG.json"
    );
    let _selector: fn(
        &locks_core::lock_policy::ContentLock,
        Option<&CreatorLockServicePointer>,
    ) -> locks_sdk::Result<LockServerPubky> = lock_server_for_content_lock;
    let _paykit_data_probe = locks_sdk::has_paykit_data;

    assert_eq!(LocksSession::new("another").export_secret(), "another");
}

#[test]
fn creator_content_lock_deletion_requests_use_closed_routes_and_modes() {
    let creator = LocksSession::new("frontend-session-secret").creator();
    let lock_id =
        locks_core::ids::LockId::from_str("000G40R40M30E209185GR38E1W8124GK2GAHC5RR34D1P70X3RFG")
            .unwrap();

    let default_graceful = creator.delete_content_lock(DeleteContentLockRequest {
        lock_id: lock_id.clone(),
        mode: DeleteContentLockMode::DefaultGraceful,
    });
    assert_eq!(default_graceful.method, "DELETE");
    assert_eq!(
        default_graceful.path,
        format!("/creator/content-locks/{lock_id}")
    );
    assert_eq!(
        default_graceful.authorization,
        "Bearer frontend-session-secret"
    );

    let explicit_graceful = creator.delete_content_lock(DeleteContentLockRequest {
        lock_id: lock_id.clone(),
        mode: DeleteContentLockMode::ExplicitGraceful,
    });
    assert_eq!(
        explicit_graceful.path,
        format!("/creator/content-locks/{lock_id}?graceful=true")
    );

    let force = creator.delete_content_lock(DeleteContentLockRequest {
        lock_id: lock_id.clone(),
        mode: DeleteContentLockMode::Force,
    });
    assert_eq!(
        force.path,
        format!("/creator/content-locks/{lock_id}?force=true")
    );

    let status = creator.get_content_lock_deletion(lock_id.clone());
    assert_eq!(status.method, "GET");
    assert_eq!(
        status.path,
        format!("/creator/content-locks/{lock_id}/deletion")
    );
    assert_eq!(status.body, SdkRequestBody::Empty);
}
