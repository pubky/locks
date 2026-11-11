#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn locks_http_provider_v2_deletion_bridge() {
    let _testnet_guard = PUBKY_TESTNET_LOCK.lock().await;
    let coord = std::path::PathBuf::from(std::env::var("LOCKS_PAYKIT_COORD_DIR").unwrap());
    let scenario = std::env::var("PROVIDER_V2_DELETION_SCENARIO").unwrap();
    let locks_key = read_coord_json(&coord, "locks-key.json").await;
    let trusted_key = locks_key["public_key"].as_str().unwrap();
    let fixture_signing_key = SigningKey::from_bytes(&[73; 32]);
    let fixture_key = pubky::PublicKey::from(
        pubky::pkarr::PublicKey::try_from(fixture_signing_key.verifying_key().as_bytes()).unwrap(),
    )
    .to_string();

    let database = TestDatabase::create().await;
    let config_text = format!(
        r#"
[http]
listen_addr = "0.0.0.0:3001"
[signed_services]
trusted_public_keys = ["{trusted_key}", "{fixture_key}"]
[setup]
allowed_origins = ["https://app.example"]
[paykit]
client_id = "app.paykit.server"
app_id = "paykit-server"
network = "testnet"
[bitcoin]
network = "testnet"
[electrum]
endpoint = "tcp://127.0.0.1:1"
poll_interval = "50ms"
request_timeout = "1s"
connect_retries = 0
[outbox]
poll_interval = "50ms"
batch_size = 16
lease_duration = "5s"
retry_initial = "1s"
retry_max = "2s"
[shutdown]
drain_timeout = "2s"
"#
    );
    let server_config = Config::from_toml_and_environment(
        &config_text,
        ConfigEnvironment {
            database_url: Some(database.database_url().to_owned()),
            master_key: Some(MASTER_KEY.to_owned()),
        },
    )
    .unwrap();
    let pool = initialize_database(&server_config).await.unwrap();
    let homeserver_database = TestDatabase::create().await;
    let testnet = build_static_pubky_testnet(homeserver_database.database_url()).await;
    let pubky = Pubky::testnet().unwrap();
    let auth_relay = testnet
        .http_relay()
        .local_url()
        .join("inbox")
        .unwrap()
        .to_string();
    let bootstrap = PubkySessionBootstrap::with_pubky(pubky, "app.paykit.server")
        .unwrap()
        .with_auth_relay(&auth_relay)
        .unwrap();
    let homeserver = PubkyPublicKey::from_public_key(&testnet.homeserver_app().public_key());
    let creators = CreatorStore::new(&pool, Arc::new(Crypto::from_master_key(&[1; 32]).unwrap()));
    let (reader, _reader_key, reader_sdk) = create_peer(&bootstrap, &homeserver).await;
    let mut creator = create_creator_with_auth_relay(
        &bootstrap,
        &homeserver,
        &creators,
        CreatorSpec {
            seed: 73,
            account_index: 0,
            amount_sats: 50_000,
            counter_seed: 7_300,
        },
        Some(&auth_relay),
    )
    .await;

    let config_path = coord.join("paykit.toml");
    std::fs::write(&config_path, config_text).unwrap();
    let mut server = Command::new(paykit_server_binary())
        .env("PAYKIT_CONFIG", &config_path)
        .env("PAYKIT_DATABASE_URL", database.database_url())
        .env("PAYKIT_MASTER_KEY", MASTER_KEY)
        .stdout(Stdio::null())
        .stderr(Stdio::inherit())
        .spawn()
        .unwrap();
    tokio::time::timeout(
        Duration::from_secs(60),
        wait_until_listening("127.0.0.1:3001".parse().unwrap()),
    )
    .await
    .expect("Paykit server did not become ready");
    write_coord_json(
        &coord,
        "paykit-ready.json",
        &serde_json::json!({
            "creator": creator.creator.to_string(),
            "reader": reader.to_string(),
        }),
    );

    let locks = read_coord_json(&coord, "locks-ready.json").await;
    creator.lock_resource = locks["lock_resource"].as_str().unwrap().to_owned();
    let bundle = locks["bundle_id"].as_str().unwrap();
    let deadline = locks["deadline"].as_str().unwrap();
    let request = || provider_v2_request(&fixture_signing_key, &creator, &reader, bundle, deadline);

    if scenario == "create-first" {
        let response = tokio::time::timeout(Duration::from_secs(600), async {
            loop {
                let response = send_http("127.0.0.1:3001".parse().unwrap(), request()).await;
                if response.status == StatusCode::OK {
                    break response;
                }
                assert_eq!(
                    response.status,
                    StatusCode::SERVICE_UNAVAILABLE,
                    "{}",
                    String::from_utf8_lossy(&response.body)
                );
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        })
        .await
        .expect("provider-v2 create did not become ready");
        assert_eq!(response.status, StatusCode::OK);
        observe_provider_v2_request(&reader_sdk, &creator, bundle).await;
        server.kill().unwrap();
        server.wait().unwrap();
        server = Command::new(paykit_server_binary())
            .env("PAYKIT_CONFIG", &config_path)
            .env("PAYKIT_DATABASE_URL", database.database_url())
            .env("PAYKIT_MASTER_KEY", MASTER_KEY)
            .stdout(Stdio::null())
            .stderr(Stdio::inherit())
            .spawn()
            .unwrap();
        tokio::time::timeout(
        Duration::from_secs(60),
        wait_until_listening("127.0.0.1:3001".parse().unwrap()),
    )
    .await
    .expect("Paykit server did not become ready");
        std::fs::write(coord.join("invoice-outcome"), b"created").unwrap();
    } else {
        assert_eq!(scenario, "drain-first");
        wait_coord_file(&coord, "fence-acknowledged").await;
        let response = tokio::time::timeout(Duration::from_secs(600), async {
            loop {
                let response = send_http("127.0.0.1:3001".parse().unwrap(), request()).await;
                if response.status != StatusCode::SERVICE_UNAVAILABLE {
                    break response;
                }
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        })
        .await
        .expect("provider-v2 fenced replay did not become ready");
        assert_eq!(
            response.status,
            StatusCode::CONFLICT,
            "{}",
            String::from_utf8_lossy(&response.body)
        );
        std::fs::write(coord.join("invoice-outcome"), b"fenced").unwrap();
    }

    tokio::time::timeout(Duration::from_secs(600), async {
        loop {
            if coord.join("locks-complete").exists() {
                break;
            }
            let _ = tokio::time::timeout(
                Duration::from_secs(5),
                drive_provider_v2_reader(&reader_sdk, &creator),
            )
            .await;
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("Locks deletion did not complete");
    let fenced: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM lock_payment_generations WHERE provider_v2_drain_fenced = TRUE AND active_drain_id IS NULL"
    ).fetch_one(&pool).await.unwrap();
    assert_eq!(
        fenced, 1,
        "cleanup must preserve permanent fence and clear active drain"
    );

    server.kill().unwrap();
    server.wait().unwrap();
    pool.close().await;
    drop(testnet);
    homeserver_database.cleanup().await;
    database.cleanup().await;
    std::fs::write(coord.join("paykit-complete"), b"ok").unwrap();
}

fn write_coord_json(coord: &std::path::Path, name: &str, value: &serde_json::Value) {
    let path = coord.join(name);
    let temporary = path.with_extension("tmp");
    std::fs::write(&temporary, serde_json::to_vec(value).unwrap()).unwrap();
    std::fs::rename(temporary, path).unwrap();
}

async fn read_coord_json(coord: &std::path::Path, name: &str) -> serde_json::Value {
    tokio::time::timeout(Duration::from_secs(600), async {
        loop {
            if let Ok(bytes) = std::fs::read(coord.join(name)) {
                return serde_json::from_slice(&bytes).unwrap();
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("timed out waiting for coordination file {name}"))
}

async fn wait_coord_file(coord: &std::path::Path, name: &str) {
    tokio::time::timeout(Duration::from_secs(600), async {
        while !coord.join(name).exists() {
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("timed out waiting for coordination file {name}"));
}

async fn observe_provider_v2_request(peer: &PeerSdk, fixture: &CreatorFixture, bundle: &str) {
    let creator = PubkyPublicKey::from_raw_or_app_key(fixture.creator.to_string()).unwrap();
    tokio::time::timeout(Duration::from_secs(600), async {
        loop {
            drive_provider_v2_reader(peer, fixture).await;
            if peer
                .received_payment_requests_from(&creator)
                .await
                .unwrap()
                .into_iter()
                .any(|request| {
                    request.terms.as_ref().is_some_and(|terms| {
                        terms.metadata.get("bundle_id") == Some(&serde_json::json!(bundle))
                    })
                })
            {
                return;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .unwrap();
}

async fn drive_provider_v2_reader(peer: &PeerSdk, fixture: &CreatorFixture) {
    let creator = PubkyPublicKey::from_raw_or_app_key(fixture.creator.to_string()).unwrap();
    let _ = peer.ensure_link_with_peer(creator.clone(), 8).await;
    let _ = peer.receive_private_messages(creator.clone()).await;
    peer.process_outbound_private_messages(creator).await.unwrap();
}
