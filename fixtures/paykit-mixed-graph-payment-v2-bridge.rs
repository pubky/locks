#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn locks_http_mixed_graph_payment_v2_bridge() {
    use std::os::unix::fs::PermissionsExt;

    let _testnet_guard = PUBKY_TESTNET_LOCK.lock().await;
    let coord = std::path::PathBuf::from(std::env::var("LOCKS_PAYKIT_COORD_DIR").unwrap());
    let locks_key = read_mixed_coord_json(&coord, "locks-key.json").await;
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
"#,
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
    let auth_relay = testnet.http_relay().local_url().join("inbox").unwrap().to_string();
    let bootstrap = PubkySessionBootstrap::with_pubky(pubky.clone(), "app.paykit.server")
        .unwrap()
        .with_auth_relay(&auth_relay)
        .unwrap();
    let homeserver = PubkyPublicKey::from_public_key(&testnet.homeserver_app().public_key());
    let creators = CreatorStore::new(&pool, Arc::new(Crypto::from_master_key(&[1; 32]).unwrap()));

    let reader_keypair = Keypair::random();
    let reader_root = PubkyLocalSecretKey::new(reader_keypair.secret_key());
    let reader_account = bootstrap
        .sign_up(
            &reader_root,
            &homeserver,
            None,
            paykit_sdk::PAYKIT_AUTHORIZER_SESSION_CAPABILITIES,
        )
        .await
        .unwrap();
    let reader = parse_reader(&format!("pubky{}", reader_account.public_key)).unwrap();
    let reader_sdk = sdk_fixtures::hosted_sdk(reader_account.access, "bitkit", 0).await;

    let creator_keypair = Keypair::random();
    let creator_root = PubkyLocalSecretKey::new(creator_keypair.secret_key());
    let creator_account = bootstrap
        .sign_up(
            &creator_root,
            &homeserver,
            None,
            paykit_sdk::PAYKIT_SESSION_CAPABILITIES,
        )
        .await
        .unwrap();
    let creator_authority = bootstrap
        .sign_in(
            &creator_root,
            "/pub/app.locks/:rw,/priv/app.locks/:rw",
        )
        .await
        .unwrap();
    #[allow(deprecated)]
    let creator_legacy_authority = pubky
        .signer(creator_keypair)
        .signin_cookie_blocking()
        .await
        .unwrap();
    #[allow(deprecated)]
    let creator_authority_secret = creator_legacy_authority
        .as_cookie()
        .expect("legacy creator authority must be cookie-backed")
        .export_secret()
        .expect("native legacy creator authority must expose its cookie secret");
    let creator = parse_creator(&format!("pubky{}", creator_account.public_key)).unwrap();
    let account_index = 0;
    let amount_sats = 50_000;
    let xpub = account_xpub(73, account_index);
    let address =
        derive_bip84_p2wpkh_address(&xpub, account_index, &BitcoinNetwork::Testnet, 0).unwrap();
    let account_session_secret = creator_account
        .export_session_secret()
        .await
        .unwrap()
        .into_inner();
    creators
        .create(&CreatorCredentials::new(
            creator.clone(),
            account_session_secret,
            creator_root.derive_paykit_identity_secret_key(1).unwrap(),
            Some(paykit_server::domain::receiving::BitcoinAccount {
                xpub: xpub.clone().into(),
                account_index,
            }),
            None,
        ))
        .await
        .unwrap();
    let observer_bootstrap = PubkySessionBootstrap::with_pubky(
        creator_account.access.outbox_client.clone(),
        "app.paykit.test-observer",
    )
    .unwrap()
    .with_auth_relay(&auth_relay)
    .unwrap();
    let observer = observer_bootstrap
        .sign_in(
            &creator_root,
            paykit_sdk::PAYKIT_AUTHORIZER_SESSION_CAPABILITIES,
        )
        .await
        .unwrap();
    let access = observer.access;
    let sdk = sdk_fixtures::hosted_sdk(access.clone(), "paykit-server", 7_300).await;
    creators.mark_setup_complete(&creator).await.unwrap();
    let mut creator = CreatorFixture {
        creator,
        sdk,
        access,
        lock_resource: String::new(),
        xpub,
        account_index,
        address,
        amount_sats,
        counter_seed: 7_300,
    };

    let authority_path = coord.join("creator-authority.secret");
    std::fs::write(&authority_path, creator_authority_secret).unwrap();
    std::fs::set_permissions(&authority_path, std::fs::Permissions::from_mode(0o600)).unwrap();
    write_mixed_coord_json(
        &coord,
        "paykit-ready.json",
        &serde_json::json!({
            "creator": creator.creator.to_string(),
            "reader": reader.to_string(),
        }),
    );

    let fixture = read_mixed_coord_json(&coord, "locks-fixture.json").await;
    let lock_path = fixture["content_lock_path"].as_str().unwrap();
    creator_authority
        .access
        .session
        .storage()
        .put_json(lock_path, &fixture["content_lock"])
        .await
        .unwrap();
    std::fs::write(coord.join("content-lock-published"), b"ok").unwrap();

    let config_path = coord.join("paykit.toml");
    std::fs::write(&config_path, config_text).unwrap();
    let spawn_server = || {
        Command::new(paykit_server_binary())
            .env("PAYKIT_CONFIG", &config_path)
            .env("PAYKIT_DATABASE_URL", database.database_url())
            .env("PAYKIT_MASTER_KEY", MASTER_KEY)
            .stdout(Stdio::null())
            .stderr(Stdio::inherit())
            .spawn()
            .unwrap()
    };
    let mut server = spawn_server();
    tokio::time::timeout(
        Duration::from_secs(60),
        wait_until_listening("127.0.0.1:3001".parse().unwrap()),
    )
    .await
    .expect("Paykit server did not become ready");

    let folded_auth = read_mixed_coord_json(&coord, "folded-auth.json").await;
    pubky
        .signer(reader_keypair)
        .approve_auth(folded_auth["auth_url"].as_str().unwrap())
        .await
        .unwrap();
    std::fs::write(coord.join("folded-auth-approved"), b"ok").unwrap();

    let locks = read_mixed_coord_json(&coord, "locks-ready.json").await;
    creator.lock_resource = locks["lock_resource"].as_str().unwrap().to_owned();
    let bundle = locks["bundle_id"].as_str().unwrap();
    accept_provider_v2_request(&reader_sdk, &creator, bundle).await;

    server.kill().unwrap();
    server.wait().unwrap();
    server = spawn_server();
    tokio::time::timeout(
        Duration::from_secs(60),
        wait_until_listening("127.0.0.1:3001".parse().unwrap()),
    )
    .await
    .expect("restarted Paykit server did not become ready");
    std::fs::write(coord.join("paykit-restarted-and-accepted"), b"ok").unwrap();

    wait_mixed_coord_file(&coord, "locks-complete").await;
    let entitlement_path = format!("/priv/app.locks/proofs/{bundle}.json");
    let readback: serde_json::Value = creator_authority
        .access
        .session
        .storage()
        .get_json(&entitlement_path)
        .await
        .unwrap();
    write_mixed_coord_json(&coord, "entitlement-readback.json", &readback);

    server.kill().unwrap();
    server.wait().unwrap();
    pool.close().await;
    drop(testnet);
    homeserver_database.cleanup().await;
    database.cleanup().await;
    std::fs::write(coord.join("paykit-complete"), b"ok").unwrap();
}

fn write_mixed_coord_json(coord: &std::path::Path, name: &str, value: &serde_json::Value) {
    let path = coord.join(name);
    let temporary = path.with_extension("tmp");
    std::fs::write(&temporary, serde_json::to_vec(value).unwrap()).unwrap();
    std::fs::rename(temporary, path).unwrap();
}

async fn read_mixed_coord_json(coord: &std::path::Path, name: &str) -> serde_json::Value {
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

async fn wait_mixed_coord_file(coord: &std::path::Path, name: &str) {
    tokio::time::timeout(Duration::from_secs(600), async {
        while !coord.join(name).exists() {
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("timed out waiting for coordination file {name}"));
}
