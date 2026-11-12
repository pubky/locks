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

    let scenarios = [
        ("graph", "000G40R40M30E209185GR38E1W", false, true),
        ("payment", "000G40R40M30E209185GR38E2W", true, true),
        ("combined", "000G40R40M30E209185GR38E3W", true, true),
        ("blocked", "000G40R40M30E209185GR38E4W", false, false),
    ];
    let invoices = paykit_server::persistence::InvoiceStore::new(
        &pool,
        Arc::new(Crypto::from_master_key(&[1; 32]).unwrap()),
    );

    for (index, (name, bundle, paid, accept)) in scenarios.iter().enumerate() {
        let mut provider_barrier = None;
        if index == 0 {
            wait_mixed_coord_file(&coord, "arm-provider-create").await;
            let mut connection = pool.acquire().await.unwrap();
            sqlx::query("SELECT pg_advisory_lock(8702604)")
                .execute(&mut *connection)
                .await
                .unwrap();
            sqlx::raw_sql(
                "CREATE FUNCTION stage3d_block_provider_create() RETURNS trigger LANGUAGE plpgsql AS $$
                 BEGIN PERFORM pg_advisory_xact_lock(8702604); RETURN NEW; END $$;
                 CREATE TRIGGER stage3d_block_provider_create BEFORE INSERT ON invoice_provider_v2
                 FOR EACH ROW EXECUTE FUNCTION stage3d_block_provider_create();",
            )
            .execute(&pool)
            .await
            .unwrap();
            provider_barrier = Some(connection);
            std::fs::write(coord.join("provider-create-armed"), b"ok").unwrap();
        }

        let folded_auth =
            read_mixed_coord_json(&coord, &format!("folded-auth-{name}.json")).await;
        pubky
            .signer(reader_keypair.clone())
            .approve_auth(folded_auth["auth_url"].as_str().unwrap())
            .await
            .unwrap();
        std::fs::write(coord.join(format!("folded-auth-approved-{name}")), b"ok").unwrap();

        let locks =
            read_mixed_coord_json(&coord, &format!("locks-ready-{name}.json")).await;
        creator.lock_resource = locks["lock_resource"].as_str().unwrap().to_owned();

        if index == 0 {
            wait_for_database_wait(&pool, "invoice_provider_v2").await;
            std::fs::write(coord.join("provider-create-blocked"), b"ok").unwrap();
            wait_mixed_coord_file(&coord, "release-provider-create").await;
            cancel_database_wait(&pool, "invoice_provider_v2").await;
            let mut connection = provider_barrier.take().unwrap();
            sqlx::query("SELECT pg_advisory_unlock(8702604)")
                .execute(&mut *connection)
                .await
                .unwrap();
            drop(connection);
            sqlx::raw_sql(
                "DROP TRIGGER stage3d_block_provider_create ON invoice_provider_v2;
                 DROP FUNCTION stage3d_block_provider_create();",
            )
            .execute(&pool)
            .await
            .unwrap();
            let (invoice_count, provider_count): (i64, i64) = sqlx::query_as(
                "SELECT
                   (SELECT COUNT(*) FROM invoices),
                   (SELECT COUNT(*) FROM invoice_provider_v2)",
            )
            .fetch_one(&pool)
            .await
            .unwrap();
            assert_eq!((invoice_count, provider_count), (0, 0));
            std::fs::write(coord.join("provider-create-released"), b"ok").unwrap();
        }

        let expected = i64::try_from(index + 1).unwrap();
        let (invoice_count, provider_count): (i64, i64) = tokio::time::timeout(
            Duration::from_secs(60),
            async {
                loop {
                    let counts: (i64, i64) = sqlx::query_as(
                        "SELECT
                           (SELECT COUNT(*) FROM invoices),
                           (SELECT COUNT(*) FROM invoice_provider_v2)",
                    )
                    .fetch_one(&pool)
                    .await
                    .unwrap();
                    if counts == (expected, expected) {
                        break counts;
                    }
                    tokio::time::sleep(Duration::from_millis(25)).await;
                }
            },
        )
        .await
        .expect("Paykit invoice/provider cardinality did not settle");
        assert_eq!((invoice_count, provider_count), (expected, expected));
        std::fs::write(coord.join(format!("provider-ready-{name}")), b"ok").unwrap();

        if index == 0 {
            wait_mixed_coord_file(&coord, "locks-invoice-persisted").await;
            server.kill().unwrap();
            server.wait().unwrap();
            std::fs::write(coord.join("paykit-stopped-after-create"), b"ok").unwrap();
            wait_mixed_coord_file(&coord, "resume-paykit-after-create").await;
            server = spawn_server();
            tokio::time::timeout(
                Duration::from_secs(60),
                wait_until_listening("127.0.0.1:3001".parse().unwrap()),
            )
            .await
            .expect("restarted Paykit server did not become ready");
            std::fs::write(coord.join("paykit-resumed-after-create"), b"ok").unwrap();
        }

        if *accept {
            wait_mixed_coord_file(&coord, &format!("accept-provider-{name}")).await;
            accept_provider_v2_request(&reader_sdk, &creator, bundle).await;
        }
        if *paid {
            creator.address = derive_bip84_p2wpkh_address(
                &creator.xpub,
                creator.account_index,
                &BitcoinNetwork::Testnet,
                i64::try_from(index).unwrap(),
            )
            .unwrap();
            let targets = invoices
                .observation_targets()
                .await
                .unwrap()
                .into_iter()
                .filter(|target| target.address() == creator.address)
                .collect::<Vec<_>>();
            assert_eq!(targets.len(), 1, "paid scenario must own one observation target");
            let observer = DeterministicElectrum {
                outputs: std::collections::HashMap::from([(
                    creator.address.clone(),
                    (
                        creator.amount_sats,
                        OutPoint::new(Txid::from_byte_array([(index + 11) as u8; 32]), 0),
                    ),
                )]),
            };
            assert_eq!(
                paykit_server::workers::observer::observe_once_at(
                    &observer,
                    &invoices,
                    &BitcoinNetwork::Testnet,
                    &targets,
                    time::OffsetDateTime::now_utc(),
                )
                .await
                .unwrap(),
                1
            );
        }
        std::fs::write(
            coord.join(format!("provider-evaluation-ready-{name}")),
            b"ok",
        )
        .unwrap();

        wait_mixed_coord_file(&coord, &format!("locks-complete-{name}")).await;
        if *accept {
            let entitlement_path = format!("/priv/app.locks/proofs/{bundle}.json");
            let readback: serde_json::Value = creator_authority
                .access
                .session
                .storage()
                .get_json(&entitlement_path)
                .await
                .unwrap();
            write_mixed_coord_json(
                &coord,
                &format!("entitlement-readback-{name}.json"),
                &readback,
            );
        }
    }

    wait_mixed_coord_file(&coord, "locks-complete").await;

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

async fn cancel_database_wait(pool: &sqlx::PgPool, marker: &str) {
    let canceled: Vec<bool> = sqlx::query_scalar(
        "SELECT pg_cancel_backend(pid)
         FROM pg_stat_activity
         WHERE datname = current_database()
           AND pid <> pg_backend_pid()
           AND wait_event_type = 'Lock'
           AND query ILIKE '%' || $1 || '%'",
    )
    .bind(marker)
    .fetch_all(pool)
    .await
    .unwrap();
    assert_eq!(
        canceled,
        vec![true],
        "exactly one blocked provider transaction must be canceled"
    );

    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let waiting: i64 = sqlx::query_scalar(
                "SELECT COUNT(*)
                 FROM pg_stat_activity
                 WHERE datname = current_database()
                   AND pid <> pg_backend_pid()
                   AND wait_event_type = 'Lock'
                   AND query ILIKE '%' || $1 || '%'",
            )
            .bind(marker)
            .fetch_one(pool)
            .await
            .unwrap();
            if waiting == 0 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .expect("canceled provider transaction remained blocked");
}
