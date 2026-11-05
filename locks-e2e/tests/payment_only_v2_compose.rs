use std::str::FromStr;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use locks_core::ids::{BundleId, CreatorPubky, LockHash, LockId, PubkyLockResource, TaskId};
use locks_core::lock_policy::PaymentPolicyV2;
use locks_core::verification::{
    SUBMITTED_PROOF_BUNDLE_V2_VERSION, SatisfiedByV2, SubmittedProofBundleV2,
};
use locks_server::config::{
    FilesystemLockServerIdentityProvider, LockServerCredentialsConfig, LockServerIdentityProvider,
};
use locks_server::paykit_http_client::PaykitHttpClient;
use locks_server::payment_worker::{PaymentVerificationWorker, PaymentWorkerTick};
use locks_service::application::models::{
    PaymentProviderCreateRequest, PaymentProviderStatus, PaymentRootState, PaymentSampleStatusV2,
    VerificationTaskStatus,
};
use locks_service::application::ports::{
    AdmitPaymentTask, Clock, EntitlementRepository, PaymentProviderClient,
    PaymentProviderCreateResponse, PaymentProviderError, PaymentTaskRepository,
};
use locks_service::infrastructure::memory::entitlements::InMemoryEntitlementRepository;
use locks_service::infrastructure::postgres::{PostgresPaymentTaskRepository, run_migrations};
use serde_json::Value;
use sqlx::postgres::PgPoolOptions;

const CREATOR: &str = "pubkytkrq8zmwb8a3m9k15csu3q17qmfgqnp9dskbrg9uq1rydpyxp7qy";
const READER: &str = "pubkyorhzqdiexwmi6iidktucgud63ufa5nwtsuzdxe176a8izd6jsqky";
const BUNDLE: &str = "000G40R40M30E209185GR38E1W";
const TASK: &str = "123e4567-e89b-42d3-a456-426614174001";

#[derive(Debug)]
struct MutableClock(Mutex<time::OffsetDateTime>);

impl MutableClock {
    fn set(&self, now: time::OffsetDateTime) {
        *self.0.lock().unwrap() = now;
    }
}

impl Clock for MutableClock {
    fn now(&self) -> time::OffsetDateTime {
        *self.0.lock().unwrap()
    }
}

struct DeadlineCrossingProvider {
    inner: Arc<PaykitHttpClient>,
    clock: Arc<MutableClock>,
    after_status_at: time::OffsetDateTime,
}

#[async_trait]
impl PaymentProviderClient for DeadlineCrossingProvider {
    async fn create_invoice(
        &self,
        request: &PaymentProviderCreateRequest,
    ) -> Result<PaymentProviderCreateResponse, PaymentProviderError> {
        PaymentProviderClient::create_invoice(self.inner.as_ref(), request).await
    }

    async fn invoice_status(
        &self,
        creator: &CreatorPubky,
        bundle_id: &BundleId,
    ) -> Result<PaymentProviderStatus, PaymentProviderError> {
        let response =
            PaymentProviderClient::invoice_status(self.inner.as_ref(), creator, bundle_id).await;
        self.clock.set(self.after_status_at);
        response
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct RoundSummary {
    status: VerificationTaskStatus,
    root: PaymentRootState,
    revision: u64,
    create_calls: u64,
    status_calls: u64,
}

#[tokio::test]
async fn payment_only_v2_server_compose_uses_http_and_persists_across_restart() {
    let database_url =
        std::env::var("TEST_DATABASE_URL").expect("Compose E2E requires TEST_DATABASE_URL");
    let provider_url = std::env::var("PAYMENT_V2_FAKE_PROVIDER_URL")
        .expect("Compose E2E requires PAYMENT_V2_FAKE_PROVIDER_URL");
    let server_url = std::env::var("PAYMENT_V2_LOCKS_SERVER_URL")
        .expect("Compose E2E requires PAYMENT_V2_LOCKS_SERVER_URL");
    let phase = std::env::var("PAYMENT_V2_E2E_PHASE").expect("Compose E2E requires phase");
    let http = reqwest::Client::new();
    wait_for_server(&http, &server_url).await;
    let pool = PgPoolOptions::new()
        .max_connections(4)
        .connect(&database_url)
        .await
        .unwrap();

    if phase == "seed" {
        sqlx::query("TRUNCATE verification_tasks CASCADE")
            .execute(&pool)
            .await
            .unwrap();
        http.post(format!("{provider_url}/reset"))
            .send()
            .await
            .unwrap()
            .error_for_status()
            .unwrap();
        PostgresPaymentTaskRepository::new(pool.clone())
            .admit_payment_task(admission(TASK))
            .await
            .unwrap();
    } else {
        assert_eq!(phase, "verify");
    }

    let lifecycle = wait_for_terminal_lifecycle(&http, &server_url).await;
    assert_eq!(lifecycle["status"], "expired");
    assert_eq!(lifecycle["terminal_reason"], "payment_request_rejected");
    let observations: Value = http
        .get(format!("{provider_url}/observations"))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(observations["create_calls"], 1);
    assert_eq!(observations["status_calls"], 1);
    assert_eq!(observations["signed_create"], true);
    assert_eq!(observations["signed_status"], true);
    pool.close().await;
}

async fn wait_for_server(http: &reqwest::Client, server_url: &str) {
    for _ in 0..240 {
        if http
            .get(format!("{server_url}/healthz"))
            .send()
            .await
            .is_ok_and(|response| response.status().is_success())
        {
            return;
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    panic!("Lock Server did not become healthy");
}

async fn wait_for_terminal_lifecycle(http: &reqwest::Client, server_url: &str) -> Value {
    for _ in 0..120 {
        if let Ok(response) = http
            .post(format!("{server_url}/verification-task-lookups"))
            .json(&serde_json::json!({"creator": CREATOR, "bundle_id": BUNDLE}))
            .send()
            .await
            && response.status().is_success()
        {
            let body: Value = response.json().await.unwrap();
            if body["status"] == "expired" {
                return body;
            }
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    panic!("payment lifecycle did not terminalize through Lock Server");
}

#[tokio::test]
async fn payment_only_v2_compose_is_deadline_exact_restart_safe_and_repeatable() {
    let database_url =
        std::env::var("TEST_DATABASE_URL").expect("Compose E2E requires TEST_DATABASE_URL");
    let provider_url = std::env::var("PAYMENT_V2_FAKE_PROVIDER_URL")
        .unwrap_or_else(|_| "http://127.0.0.1:18082".to_owned());
    let pool = PgPoolOptions::new()
        .max_connections(8)
        .connect(&database_url)
        .await
        .unwrap();
    run_migrations(&pool).await.unwrap();

    let secret_dir = tempfile::tempdir().unwrap();
    let secret_path = secret_dir.path().join("lock-server.keypair-seed");
    let public_key = FilesystemLockServerIdentityProvider
        .generate_secret(&secret_path)
        .unwrap();
    let client = Arc::new(
        PaykitHttpClient::new(
            &provider_url,
            &LockServerCredentialsConfig {
                lock_server_secret_key: secret_path,
                lock_server_public_key: public_key,
                max_ttl_seconds: 900,
            },
        )
        .unwrap(),
    );

    let first = run_round(&pool, &provider_url, client.clone()).await;
    let second = run_round(&pool, &provider_url, client).await;
    assert_eq!(
        second, first,
        "fresh repeated runs must have the same outcome"
    );

    pool.close().await;
}

async fn run_round(
    pool: &sqlx::PgPool,
    provider_url: &str,
    client: Arc<PaykitHttpClient>,
) -> RoundSummary {
    sqlx::query("TRUNCATE verification_tasks CASCADE")
        .execute(pool)
        .await
        .unwrap();
    let http = reqwest::Client::new();
    http.post(format!("{provider_url}/reset"))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap();

    let repository = Arc::new(PostgresPaymentTaskRepository::new(pool.clone()));
    let admitted = repository
        .admit_payment_task(admission(TASK))
        .await
        .unwrap();
    let accepted_at = admitted.snapshot.as_v1().accepted_at;
    let deadline = admitted.snapshot.as_v1().deadline_at;
    assert_eq!(deadline - accepted_at, time::Duration::seconds(60));

    let clock = Arc::new(MutableClock(Mutex::new(accepted_at)));
    let entitlements = Arc::new(InMemoryEntitlementRepository::new());
    let provider = Arc::new(DeadlineCrossingProvider {
        inner: client,
        clock: clock.clone(),
        after_status_at: deadline + time::Duration::milliseconds(1),
    });
    let create_worker = worker(
        repository.clone(),
        entitlements.clone(),
        provider.clone(),
        clock.clone(),
        "payment-create-worker",
    );
    assert_eq!(
        create_worker.run_once().await.unwrap(),
        PaymentWorkerTick::RetryScheduled(admitted.task_id)
    );

    let created = repository
        .get_payment_task_by_handle(&creator(), &bundle())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(created.evaluator_revision, 1);
    assert!(matches!(
        created.snapshot.as_v1().invoice,
        locks_service::application::models::PaymentInvoiceState::Created { .. }
    ));

    // Reconstruct the durable adapter and worker after invoice creation. The
    // one-millisecond retry is intentionally allowed to become claimable.
    tokio::time::sleep(Duration::from_millis(20)).await;
    let restarted_repository = Arc::new(PostgresPaymentTaskRepository::new(pool.clone()));
    clock.set(deadline - time::Duration::milliseconds(1));
    let status_worker = worker(
        restarted_repository.clone(),
        entitlements.clone(),
        provider,
        clock,
        "payment-status-worker",
    );
    assert_eq!(
        status_worker.run_once().await.unwrap(),
        PaymentWorkerTick::Terminal(admitted.task_id)
    );

    let completed = restarted_repository
        .get_payment_task_by_handle(&creator(), &bundle())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(completed.status, VerificationTaskStatus::Completed);
    assert_eq!(completed.snapshot.as_v1().root, PaymentRootState::Satisfied);
    let provider_status = completed.snapshot.as_v1().provider_status.as_ref().unwrap();
    assert_eq!(
        provider_status
            .first_threshold_satisfied
            .as_ref()
            .unwrap()
            .observed_at,
        deadline,
        "threshold evidence at the exact deadline is timely"
    );
    assert_eq!(
        provider_status.current_sample.as_ref().unwrap().status,
        PaymentSampleStatusV2::Undetected,
        "current state models a post-threshold reorg"
    );
    assert!(
        provider_status.first_threshold_satisfied.is_some(),
        "first threshold evidence remains sticky across the reorg"
    );

    let entitlement = entitlements
        .get_verified_proof_bundle_v2(&creator(), &bundle())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(entitlement.satisfied_by, [SatisfiedByV2::Payment]);

    let observations: Value = http
        .get(format!("{provider_url}/observations"))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(observations["create_calls"], 1);
    assert_eq!(observations["status_calls"], 1);
    assert_eq!(observations["signed_create"], true);
    assert_eq!(observations["signed_status"], true);
    let observed_deadline = time::OffsetDateTime::parse(
        observations["last_create"]["deadline"].as_str().unwrap(),
        &time::format_description::well_known::Rfc3339,
    )
    .unwrap();
    assert_eq!(observed_deadline, deadline);
    assert_eq!(observations["last_status"]["creator"], CREATOR);
    assert_eq!(observations["last_status"]["bundle_id"], BUNDLE);

    let replay = restarted_repository
        .admit_payment_task(admission("123e4567-e89b-42d3-a456-426614174099"))
        .await
        .unwrap();
    assert_eq!(replay.task_id, admitted.task_id);
    assert_eq!(
        status_worker.run_once().await.unwrap(),
        PaymentWorkerTick::Idle
    );

    let after_replay: Value = http
        .get(format!("{provider_url}/observations"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(after_replay["create_calls"], observations["create_calls"]);
    assert_eq!(after_replay["status_calls"], observations["status_calls"]);

    RoundSummary {
        status: completed.status,
        root: completed.snapshot.as_v1().root,
        revision: completed.evaluator_revision,
        create_calls: observations["create_calls"].as_u64().unwrap(),
        status_calls: observations["status_calls"].as_u64().unwrap(),
    }
}

fn worker(
    repository: Arc<PostgresPaymentTaskRepository>,
    entitlements: Arc<InMemoryEntitlementRepository>,
    provider: Arc<DeadlineCrossingProvider>,
    clock: Arc<MutableClock>,
    worker_id: &str,
) -> PaymentVerificationWorker {
    PaymentVerificationWorker::new(
        repository,
        entitlements,
        provider,
        clock,
        worker_id.to_owned(),
        time::Duration::seconds(30),
    )
}

fn admission(task_id: &str) -> AdmitPaymentTask {
    let hash = LockHash::from_bytes([7; 32]);
    let resource = PubkyLockResource::from_str(&format!(
        "{CREATOR}/pub/app.locks/{}.json",
        LockId::from_hash(hash)
    ))
    .unwrap();
    AdmitPaymentTask {
        task_id: TaskId::from_str(task_id).unwrap(),
        submitted_proof_bundle: SubmittedProofBundleV2 {
            version: SUBMITTED_PROOF_BUNDLE_V2_VERSION,
            bundle_id: bundle(),
            pubky_lock_resource: resource,
            reader_pubky: Some(reader()),
        },
        lock_hash: [7; 32],
        payment: PaymentPolicyV2 {
            recipient_pubky: creator(),
            amount: "50000".to_owned(),
            asset: "BTC".to_owned(),
            minimum_confirmations: 1,
        },
        verification_window_seconds: 60,
    }
}

fn creator() -> CreatorPubky {
    CreatorPubky::from_str(CREATOR).unwrap()
}

fn reader() -> CreatorPubky {
    CreatorPubky::from_str(READER).unwrap()
}

fn bundle() -> BundleId {
    BundleId::from_str(BUNDLE).unwrap()
}
