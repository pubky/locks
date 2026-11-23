use std::collections::HashMap;
use std::str::FromStr;
use std::sync::Arc;

use async_trait::async_trait;
use locks_core::ids::{BundleId, CreatorPubky, PubkyLockResource};
use locks_core::lock_policy::PaykitPaymentAsset;
use locks_service::application::models::{PaymentProviderCreateRequest, PaymentProviderStatus};
use locks_service::application::ports::{
    PaykitAuthority, PaymentDrainCleanupToken, PaymentDrainClient, PaymentDrainClientError,
    PaymentDrainSummary, PaymentProviderClient, PaymentProviderCreateResponse,
    PaymentProviderError, PaymentRequestStatus,
};
use locks_service::infrastructure::verifiers::paykit_payment::{
    PaykitPaymentStatus, PaykitPaymentStatusClient, PaykitPaymentStatusError,
};
use uuid::Uuid;

use crate::config::{LockServerCredentialsConfig, PaykitConfig};
use crate::paykit_http_client::{
    PaykitClientError, PaykitConnectionStatusRequest, PaykitConnectionStatusResponse,
    PaykitHttpClient, PaykitInvoiceCreator, PaykitInvoiceRequest, PaykitInvoiceWindow,
    PaykitSetupStatusKind, PaykitSetupStatusProvider, PaykitSetupStatusProviderError,
};

/// Selects the Paykit Server for each creator: an operator-set
/// `paykit.creator_server_urls` entry, otherwise `paykit.server_url`.
///
/// Every request is signed with the same Lock Server key, so each configured
/// Paykit Server must trust that key.
pub struct PaykitRouter {
    default: Arc<PaykitHttpClient>,
    creators: HashMap<CreatorPubky, Arc<PaykitHttpClient>>,
}

impl PaykitRouter {
    pub fn from_config(
        paykit: &PaykitConfig,
        credentials: &LockServerCredentialsConfig,
    ) -> Result<Self, PaykitClientError> {
        let default = Arc::new(PaykitHttpClient::new(&paykit.server_url, credentials)?);
        let mut by_url = HashMap::from([(paykit.server_url.clone(), Arc::clone(&default))]);
        let mut creators = HashMap::with_capacity(paykit.creator_server_urls.len());
        for (creator, server_url) in &paykit.creator_server_urls {
            let client = match by_url.get(server_url) {
                Some(client) => Arc::clone(client),
                None => {
                    let client = Arc::new(PaykitHttpClient::new(server_url, credentials)?);
                    by_url.insert(server_url.clone(), Arc::clone(&client));
                    client
                }
            };
            creators.insert(creator.clone(), client);
        }
        Ok(Self { default, creators })
    }

    pub fn single(client: Arc<PaykitHttpClient>) -> Self {
        Self {
            default: client,
            creators: HashMap::new(),
        }
    }

    #[cfg(test)]
    pub(crate) fn with_creator(
        mut self,
        creator: CreatorPubky,
        client: Arc<PaykitHttpClient>,
    ) -> Self {
        self.creators.insert(creator, client);
        self
    }

    pub fn for_creator(&self, creator: &CreatorPubky) -> &PaykitHttpClient {
        self.creators.get(creator).unwrap_or(&self.default)
    }

    fn for_creator_text(&self, creator: &str) -> &PaykitHttpClient {
        CreatorPubky::from_str(creator).map_or(&self.default, |creator| self.for_creator(&creator))
    }

    fn for_lock_resource_text(&self, lock_resource: &str) -> &PaykitHttpClient {
        PubkyLockResource::from_str(lock_resource).map_or(&self.default, |lock_resource| {
            self.for_creator(lock_resource.creator())
        })
    }

    pub async fn connection_status(
        &self,
        request: &PaykitConnectionStatusRequest,
    ) -> Result<PaykitConnectionStatusResponse, PaykitClientError> {
        self.for_creator_text(&request.creator)
            .connection_status(request)
            .await
    }
}

#[async_trait]
impl PaykitInvoiceCreator for PaykitRouter {
    async fn create_invoice(
        &self,
        request_id: Uuid,
        request: &PaykitInvoiceRequest,
    ) -> Result<PaykitInvoiceWindow, PaykitClientError> {
        self.for_lock_resource_text(&request.lock_resource)
            .create_invoice(request_id, request)
            .await
    }
}

#[async_trait]
impl PaymentProviderClient for PaykitRouter {
    async fn create_invoice(
        &self,
        request: &PaymentProviderCreateRequest,
    ) -> Result<PaymentProviderCreateResponse, PaymentProviderError> {
        let client = self.for_creator(request.lock_resource.creator());
        PaymentProviderClient::create_invoice(client, request).await
    }

    async fn invoice_status(
        &self,
        creator: &CreatorPubky,
        bundle_id: &BundleId,
    ) -> Result<PaymentProviderStatus, PaymentProviderError> {
        PaymentProviderClient::invoice_status(self.for_creator(creator), creator, bundle_id).await
    }
}

#[async_trait]
impl PaymentDrainClient for PaykitRouter {
    fn authority(&self) -> Option<PaykitAuthority> {
        self.default.authority()
    }

    fn authority_for(&self, lock_resource: &PubkyLockResource) -> Option<PaykitAuthority> {
        self.for_creator(lock_resource.creator()).authority()
    }

    async fn start_payment_drain(
        &self,
        lock_resource: &PubkyLockResource,
    ) -> Result<PaymentDrainSummary, PaymentDrainClientError> {
        self.for_creator(lock_resource.creator())
            .start_payment_drain(lock_resource)
            .await
    }

    async fn lookup_payment_drain(
        &self,
        lock_resource: &PubkyLockResource,
    ) -> Result<Option<PaymentDrainSummary>, PaymentDrainClientError> {
        self.for_creator(lock_resource.creator())
            .lookup_payment_drain(lock_resource)
            .await
    }

    async fn cleanup_payment_drain(
        &self,
        lock_resource: &PubkyLockResource,
        cleanup_token: &PaymentDrainCleanupToken,
    ) -> Result<(), PaymentDrainClientError> {
        self.for_creator(lock_resource.creator())
            .cleanup_payment_drain(lock_resource, cleanup_token)
            .await
    }

    async fn payment_request_status(
        &self,
        creator: &CreatorPubky,
        bundle_id: &BundleId,
    ) -> Result<Option<PaymentRequestStatus>, PaymentDrainClientError> {
        PaymentDrainClient::payment_request_status(self.for_creator(creator), creator, bundle_id)
            .await
    }

    async fn replay_provider_v2_invoice(
        &self,
        request: &PaymentProviderCreateRequest,
    ) -> Result<PaymentProviderCreateResponse, PaymentDrainClientError> {
        self.for_creator(request.lock_resource.creator())
            .replay_provider_v2_invoice(request)
            .await
    }

    async fn provider_v2_invoice_status(
        &self,
        creator: &CreatorPubky,
        bundle_id: &BundleId,
    ) -> Result<Option<PaymentProviderStatus>, PaymentDrainClientError> {
        self.for_creator(creator)
            .provider_v2_invoice_status(creator, bundle_id)
            .await
    }
}

#[async_trait]
impl PaykitPaymentStatusClient for PaykitRouter {
    async fn payment_request_status(
        &self,
        creator: &CreatorPubky,
        bundle_id: &BundleId,
    ) -> Result<PaykitPaymentStatus, PaykitPaymentStatusError> {
        PaykitPaymentStatusClient::payment_request_status(
            self.for_creator(creator),
            creator,
            bundle_id,
        )
        .await
    }
}

#[async_trait]
impl PaykitSetupStatusProvider for PaykitRouter {
    async fn setup_status(
        &self,
        creator: &CreatorPubky,
        asset: PaykitPaymentAsset,
    ) -> Result<PaykitSetupStatusKind, PaykitSetupStatusProviderError> {
        PaykitSetupStatusProvider::setup_status(self.for_creator(creator), creator, asset).await
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use axum::Router;
    use axum::extract::{OriginalUri, State};
    use axum::http::StatusCode;
    use base64::Engine as _;
    use base64::engine::general_purpose::URL_SAFE_NO_PAD;
    use locks_core::ids::{ContentLockPath, LockServerPubky};
    use pubky_common::crypto::Keypair;
    use tokio::net::TcpListener;
    use url::Url;

    use super::*;
    use crate::testing::TestServerApp;

    const LOCK_ID: &str = "1111111111111111111111111111111111111111111111111111";
    const BUNDLE_ID: &str = "000G40R40M30E209185GR38E1W";

    fn creator(seed: u8) -> CreatorPubky {
        CreatorPubky::from_str(&Keypair::from_secret(&[seed; 32]).public_key().to_string()).unwrap()
    }

    fn lock_resource(creator: &CreatorPubky) -> PubkyLockResource {
        PubkyLockResource::new(
            creator.clone(),
            ContentLockPath::from_str(&format!("/pub/app.locks/{LOCK_ID}.json")).unwrap(),
        )
    }

    #[derive(Clone, Default)]
    struct Paths(Arc<Mutex<Vec<String>>>);

    impl Paths {
        fn take(&self) -> Vec<String> {
            std::mem::take(&mut *self.0.lock().unwrap())
        }
    }

    async fn record(State(paths): State<Paths>, uri: OriginalUri) -> StatusCode {
        paths.0.lock().unwrap().push(uri.path().to_owned());
        StatusCode::SERVICE_UNAVAILABLE
    }

    async fn spawn_recording_server() -> (String, Paths) {
        let paths = Paths::default();
        let app = Router::new().fallback(record).with_state(paths.clone());
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        (format!("http://{addr}"), paths)
    }

    fn test_client(server_url: &str) -> Arc<PaykitHttpClient> {
        Arc::new(
            PaykitHttpClient::new_for_test(server_url, Keypair::from_secret(&[9_u8; 32])).unwrap(),
        )
    }

    #[tokio::test]
    async fn routes_each_paykit_call_by_creator_and_falls_back_to_the_default_server() {
        let (default_url, default_paths) = spawn_recording_server().await;
        let (mapped_url, mapped_paths) = spawn_recording_server().await;
        let mapped = creator(1);
        let unmapped = creator(2);
        let router = PaykitRouter::single(test_client(&default_url))
            .with_creator(mapped.clone(), test_client(&mapped_url));
        let bundle_id = BundleId::from_str(BUNDLE_ID).unwrap();

        for (creator, paths, other) in [
            (&mapped, &mapped_paths, &default_paths),
            (&unmapped, &default_paths, &mapped_paths),
        ] {
            let resource = lock_resource(creator);
            let _ = PaykitInvoiceCreator::create_invoice(
                &router,
                Uuid::new_v4(),
                &PaykitInvoiceRequest {
                    bundle_id: BUNDLE_ID.to_owned(),
                    lock_resource: resource.to_string(),
                    reader: creator.to_string(),
                    payment_in: 1,
                },
            )
            .await;
            let _ = router
                .connection_status(&PaykitConnectionStatusRequest {
                    creator: creator.to_string(),
                    bundle_id: BUNDLE_ID.to_owned(),
                })
                .await;
            let _ = PaykitPaymentStatusClient::payment_request_status(&router, creator, &bundle_id)
                .await;
            let _ = PaymentProviderClient::invoice_status(&router, creator, &bundle_id).await;
            let _ =
                PaykitSetupStatusProvider::setup_status(&router, creator, PaykitPaymentAsset::Btc)
                    .await;
            let _ = router.start_payment_drain(&resource).await;
            let _ = router.lookup_payment_drain(&resource).await;

            assert_eq!(
                paths.take(),
                [
                    "/invoices",
                    "/connections/status",
                    "/payment-requests/status",
                    "/v2/invoices/status",
                    "/setup/status",
                    "/payment-request-drains",
                    "/payment-request-drain-lookups",
                ]
            );
            assert!(other.take().is_empty());
        }

        let mapped_port = Url::parse(&mapped_url).unwrap().port().unwrap();
        let default_port = Url::parse(&default_url).unwrap().port().unwrap();
        assert_eq!(
            router.authority_for(&lock_resource(&mapped)).unwrap().port,
            mapped_port
        );
        assert_eq!(
            router
                .authority_for(&lock_resource(&unmapped))
                .unwrap()
                .port,
            default_port
        );
        assert_eq!(router.authority().unwrap().port, default_port);
    }

    #[test]
    fn builds_one_signed_client_per_configured_origin() {
        let temp_dir = tempfile::tempdir().unwrap();
        let secret_path = temp_dir.path().join("secret.sess");
        std::fs::write(
            &secret_path,
            format!("keypair-seed:{}", URL_SAFE_NO_PAD.encode([9_u8; 32])),
        )
        .unwrap();
        let mut credentials = TestServerApp::default_in_memory_config().credentials;
        credentials.lock_server_secret_key = secret_path;
        credentials.lock_server_public_key =
            LockServerPubky::from_str(&Keypair::from_secret(&[9_u8; 32]).public_key().to_string())
                .unwrap();
        let shared_a = creator(1);
        let shared_b = creator(2);
        let on_default = creator(3);
        let unmapped = creator(4);
        let paykit = PaykitConfig {
            server_url: "https://paykit.default.example".to_owned(),
            minimum_confirmations: 0,
            creator_server_urls: HashMap::from([
                (shared_a.clone(), "https://paykit.shared.example".to_owned()),
                (shared_b.clone(), "https://paykit.shared.example".to_owned()),
                (
                    on_default.clone(),
                    "https://paykit.default.example".to_owned(),
                ),
            ]),
        };

        let router = PaykitRouter::from_config(&paykit, &credentials).unwrap();

        let host =
            |creator: &CreatorPubky| router.authority_for(&lock_resource(creator)).unwrap().host;
        assert_eq!(host(&shared_a), "paykit.shared.example");
        assert_eq!(host(&on_default), "paykit.default.example");
        assert_eq!(host(&unmapped), "paykit.default.example");
        assert!(std::ptr::eq(
            router.for_creator(&shared_a),
            router.for_creator(&shared_b)
        ));
        assert!(std::ptr::eq(
            router.for_creator(&on_default),
            router.for_creator(&unmapped)
        ));
    }
}
