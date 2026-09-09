#[cfg(any(test, target_arch = "wasm32"))]
use serde_json::Value;
use wasm_bindgen::prelude::*;

use crate::creator::Creator;
#[cfg(target_arch = "wasm32")]
use crate::js_error::{JsResult, invalid_input};
use crate::locks::LocksOptions;
#[cfg(target_arch = "wasm32")]
use futures_lite::{StreamExt, pin};
#[cfg(target_arch = "wasm32")]
use std::collections::BTreeMap;
#[cfg(any(test, target_arch = "wasm32"))]
use std::num::NonZeroUsize;
#[cfg(any(test, target_arch = "wasm32"))]
use std::sync::{Arc, OnceLock};

#[cfg(any(test, target_arch = "wasm32"))]
#[cfg_attr(target_arch = "wasm32", allow(dead_code))]
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct JsAuthorizedRequestPlan {
    pub method: &'static str,
    pub path: String,
    pub url: url::Url,
    pub authorization: String,
    pub content_type: Option<String>,
    pub body: JsRequestBody,
}

#[cfg(any(test, target_arch = "wasm32"))]
#[cfg_attr(target_arch = "wasm32", allow(dead_code))]
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum JsRequestBody {
    Json(Value),
    Bytes(Vec<u8>),
    Empty,
}

#[cfg(any(test, target_arch = "wasm32"))]
#[cfg_attr(target_arch = "wasm32", allow(dead_code))]
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct JsPreparedAuthorizedRequest {
    pub method: &'static str,
    pub path: String,
    pub url: url::Url,
    pub pubky_host: Option<String>,
    pub authorization: String,
    pub content_type: Option<String>,
    pub body: JsRequestBody,
}

#[cfg(test)]
pub(crate) trait BrowserEndpointResolver {
    fn resolve_browser_endpoint(
        &self,
        qname: &str,
    ) -> locks_sdk::Result<locks_sdk::transport::BrowserEndpoint>;
}

#[cfg(target_arch = "wasm32")]
#[derive(Debug, Clone)]
pub(crate) struct BrowserPkarrResolver {
    client: pkarr8::Client,
}

#[cfg(any(test, target_arch = "wasm32"))]
fn browser_pkarr_cache() -> Arc<dyn pkarr8::Cache> {
    static CACHE: OnceLock<Arc<dyn pkarr8::Cache>> = OnceLock::new();

    CACHE
        .get_or_init(|| {
            Arc::new(pkarr8::InMemoryCache::new(
                NonZeroUsize::new(pkarr8::DEFAULT_CACHE_SIZE)
                    .expect("pkarr default cache size is non-zero"),
            ))
        })
        .clone()
}

#[cfg(target_arch = "wasm32")]
fn build_browser_pkarr_client(options: &LocksOptions) -> locks_sdk::Result<pkarr8::Client> {
    let mut builder = pkarr8::Client::builder();
    builder.cache(browser_pkarr_cache());
    if !options.pkarr_relay_urls().is_empty() {
        builder
            .relays(options.pkarr_relay_urls())
            .map_err(|_| locks_sdk::LocksSdkError::MissingBrowserDomainEndpoint)?;
    }
    builder
        .build()
        .map_err(|_| locks_sdk::LocksSdkError::MissingBrowserDomainEndpoint)
}

#[cfg(any(test, target_arch = "wasm32"))]
fn creator_homeserver_qname_from_packet(
    packet: &pkarr8::SignedPacket,
) -> locks_sdk::Result<String> {
    let qname = packet
        .resource_records("_pubky")
        .find_map(|record| match &record.rdata {
            pkarr8::dns::rdata::RData::SVCB(svcb) => Some(svcb.target.to_string()),
            pkarr8::dns::rdata::RData::HTTPS(https) => Some(https.0.target.to_string()),
            _ => None,
        })
        .ok_or(locks_sdk::LocksSdkError::MissingBrowserDomainEndpoint)?;
    let homeserver = pkarr8::PublicKey::try_from(qname.as_str())
        .map_err(|_| locks_sdk::LocksSdkError::MissingBrowserDomainEndpoint)?;
    Ok(homeserver.to_z32())
}

#[cfg(target_arch = "wasm32")]
impl BrowserPkarrResolver {
    pub(crate) fn new_with_options(options: &LocksOptions) -> locks_sdk::Result<Self> {
        Ok(Self {
            client: build_browser_pkarr_client(options)?,
        })
    }

    pub(crate) async fn resolve_browser_endpoint(
        &self,
        qname: &str,
    ) -> locks_sdk::Result<locks_sdk::transport::BrowserEndpoint> {
        let stream = self.client.resolve_https_endpoints(qname);
        pin!(stream);

        while let Some(endpoint) = stream.next().await {
            if let Some(browser_endpoint) = browser_endpoint_from_pkarr_endpoint(&endpoint) {
                return Ok(browser_endpoint);
            }
        }

        Err(locks_sdk::LocksSdkError::MissingBrowserDomainEndpoint)
    }

    pub(crate) async fn resolve_browser_endpoint_for_creator_qname(
        &self,
        qname: &str,
    ) -> locks_sdk::Result<locks_sdk::transport::BrowserEndpoint> {
        let creator = pkarr8::PublicKey::try_from(qname)
            .map_err(|_| locks_sdk::LocksSdkError::MissingBrowserDomainEndpoint)?;
        let packet = self
            .client
            .resolve(&creator, pkarr8::ResolvePolicy::CacheFirst)
            .await
            .map_err(|_| locks_sdk::LocksSdkError::MissingBrowserDomainEndpoint)?;
        let homeserver_qname = creator_homeserver_qname_from_packet(&packet)?;
        self.resolve_browser_endpoint(&homeserver_qname).await
    }
}

#[cfg(target_arch = "wasm32")]
fn browser_endpoint_from_pkarr_endpoint(
    endpoint: &pkarr8::extra::endpoints::Endpoint,
) -> Option<locks_sdk::transport::BrowserEndpoint> {
    let domain = endpoint.domain()?.to_owned();
    let mut params = BTreeMap::new();
    if let Some(http_port) = endpoint
        .get_param(locks_sdk::transport::HTTP_PORT_PARAM)
        .and_then(http_port_from_svc_param)
    {
        params.insert(locks_sdk::transport::HTTP_PORT_PARAM, http_port);
    }

    Some(locks_sdk::transport::BrowserEndpoint {
        domain: Some(domain),
        port: endpoint.port(),
        params,
    })
}

#[cfg(target_arch = "wasm32")]
fn http_port_from_svc_param(param: &pkarr8::dns::rdata::SVCParam<'_>) -> Option<u16> {
    match param {
        pkarr8::dns::rdata::SVCParam::Unknown(_, bytes) => <[u8; 2]>::try_from(bytes.as_ref())
            .ok()
            .map(u16::from_be_bytes),
        pkarr8::dns::rdata::SVCParam::Port(port) => Some(*port),
        _ => None,
    }
}

#[cfg(any(test, target_arch = "wasm32"))]
impl JsAuthorizedRequestPlan {
    #[cfg_attr(target_arch = "wasm32", allow(dead_code))]
    pub(crate) fn prepare_for_browser_endpoint(
        &self,
        endpoint: &locks_sdk::transport::BrowserEndpoint,
        testnet_host: Option<&str>,
    ) -> locks_sdk::Result<JsPreparedAuthorizedRequest> {
        let browser_request = locks_sdk::transport::rewrite_browser_request(
            self.url.as_str(),
            endpoint,
            testnet_host,
        )?;

        Ok(JsPreparedAuthorizedRequest {
            method: self.method,
            path: self.path.clone(),
            url: browser_request.url,
            pubky_host: browser_request.pubky_host,
            authorization: self.authorization.clone(),
            content_type: self.content_type.clone(),
            body: self.body.clone(),
        })
    }

    #[cfg(test)]
    pub(crate) fn prepare_with_resolver(
        &self,
        resolver: &dyn BrowserEndpointResolver,
        testnet_host: Option<&str>,
    ) -> locks_sdk::Result<JsPreparedAuthorizedRequest> {
        let qname = self
            .url
            .host_str()
            .and_then(|host| host.strip_prefix("_pubky."))
            .ok_or(locks_sdk::LocksSdkError::InvalidTransportUrl)?;
        let endpoint = resolver.resolve_browser_endpoint(qname)?;
        self.prepare_for_browser_endpoint(&endpoint, testnet_host)
    }

    #[cfg(target_arch = "wasm32")]
    pub(crate) async fn prepare_with_pkarr_resolver(
        &self,
        resolver: &BrowserPkarrResolver,
        testnet_host: Option<&str>,
    ) -> locks_sdk::Result<JsPreparedAuthorizedRequest> {
        let qname = self
            .url
            .host_str()
            .and_then(|host| host.strip_prefix("_pubky."))
            .ok_or(locks_sdk::LocksSdkError::InvalidTransportUrl)?;
        let endpoint = resolver.resolve_browser_endpoint(qname).await?;
        self.prepare_for_browser_endpoint(&endpoint, testnet_host)
    }
}

#[wasm_bindgen]
#[derive(Debug, Clone)]
pub struct Session {
    inner: locks_sdk::LocksSession,
    client: locks_sdk::LocksClient,
    creator_pubky: Option<String>,
    #[cfg_attr(not(any(test, target_arch = "wasm32")), allow(dead_code))]
    options: LocksOptions,
}

#[wasm_bindgen]
impl Session {
    #[wasm_bindgen(js_name = exportSecret)]
    pub fn export_secret(&self) -> String {
        self.inner.export_secret().to_owned()
    }

    #[wasm_bindgen(js_name = lockServer)]
    pub fn lock_server(&self) -> String {
        self.client.lock_server().to_string()
    }

    #[wasm_bindgen(js_name = creatorPubky)]
    pub fn creator_pubky(&self) -> Option<String> {
        self.creator_pubky.clone()
    }

    #[wasm_bindgen(getter)]
    pub fn creator(&self) -> Creator {
        Creator::new(self.clone())
    }

    #[cfg(target_arch = "wasm32")]
    pub async fn signout(&self) -> JsResult<()> {
        let resolver = BrowserPkarrResolver::new_with_options(&self.options)
            .map_err(|err| invalid_input(err.to_string()))?;
        let request = self
            .build_signout_request()
            .prepare_with_pkarr_resolver(&resolver, None)
            .await
            .map_err(|err| invalid_input(err.to_string()))?;
        fetch_authorized_empty(&request).await
    }
}

impl Session {
    pub(crate) fn new(
        inner: locks_sdk::LocksSession,
        client: locks_sdk::LocksClient,
        options: LocksOptions,
    ) -> Self {
        Self::new_with_creator(inner, client, options, None)
    }

    pub(crate) fn new_with_creator(
        inner: locks_sdk::LocksSession,
        client: locks_sdk::LocksClient,
        options: LocksOptions,
        creator_pubky: Option<String>,
    ) -> Self {
        Self {
            inner,
            client,
            creator_pubky,
            options,
        }
    }

    #[cfg(test)]
    pub(crate) fn pkarr_relay_urls(&self) -> Vec<String> {
        self.options.pkarr_relay_urls().to_vec()
    }

    #[cfg_attr(not(target_arch = "wasm32"), allow(dead_code))]
    pub(crate) fn inner(&self) -> &locks_sdk::LocksSession {
        &self.inner
    }

    #[cfg_attr(not(target_arch = "wasm32"), allow(dead_code))]
    pub(crate) fn options(&self) -> &LocksOptions {
        &self.options
    }

    #[cfg(any(test, target_arch = "wasm32"))]
    #[cfg_attr(target_arch = "wasm32", allow(dead_code))]
    pub(crate) fn authorization_header_value(&self) -> String {
        format!("Bearer {}", self.inner.export_secret())
    }

    #[cfg(any(test, target_arch = "wasm32"))]
    pub(crate) fn transport_url(&self, path: &str) -> url::Url {
        self.client
            .transport_url(path)
            .expect("binding request path is a valid Lock Server URL")
    }

    #[cfg(any(test, target_arch = "wasm32"))]
    #[cfg_attr(target_arch = "wasm32", allow(dead_code))]
    pub(crate) fn build_signout_request(&self) -> JsAuthorizedRequestPlan {
        JsAuthorizedRequestPlan {
            method: "DELETE",
            path: "/frontend-sessions/current".to_owned(),
            url: self.transport_url("/frontend-sessions/current"),
            authorization: self.authorization_header_value(),
            content_type: None,
            body: JsRequestBody::Empty,
        }
    }
}

#[cfg(target_arch = "wasm32")]
pub(crate) async fn fetch_authorized_empty(request: &JsPreparedAuthorizedRequest) -> JsResult<()> {
    let response = fetch_authorized(request).await?;
    if !response.ok() {
        return Err(invalid_input(format!(
            "Lock Server request failed with HTTP {}",
            response.status()
        )));
    }
    Ok(())
}

#[cfg(target_arch = "wasm32")]
pub(crate) async fn fetch_authorized_json(
    request: &JsPreparedAuthorizedRequest,
) -> JsResult<wasm_bindgen::JsValue> {
    let response = fetch_authorized(request).await?;
    if !response.ok() {
        return Err(invalid_input(format!(
            "Lock Server request failed with HTTP {}",
            response.status()
        )));
    }
    wasm_bindgen_futures::JsFuture::from(
        response
            .json()
            .map_err(|err| invalid_input(format!("failed to read JSON response: {err:?}")))?,
    )
    .await
    .map_err(|err| invalid_input(format!("failed to parse JSON response: {err:?}")))
}

#[cfg(target_arch = "wasm32")]
async fn fetch_authorized(request: &JsPreparedAuthorizedRequest) -> JsResult<web_sys::Response> {
    use wasm_bindgen::JsCast;

    let request_init = web_sys::RequestInit::new();
    request_init.set_method(request.method);
    request_init.set_mode(web_sys::RequestMode::Cors);
    match &request.body {
        JsRequestBody::Json(body) => {
            request_init.set_body(&wasm_bindgen::JsValue::from_str(&body.to_string()));
        }
        JsRequestBody::Bytes(bytes) => {
            request_init.set_body(&js_sys::Uint8Array::from(bytes.as_slice()).into());
        }
        JsRequestBody::Empty => {}
    }

    let web_request = web_sys::Request::new_with_str_and_init(request.url.as_str(), &request_init)
        .map_err(|err| invalid_input(format!("failed to build request: {err:?}")))?;
    web_request
        .headers()
        .set("authorization", &request.authorization)
        .map_err(|err| invalid_input(format!("failed to set authorization header: {err:?}")))?;
    if let Some(pubky_host) = &request.pubky_host {
        web_request
            .headers()
            .set("pubky-host", pubky_host)
            .map_err(|err| invalid_input(format!("failed to set pubky-host header: {err:?}")))?;
    }
    if let Some(content_type) = &request.content_type {
        web_request
            .headers()
            .set("content-type", content_type)
            .map_err(|err| invalid_input(format!("failed to set content-type header: {err:?}")))?;
    }

    let window = web_sys::window().ok_or_else(|| invalid_input("window is unavailable"))?;
    let response_value =
        wasm_bindgen_futures::JsFuture::from(window.fetch_with_request(&web_request))
            .await
            .map_err(|err| invalid_input(format!("Lock Server request failed: {err:?}")))?;
    response_value
        .dyn_into()
        .map_err(|_| invalid_input("fetch returned a non-Response value"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::cell::RefCell;

    #[test]
    fn browser_pkarr_clients_share_singleton_cache() {
        let first = browser_pkarr_cache();
        let second = browser_pkarr_cache();

        assert!(Arc::ptr_eq(&first, &second));
    }

    #[test]
    fn creator_homeserver_qname_is_resolved_from_the_creator_pubky_record() {
        let creator = pkarr8::Keypair::random();
        let homeserver = pkarr8::Keypair::random().public_key().to_z32();
        let packet = pkarr8::SignedPacket::builder()
            .https(
                "_pubky".try_into().expect("_pubky name"),
                pkarr8::dns::rdata::SVCB::new(
                    0,
                    homeserver.as_str().try_into().expect("homeserver qname"),
                ),
                3600,
            )
            .sign(&creator)
            .expect("signed creator packet");

        assert_eq!(
            creator_homeserver_qname_from_packet(&packet).unwrap(),
            homeserver
        );
    }

    struct FixedBrowserEndpointResolver {
        endpoint: locks_sdk::transport::BrowserEndpoint,
        last_qname: RefCell<Option<String>>,
    }

    impl FixedBrowserEndpointResolver {
        fn new(endpoint: locks_sdk::transport::BrowserEndpoint) -> Self {
            Self {
                endpoint,
                last_qname: RefCell::new(None),
            }
        }

        fn last_qname(&self) -> Option<String> {
            self.last_qname.borrow().clone()
        }
    }

    impl BrowserEndpointResolver for FixedBrowserEndpointResolver {
        fn resolve_browser_endpoint(
            &self,
            qname: &str,
        ) -> locks_sdk::Result<locks_sdk::transport::BrowserEndpoint> {
            self.last_qname.replace(Some(qname.to_owned()));
            Ok(self.endpoint.clone())
        }
    }

    fn test_client() -> locks_sdk::LocksClient {
        use std::str::FromStr;

        let lock_server = locks_core::ids::LockServerPubky::from_str(
            "pubky7ir1ttte48bcp4zjychjyscicrwi1j34mtt91ptsafdbjmr8g9eo",
        )
        .unwrap();
        locks_sdk::LocksClient::for_server(lock_server)
    }

    #[test]
    fn session_export_restore_roundtrip_keeps_secret_and_creator_accessor() {
        let client = test_client();
        let session = Session::new(
            client.restore_session("frontend-session-secret"),
            client.clone(),
            LocksOptions::new(),
        );

        let exported = session.export_secret();
        let restored = Session::new(
            client.restore_session(exported.clone()),
            client,
            LocksOptions::new(),
        );

        assert_eq!(restored.export_secret(), "frontend-session-secret");
        assert_eq!(
            restored.lock_server(),
            "pubky7ir1ttte48bcp4zjychjyscicrwi1j34mtt91ptsafdbjmr8g9eo"
        );
        assert_eq!(
            restored.creator().export_session_secret_for_tests(),
            "frontend-session-secret"
        );
    }

    #[test]
    fn exchanged_session_exposes_authenticated_creator_pubky() {
        let client = test_client();
        let creator = "pubkytkrq8zmwb8a3m9k15csu3q17qmfgqnp9dskbrg9uq1rydpyxp7qy";
        let session = Session::new_with_creator(
            client.restore_session("frontend-session-secret"),
            client,
            LocksOptions::new(),
            Some(creator.to_owned()),
        );

        assert_eq!(session.creator_pubky(), Some(creator.to_owned()));
    }

    #[test]
    fn signout_request_uses_current_frontend_session_endpoint_and_bearer() {
        let client = test_client();
        let session = Session::new(
            client.restore_session("frontend-session-secret"),
            client,
            LocksOptions::new(),
        );

        let request = session.build_signout_request();

        assert_eq!(request.method, "DELETE");
        assert_eq!(request.path, "/frontend-sessions/current");
        assert_eq!(request.authorization, "Bearer frontend-session-secret");
    }

    #[test]
    fn signout_request_includes_direct_lock_server_url() {
        let client = test_client();
        let session = Session::new(
            client.restore_session("frontend-session-secret"),
            client,
            LocksOptions::new(),
        );

        let request = session.build_signout_request();

        assert_eq!(
            request.url.as_str(),
            "https://_pubky.7ir1ttte48bcp4zjychjyscicrwi1j34mtt91ptsafdbjmr8g9eo/frontend-sessions/current"
        );
    }

    #[test]
    fn signout_request_prepares_browser_fetch_url_and_pubky_host_from_endpoint() {
        let client = test_client();
        let session = Session::new(
            client.restore_session("frontend-session-secret"),
            client,
            LocksOptions::new(),
        );
        let request = session.build_signout_request();
        let endpoint = locks_sdk::transport::BrowserEndpoint {
            domain: Some("locks.example".to_owned()),
            port: Some(8443),
            params: std::collections::BTreeMap::new(),
        };

        let prepared = request
            .prepare_for_browser_endpoint(&endpoint, None)
            .unwrap();

        assert_eq!(prepared.method, "DELETE");
        assert_eq!(prepared.path, "/frontend-sessions/current");
        assert_eq!(
            prepared.url.as_str(),
            "https://locks.example:8443/frontend-sessions/current"
        );
        assert_eq!(
            prepared.pubky_host.as_deref(),
            Some("7ir1ttte48bcp4zjychjyscicrwi1j34mtt91ptsafdbjmr8g9eo")
        );
        assert_eq!(prepared.authorization, "Bearer frontend-session-secret");
    }

    #[test]
    fn signout_request_prepares_with_resolved_browser_endpoint() {
        let client = test_client();
        let session = Session::new(
            client.restore_session("frontend-session-secret"),
            client,
            LocksOptions::new(),
        );
        let request = session.build_signout_request();
        let resolver = FixedBrowserEndpointResolver::new(locks_sdk::transport::BrowserEndpoint {
            domain: Some("locks.example".to_owned()),
            port: Some(9443),
            params: std::collections::BTreeMap::new(),
        });

        let prepared = request.prepare_with_resolver(&resolver, None).unwrap();

        assert_eq!(
            resolver.last_qname().as_deref(),
            Some("7ir1ttte48bcp4zjychjyscicrwi1j34mtt91ptsafdbjmr8g9eo")
        );
        assert_eq!(
            prepared.url.as_str(),
            "https://locks.example:9443/frontend-sessions/current"
        );
        assert_eq!(
            prepared.pubky_host.as_deref(),
            Some("7ir1ttte48bcp4zjychjyscicrwi1j34mtt91ptsafdbjmr8g9eo")
        );
    }
}
