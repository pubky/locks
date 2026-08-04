use std::str::FromStr;

use locks_core::ids::LockServerPubky;
#[cfg(any(test, target_arch = "wasm32"))]
use locks_core::ids::{CreatorPubky, PubkyLockResource};
use serde_json::Value;
#[cfg(target_arch = "wasm32")]
use wasm_bindgen::JsCast;
use wasm_bindgen::prelude::*;

use crate::js_error::{JsResult, invalid_input};
#[cfg(target_arch = "wasm32")]
use crate::json::serializable_to_plain_js_value;
#[cfg(target_arch = "wasm32")]
use crate::session::BrowserPkarrResolver;
use crate::session::Session;
use crate::viewer::Viewer;
use url::Url;

#[wasm_bindgen]
#[derive(Debug, Clone, Default)]
pub struct LocksOptions {
    pkarr_relays: Vec<String>,
}

#[wasm_bindgen]
impl LocksOptions {
    #[wasm_bindgen(constructor)]
    pub fn new() -> Self {
        Self::default()
    }

    #[wasm_bindgen(js_name = addPkarrRelay)]
    pub fn add_pkarr_relay(&mut self, relay_url: String) -> JsResult<LocksOptions> {
        self.try_add_pkarr_relay(relay_url).map_err(invalid_input)?;
        Ok(self.clone())
    }

    #[wasm_bindgen(getter, js_name = pkarrRelays)]
    pub fn pkarr_relays(&self) -> Vec<String> {
        self.pkarr_relays.clone()
    }
}

impl LocksOptions {
    fn try_add_pkarr_relay(&mut self, relay_url: String) -> Result<(), String> {
        let relay =
            Url::parse(&relay_url).map_err(|err| format!("invalid PKARR relay URL: {err}"))?;
        match relay.scheme() {
            "http" | "https" => {}
            scheme => return Err(format!("invalid PKARR relay URL scheme: {scheme}")),
        }
        self.pkarr_relays.push(relay.to_string());
        Ok(())
    }

    #[cfg_attr(not(target_arch = "wasm32"), allow(dead_code))]
    pub(crate) fn pkarr_relay_urls(&self) -> &[String] {
        &self.pkarr_relays
    }
}

#[wasm_bindgen]
#[derive(Debug, Clone)]
pub struct Locks {
    inner: locks_sdk::LocksClient,
    options: LocksOptions,
}

#[wasm_bindgen]
pub struct ConnectUrlOptions {
    return_to: String,
    state: String,
}

#[wasm_bindgen]
impl ConnectUrlOptions {
    #[wasm_bindgen(constructor)]
    pub fn new(return_to: String, state: String) -> Self {
        Self { return_to, state }
    }

    #[wasm_bindgen(getter, js_name = returnTo)]
    pub fn return_to(&self) -> String {
        self.return_to.clone()
    }

    #[wasm_bindgen(getter)]
    pub fn state(&self) -> String {
        self.state.clone()
    }
}

#[wasm_bindgen]
pub struct ConnectCallback {
    code: String,
    state: String,
}

#[wasm_bindgen]
impl ConnectCallback {
    #[wasm_bindgen(getter)]
    pub fn code(&self) -> String {
        self.code.clone()
    }

    #[wasm_bindgen(getter)]
    pub fn state(&self) -> String {
        self.state.clone()
    }
}

#[wasm_bindgen]
pub struct ExchangeFrontendSessionCodeOptions {
    code: String,
    state: String,
}

#[wasm_bindgen]
impl ExchangeFrontendSessionCodeOptions {
    #[wasm_bindgen(constructor)]
    pub fn new(code: String, state: String) -> Self {
        Self { code, state }
    }

    #[wasm_bindgen(getter)]
    pub fn code(&self) -> String {
        self.code.clone()
    }

    #[wasm_bindgen(getter)]
    pub fn state(&self) -> String {
        self.state.clone()
    }
}

#[cfg(any(test, target_arch = "wasm32"))]
#[derive(Debug, Clone, PartialEq)]
struct JsRequestPlan {
    method: &'static str,
    path: &'static str,
    body: Value,
}

#[cfg(any(test, target_arch = "wasm32"))]
#[derive(Debug, Clone, PartialEq)]
struct JsPreparedRequest {
    method: &'static str,
    path: &'static str,
    url: Url,
    pubky_host: Option<String>,
    body: Value,
}

#[cfg(any(test, target_arch = "wasm32"))]
#[derive(Debug, Clone, PartialEq)]
struct JsCreatorPointerRequest {
    method: &'static str,
    path: &'static str,
    url: Url,
}

#[cfg(any(test, target_arch = "wasm32"))]
#[derive(Debug, Clone, PartialEq)]
struct JsPreparedCreatorPointerRequest {
    method: &'static str,
    path: &'static str,
    url: Url,
    pubky_host: Option<String>,
}

#[cfg(any(test, target_arch = "wasm32"))]
#[derive(Debug, Clone, PartialEq)]
struct JsContentLockRequest {
    method: &'static str,
    path: String,
    url: Url,
}

#[cfg(any(test, target_arch = "wasm32"))]
#[derive(Debug, Clone, PartialEq)]
struct JsPreparedContentLockRequest {
    method: &'static str,
    path: String,
    url: Url,
    pubky_host: Option<String>,
}

#[wasm_bindgen]
impl Locks {
    #[wasm_bindgen(js_name = forServer)]
    pub fn for_server(lock_server: &str) -> JsResult<Locks> {
        Self::for_server_with_options(lock_server, &LocksOptions::new())
    }

    #[wasm_bindgen(js_name = forServerWithOptions)]
    pub fn for_server_with_options(lock_server: &str, options: &LocksOptions) -> JsResult<Locks> {
        let lock_server = LockServerPubky::from_str(lock_server)
            .map_err(|err| invalid_input(format!("invalid lock server pubky: {err}")))?;
        Ok(Self {
            inner: locks_sdk::LocksClient::for_server(lock_server),
            options: options.clone(),
        })
    }

    #[wasm_bindgen(js_name = fromCreatorLockServicePointer)]
    pub fn from_creator_lock_service_pointer(pointer: JsValue) -> JsResult<Locks> {
        let value: Value = serde_wasm_bindgen::from_value(pointer).map_err(|err| {
            invalid_input(format!("invalid creator lock service pointer JSON: {err}"))
        })?;
        Self::from_creator_lock_service_pointer_value(value, None).map_err(invalid_input)
    }

    #[cfg(target_arch = "wasm32")]
    #[wasm_bindgen(js_name = forCreator)]
    pub async fn for_creator(creator: &str) -> JsResult<Locks> {
        Self::for_creator_with_options(creator, &LocksOptions::new()).await
    }

    #[cfg(target_arch = "wasm32")]
    #[wasm_bindgen(js_name = forCreatorWithOptions)]
    pub async fn for_creator_with_options(
        creator: &str,
        options: &LocksOptions,
    ) -> JsResult<Locks> {
        let options = options.clone();
        let creator = CreatorPubky::from_str(creator)
            .map_err(|err| invalid_input(format!("invalid creator pubky: {err}")))?;
        let request = build_creator_pointer_request(&creator)
            .map_err(|err| invalid_input(err.to_string()))?;
        let resolver = BrowserPkarrResolver::new_with_options(&options)
            .map_err(|err| invalid_input(err.to_string()))?;
        let request =
            prepare_creator_pointer_request_with_pkarr_resolver(&request, &resolver, None)
                .await
                .map_err(|err| invalid_input(err.to_string()))?;
        let value = fetch_creator_pointer_json(&request).await?;
        Self::from_creator_lock_service_pointer_value(value, Some(options)).map_err(invalid_input)
    }

    #[cfg(target_arch = "wasm32")]
    #[wasm_bindgen(js_name = readContentLock)]
    pub async fn read_content_lock(resource: &str) -> JsResult<JsValue> {
        Self::read_content_lock_with_options(resource, &LocksOptions::new()).await
    }

    #[cfg(target_arch = "wasm32")]
    #[wasm_bindgen(js_name = readContentLockWithOptions)]
    pub async fn read_content_lock_with_options(
        resource: &str,
        options: &LocksOptions,
    ) -> JsResult<JsValue> {
        let resource = PubkyLockResource::from_str(resource)
            .map_err(|err| invalid_input(format!("invalid content lock resource: {err}")))?;
        let content_lock = fetch_validated_content_lock(&resource, options).await?;
        serializable_to_plain_js_value(&content_lock)
            .map_err(|err| invalid_input(format!("failed to serialize content lock: {err:?}")))
    }

    #[cfg(target_arch = "wasm32")]
    #[wasm_bindgen(js_name = forContentLock)]
    pub async fn for_content_lock(resource: &str) -> JsResult<Locks> {
        Self::for_content_lock_with_options(resource, &LocksOptions::new()).await
    }

    #[cfg(target_arch = "wasm32")]
    #[wasm_bindgen(js_name = forContentLockWithOptions)]
    pub async fn for_content_lock_with_options(
        resource: &str,
        options: &LocksOptions,
    ) -> JsResult<Locks> {
        let options = options.clone();
        let resource = PubkyLockResource::from_str(resource)
            .map_err(|err| invalid_input(format!("invalid content lock resource: {err}")))?;
        let content_lock = fetch_validated_content_lock(&resource, &options).await?;
        match locks_sdk::lock_server_for_content_lock(&content_lock, None) {
            Ok(lock_server) => {
                return Ok(Self {
                    inner: locks_sdk::LocksClient::for_server(lock_server),
                    options,
                });
            }
            Err(locks_sdk::LocksSdkError::MissingCreatorLockServicePointer) => {}
            Err(err) => return Err(invalid_input(err.to_string())),
        }
        let creator = resource.creator().to_string();
        Self::for_creator_with_options(&creator, &options).await
    }

    #[wasm_bindgen(js_name = lockServer)]
    pub fn lock_server(&self) -> String {
        self.inner.lock_server().to_string()
    }

    #[wasm_bindgen(js_name = restoreSession)]
    pub fn restore_session(&self, secret: &str) -> Session {
        Session::new(
            self.inner.restore_session(secret),
            self.inner.clone(),
            self.options.clone(),
        )
    }

    #[wasm_bindgen(getter)]
    pub fn viewer(&self) -> Viewer {
        Viewer::new(self.inner.clone(), self.options.clone())
    }

    #[wasm_bindgen(js_name = createConnectUrl)]
    #[cfg(not(target_arch = "wasm32"))]
    pub fn create_connect_url(&self, options: &ConnectUrlOptions) -> JsResult<String> {
        self.build_connect_url(&options.return_to, &options.state)
            .map(|url| url.to_string())
            .map_err(invalid_input)
    }

    #[wasm_bindgen(js_name = createConnectUrl)]
    #[cfg(target_arch = "wasm32")]
    pub async fn create_connect_url(&self, options: &ConnectUrlOptions) -> JsResult<String> {
        let resolver = BrowserPkarrResolver::new_with_options(&self.options)
            .map_err(|err| invalid_input(err.to_string()))?;
        self.build_resolved_connect_url(&options.return_to, &options.state, &resolver, None)
            .await
            .map(|url| url.to_string())
            .map_err(|err| invalid_input(err.to_string()))
    }

    #[wasm_bindgen(js_name = parseConnectCallback)]
    pub fn parse_connect_callback(callback_url: &str) -> JsResult<ConnectCallback> {
        parse_connect_callback_url(callback_url).map_err(invalid_input)
    }

    #[cfg(target_arch = "wasm32")]
    #[wasm_bindgen(js_name = exchangeFrontendSessionCode)]
    pub async fn exchange_frontend_session_code(
        &self,
        options: &ExchangeFrontendSessionCodeOptions,
    ) -> JsResult<Session> {
        let request = exchange_frontend_session_code_request(options);
        let resolver = BrowserPkarrResolver::new_with_options(&self.options)
            .map_err(|err| invalid_input(err.to_string()))?;
        let request = self
            .prepare_exchange_request_with_pkarr_resolver(&request, &resolver, None)
            .await
            .map_err(|err| invalid_input(err.to_string()))?;
        let response = post_json_for_session(&request).await?;
        Ok(Session::new_with_creator(
            self.inner.restore_session(&response.session_token),
            self.inner.clone(),
            self.options.clone(),
            Some(response.creator.to_string()),
        ))
    }
}

impl Locks {
    fn from_creator_lock_service_pointer_value(
        value: Value,
        options: Option<LocksOptions>,
    ) -> Result<Locks, String> {
        let pointer = locks_sdk::CreatorLockServicePointer::validate_value(value)
            .map_err(|err| format!("invalid creator lock service pointer: {err}"))?;
        Ok(Self {
            inner: locks_sdk::LocksClient::for_creator_pointer(pointer),
            options: options.unwrap_or_default(),
        })
    }

    #[cfg(test)]
    pub(crate) fn pkarr_relay_urls(&self) -> Vec<String> {
        self.options.pkarr_relay_urls().to_vec()
    }

    fn build_connect_url(&self, return_to: &str, state: &str) -> Result<Url, String> {
        if return_to.is_empty() {
            return Err("returnTo must not be empty".to_owned());
        }
        if state.is_empty() {
            return Err("state must not be empty".to_owned());
        }

        let mut url = self
            .inner
            .transport_url("/connect")
            .map_err(|err| err.to_string())?;
        url.query_pairs_mut()
            .append_pair("return_to", return_to)
            .append_pair("state", state);
        Ok(url)
    }

    #[cfg(any(test, target_arch = "wasm32"))]
    fn build_connect_url_for_endpoint(
        &self,
        return_to: &str,
        state: &str,
        endpoint: &locks_sdk::transport::BrowserEndpoint,
        testnet_host: Option<&str>,
    ) -> locks_sdk::Result<Url> {
        let url = self
            .build_connect_url(return_to, state)
            .map_err(|_| locks_sdk::LocksSdkError::InvalidTransportUrl)?;
        let browser_request =
            locks_sdk::transport::rewrite_browser_request(url.as_str(), endpoint, testnet_host)?;
        Ok(browser_request.url)
    }

    #[cfg(target_arch = "wasm32")]
    async fn build_resolved_connect_url(
        &self,
        return_to: &str,
        state: &str,
        resolver: &BrowserPkarrResolver,
        testnet_host: Option<&str>,
    ) -> locks_sdk::Result<Url> {
        let url = self
            .build_connect_url(return_to, state)
            .map_err(|_| locks_sdk::LocksSdkError::InvalidTransportUrl)?;
        let qname = url
            .host_str()
            .and_then(|host| host.strip_prefix("_pubky."))
            .ok_or(locks_sdk::LocksSdkError::InvalidTransportUrl)?;
        let endpoint = resolver.resolve_browser_endpoint(qname).await?;
        self.build_connect_url_for_endpoint(return_to, state, &endpoint, testnet_host)
    }

    #[cfg(any(test, target_arch = "wasm32"))]
    fn prepare_exchange_request_for_endpoint(
        &self,
        request: &JsRequestPlan,
        endpoint: &locks_sdk::transport::BrowserEndpoint,
        testnet_host: Option<&str>,
    ) -> locks_sdk::Result<JsPreparedRequest> {
        let url = self.inner.transport_url(request.path)?;
        let browser_request =
            locks_sdk::transport::rewrite_browser_request(url.as_str(), endpoint, testnet_host)?;
        Ok(JsPreparedRequest {
            method: request.method,
            path: request.path,
            url: browser_request.url,
            pubky_host: browser_request.pubky_host,
            body: request.body.clone(),
        })
    }

    #[cfg(target_arch = "wasm32")]
    async fn prepare_exchange_request_with_pkarr_resolver(
        &self,
        request: &JsRequestPlan,
        resolver: &BrowserPkarrResolver,
        testnet_host: Option<&str>,
    ) -> locks_sdk::Result<JsPreparedRequest> {
        let url = self.inner.transport_url(request.path)?;
        let qname = url
            .host_str()
            .and_then(|host| host.strip_prefix("_pubky."))
            .ok_or(locks_sdk::LocksSdkError::InvalidTransportUrl)?;
        let endpoint = resolver.resolve_browser_endpoint(qname).await?;
        self.prepare_exchange_request_for_endpoint(request, &endpoint, testnet_host)
    }
}

fn parse_connect_callback_url(callback_url: &str) -> Result<ConnectCallback, String> {
    let url = Url::parse(callback_url).map_err(|err| format!("invalid callback URL: {err}"))?;
    let mut code = None;
    let mut state = None;
    for (key, value) in url.query_pairs() {
        match key.as_ref() {
            "code" => code = Some(value.into_owned()),
            "state" => state = Some(value.into_owned()),
            _ => {}
        }
    }
    let code = code
        .filter(|value| !value.is_empty())
        .ok_or("missing callback code")?;
    let state = state
        .filter(|value| !value.is_empty())
        .ok_or("missing callback state")?;
    Ok(ConnectCallback { code, state })
}

#[cfg(any(test, target_arch = "wasm32"))]
fn build_creator_pointer_request(
    creator: &CreatorPubky,
) -> locks_sdk::Result<JsCreatorPointerRequest> {
    Ok(JsCreatorPointerRequest {
        method: "GET",
        path: locks_core::lock_service_pointer::LOCK_SERVICE_POINTER_PATH,
        url: locks_sdk::creator_lock_service_pointer_url(creator)?,
    })
}

#[cfg(any(test, target_arch = "wasm32"))]
fn prepare_creator_pointer_request_for_endpoint(
    request: &JsCreatorPointerRequest,
    endpoint: &locks_sdk::transport::BrowserEndpoint,
    testnet_host: Option<&str>,
) -> locks_sdk::Result<JsPreparedCreatorPointerRequest> {
    let browser_request = locks_sdk::transport::rewrite_browser_request(
        request.url.as_str(),
        endpoint,
        testnet_host,
    )?;
    Ok(JsPreparedCreatorPointerRequest {
        method: request.method,
        path: request.path,
        url: browser_request.url,
        pubky_host: browser_request.pubky_host,
    })
}

#[cfg(any(test, target_arch = "wasm32"))]
fn build_content_lock_request(
    resource: &PubkyLockResource,
) -> locks_sdk::Result<JsContentLockRequest> {
    Ok(JsContentLockRequest {
        method: "GET",
        path: resource.content_lock_path().to_string(),
        url: locks_sdk::content_lock_resource_url(resource)?,
    })
}

#[cfg(any(test, target_arch = "wasm32"))]
fn prepare_content_lock_request_for_endpoint(
    request: &JsContentLockRequest,
    endpoint: &locks_sdk::transport::BrowserEndpoint,
    testnet_host: Option<&str>,
) -> locks_sdk::Result<JsPreparedContentLockRequest> {
    let browser_request = locks_sdk::transport::rewrite_browser_request(
        request.url.as_str(),
        endpoint,
        testnet_host,
    )?;
    Ok(JsPreparedContentLockRequest {
        method: request.method,
        path: request.path.clone(),
        url: browser_request.url,
        pubky_host: browser_request.pubky_host,
    })
}

#[cfg(target_arch = "wasm32")]
async fn prepare_creator_pointer_request_with_pkarr_resolver(
    request: &JsCreatorPointerRequest,
    resolver: &BrowserPkarrResolver,
    testnet_host: Option<&str>,
) -> locks_sdk::Result<JsPreparedCreatorPointerRequest> {
    let qname = request
        .url
        .host_str()
        .and_then(|host| host.strip_prefix("_pubky."))
        .ok_or(locks_sdk::LocksSdkError::InvalidTransportUrl)?;
    let endpoint = resolver
        .resolve_browser_endpoint_for_creator_qname(qname)
        .await?;
    prepare_creator_pointer_request_for_endpoint(request, &endpoint, testnet_host)
}

#[cfg(target_arch = "wasm32")]
async fn prepare_content_lock_request_with_pkarr_resolver(
    request: &JsContentLockRequest,
    resolver: &BrowserPkarrResolver,
    testnet_host: Option<&str>,
) -> locks_sdk::Result<JsPreparedContentLockRequest> {
    let qname = request
        .url
        .host_str()
        .and_then(|host| host.strip_prefix("_pubky."))
        .ok_or(locks_sdk::LocksSdkError::InvalidTransportUrl)?;
    let endpoint = resolver
        .resolve_browser_endpoint_for_creator_qname(qname)
        .await?;
    prepare_content_lock_request_for_endpoint(request, &endpoint, testnet_host)
}

#[cfg(any(test, target_arch = "wasm32"))]
fn exchange_frontend_session_code_request(
    options: &ExchangeFrontendSessionCodeOptions,
) -> JsRequestPlan {
    JsRequestPlan {
        method: "POST",
        path: "/frontend-sessions",
        body: serde_json::json!({
            "code": options.code,
            "state": options.state,
        }),
    }
}

#[cfg(target_arch = "wasm32")]
async fn fetch_creator_pointer_json(
    request_plan: &JsPreparedCreatorPointerRequest,
) -> JsResult<Value> {
    let request_init = web_sys::RequestInit::new();
    request_init.set_method(request_plan.method);
    request_init.set_mode(web_sys::RequestMode::Cors);

    let request = web_sys::Request::new_with_str_and_init(request_plan.url.as_str(), &request_init)
        .map_err(|err| invalid_input(format!("failed to build request: {err:?}")))?;
    if let Some(pubky_host) = &request_plan.pubky_host {
        request
            .headers()
            .set("pubky-host", pubky_host)
            .map_err(|err| invalid_input(format!("failed to set pubky-host header: {err:?}")))?;
    }

    let window = web_sys::window().ok_or_else(|| invalid_input("window is unavailable"))?;
    let response_value = wasm_bindgen_futures::JsFuture::from(window.fetch_with_request(&request))
        .await
        .map_err(|err| invalid_input(format!("creator pointer fetch failed: {err:?}")))?;
    let response: web_sys::Response = response_value
        .dyn_into()
        .map_err(|_| invalid_input("fetch returned a non-Response value"))?;
    if !response.ok() {
        return Err(invalid_input(format!(
            "creator pointer fetch failed with HTTP {}",
            response.status()
        )));
    }
    let json =
        wasm_bindgen_futures::JsFuture::from(response.json().map_err(|err| {
            invalid_input(format!("failed to read creator pointer JSON: {err:?}"))
        })?)
        .await
        .map_err(|err| invalid_input(format!("failed to parse creator pointer JSON: {err:?}")))?;
    serde_wasm_bindgen::from_value(json)
        .map_err(|err| invalid_input(format!("invalid creator pointer JSON: {err}")))
}

#[cfg(target_arch = "wasm32")]
async fn fetch_validated_content_lock(
    resource: &PubkyLockResource,
    options: &LocksOptions,
) -> JsResult<locks_core::lock_policy::ContentLock> {
    let request =
        build_content_lock_request(resource).map_err(|err| invalid_input(err.to_string()))?;
    let resolver = BrowserPkarrResolver::new_with_options(options)
        .map_err(|err| invalid_input(err.to_string()))?;
    let request = prepare_content_lock_request_with_pkarr_resolver(&request, &resolver, None)
        .await
        .map_err(|err| invalid_input(err.to_string()))?;
    let value = fetch_content_lock_json(&request).await?;
    locks_sdk::validate_content_lock_value(value, resource)
        .map_err(|err| invalid_input(err.to_string()))
}

#[cfg(target_arch = "wasm32")]
async fn fetch_content_lock_json(request_plan: &JsPreparedContentLockRequest) -> JsResult<Value> {
    let request_init = web_sys::RequestInit::new();
    request_init.set_method(request_plan.method);
    request_init.set_mode(web_sys::RequestMode::Cors);

    let request = web_sys::Request::new_with_str_and_init(request_plan.url.as_str(), &request_init)
        .map_err(|err| invalid_input(format!("failed to build request: {err:?}")))?;
    if let Some(pubky_host) = &request_plan.pubky_host {
        request
            .headers()
            .set("pubky-host", pubky_host)
            .map_err(|err| invalid_input(format!("failed to set pubky-host header: {err:?}")))?;
    }

    let window = web_sys::window().ok_or_else(|| invalid_input("window is unavailable"))?;
    let response_value = wasm_bindgen_futures::JsFuture::from(window.fetch_with_request(&request))
        .await
        .map_err(|err| invalid_input(format!("content lock fetch failed: {err:?}")))?;
    let response: web_sys::Response = response_value
        .dyn_into()
        .map_err(|_| invalid_input("fetch returned a non-Response value"))?;
    if !response.ok() {
        return Err(invalid_input(format!(
            "content lock fetch failed with HTTP {}",
            response.status()
        )));
    }
    let json = wasm_bindgen_futures::JsFuture::from(
        response
            .json()
            .map_err(|err| invalid_input(format!("failed to read content lock JSON: {err:?}")))?,
    )
    .await
    .map_err(|err| invalid_input(format!("failed to parse content lock JSON: {err:?}")))?;
    serde_wasm_bindgen::from_value(json)
        .map_err(|err| invalid_input(format!("invalid content lock JSON: {err}")))
}

#[cfg(target_arch = "wasm32")]
async fn post_json_for_session(
    request_plan: &JsPreparedRequest,
) -> JsResult<FrontendSessionResponse> {
    let request_init = web_sys::RequestInit::new();
    request_init.set_method(request_plan.method);
    request_init.set_mode(web_sys::RequestMode::Cors);
    request_init.set_body(&JsValue::from_str(&request_plan.body.to_string()));

    let request = web_sys::Request::new_with_str_and_init(request_plan.url.as_str(), &request_init)
        .map_err(|err| invalid_input(format!("failed to build request: {err:?}")))?;
    request
        .headers()
        .set("content-type", "application/json")
        .map_err(|err| invalid_input(format!("failed to set content-type header: {err:?}")))?;
    if let Some(pubky_host) = &request_plan.pubky_host {
        request
            .headers()
            .set("pubky-host", pubky_host)
            .map_err(|err| invalid_input(format!("failed to set pubky-host header: {err:?}")))?;
    }

    let window = web_sys::window().ok_or_else(|| invalid_input("window is unavailable"))?;
    let response_value = wasm_bindgen_futures::JsFuture::from(window.fetch_with_request(&request))
        .await
        .map_err(|err| invalid_input(format!("frontend session exchange failed: {err:?}")))?;
    let response: web_sys::Response = response_value
        .dyn_into()
        .map_err(|_| invalid_input("fetch returned a non-Response value"))?;
    if !response.ok() {
        return Err(invalid_input(format!(
            "frontend session exchange failed with HTTP {}",
            response.status()
        )));
    }
    let json = wasm_bindgen_futures::JsFuture::from(
        response
            .json()
            .map_err(|err| invalid_input(format!("failed to read JSON response: {err:?}")))?,
    )
    .await
    .map_err(|err| invalid_input(format!("failed to parse JSON response: {err:?}")))?;
    let value: Value = serde_wasm_bindgen::from_value(json)
        .map_err(|err| invalid_input(format!("invalid session response JSON: {err}")))?;
    parse_frontend_session_response(value).map_err(invalid_input)
}

#[cfg(any(test, target_arch = "wasm32"))]
#[derive(Debug, Clone, PartialEq, Eq)]
struct FrontendSessionResponse {
    session_token: String,
    creator: CreatorPubky,
}

#[cfg(any(test, target_arch = "wasm32"))]
fn parse_frontend_session_response(value: Value) -> Result<FrontendSessionResponse, String> {
    let session_token = value
        .get("session_token")
        .and_then(Value::as_str)
        .map(ToOwned::to_owned)
        .ok_or_else(|| "frontend session response missing session_token".to_owned())?;
    let creator = value
        .get("creator")
        .and_then(Value::as_str)
        .ok_or_else(|| "frontend session response missing creator".to_owned())
        .and_then(|creator| {
            CreatorPubky::from_str(creator)
                .map_err(|_| "frontend session response contains invalid creator".to_owned())
        })?;
    Ok(FrontendSessionResponse {
        session_token,
        creator,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn frontend_session_response_preserves_authenticated_creator() {
        let response = parse_frontend_session_response(serde_json::json!({
            "session_token": "frontend-session-secret",
            "creator": "pubkytkrq8zmwb8a3m9k15csu3q17qmfgqnp9dskbrg9uq1rydpyxp7qy",
            "expires_at": "2030-01-01T00:00:00Z"
        }))
        .unwrap();

        assert_eq!(response.session_token, "frontend-session-secret");
        assert_eq!(
            response.creator.to_string(),
            "pubkytkrq8zmwb8a3m9k15csu3q17qmfgqnp9dskbrg9uq1rydpyxp7qy"
        );
    }

    #[test]
    fn frontend_session_response_rejects_missing_creator() {
        let error = parse_frontend_session_response(serde_json::json!({
            "session_token": "frontend-session-secret",
            "expires_at": "2030-01-01T00:00:00Z"
        }))
        .unwrap_err();

        assert!(error.contains("missing creator"));
    }

    #[test]
    fn locks_constructor_is_available_for_valid_lock_server_pubky() {
        let locks =
            Locks::for_server("pubky7ir1ttte48bcp4zjychjyscicrwi1j34mtt91ptsafdbjmr8g9eo").unwrap();

        assert_eq!(
            locks.lock_server(),
            "pubky7ir1ttte48bcp4zjychjyscicrwi1j34mtt91ptsafdbjmr8g9eo"
        );
    }

    #[test]
    fn locks_options_collects_and_validates_pkarr_relays() {
        let mut options = LocksOptions::new();

        options
            .add_pkarr_relay("http://localhost:15411".to_owned())
            .unwrap();

        assert_eq!(
            options.pkarr_relays(),
            vec!["http://localhost:15411/".to_owned()]
        );
        assert!(options.try_add_pkarr_relay("not a url".to_owned()).is_err());
    }

    #[test]
    fn locks_for_server_retains_pkarr_relay_options_for_restored_sessions() {
        let mut options = LocksOptions::new();
        options
            .add_pkarr_relay("http://localhost:15411".to_owned())
            .unwrap();

        let locks = Locks::for_server_with_options(
            "pubky7ir1ttte48bcp4zjychjyscicrwi1j34mtt91ptsafdbjmr8g9eo",
            &options,
        )
        .unwrap();
        let session = locks.restore_session("frontend-session-secret");

        assert_eq!(
            locks.pkarr_relay_urls(),
            vec!["http://localhost:15411/".to_owned()]
        );
        assert_eq!(
            session.pkarr_relay_urls(),
            vec!["http://localhost:15411/".to_owned()]
        );
    }

    #[test]
    fn locks_for_server_defaults_to_pkarr_crate_relays_when_options_omitted() {
        let locks =
            Locks::for_server("pubky7ir1ttte48bcp4zjychjyscicrwi1j34mtt91ptsafdbjmr8g9eo").unwrap();

        assert!(locks.pkarr_relay_urls().is_empty());
    }

    #[test]
    fn locks_can_be_constructed_from_creator_lock_service_pointer_json() {
        let locks = Locks::from_creator_lock_service_pointer_value(
            serde_json::json!({
                "version": 1,
                "default_lock_server": "pubky7ir1ttte48bcp4zjychjyscicrwi1j34mtt91ptsafdbjmr8g9eo",
                "created_at": "2026-06-03T00:00:00Z"
            }),
            None,
        )
        .unwrap();

        assert_eq!(
            locks.lock_server(),
            "pubky7ir1ttte48bcp4zjychjyscicrwi1j34mtt91ptsafdbjmr8g9eo"
        );
    }

    #[test]
    fn locks_reject_invalid_creator_lock_service_pointer_json() {
        let unsupported_version = serde_json::json!({
            "version": 2,
            "default_lock_server": "pubky7ir1ttte48bcp4zjychjyscicrwi1j34mtt91ptsafdbjmr8g9eo",
            "created_at": "2026-06-03T00:00:00Z"
        });
        assert!(Locks::from_creator_lock_service_pointer_value(unsupported_version, None).is_err());

        let unknown_field = serde_json::json!({
            "version": 1,
            "default_lock_server": "pubky7ir1ttte48bcp4zjychjyscicrwi1j34mtt91ptsafdbjmr8g9eo",
            "created_at": "2026-06-03T00:00:00Z",
            "base_url": "https://locks.example"
        });
        assert!(Locks::from_creator_lock_service_pointer_value(unknown_field, None).is_err());

        let url_lock_server = serde_json::json!({
            "version": 1,
            "default_lock_server": "https://locks.example",
            "created_at": "2026-06-03T00:00:00Z"
        });
        assert!(Locks::from_creator_lock_service_pointer_value(url_lock_server, None).is_err());
    }

    #[test]
    fn creator_pointer_request_uses_public_config_path_without_auth() {
        let creator =
            CreatorPubky::from_str("pubkytkrq8zmwb8a3m9k15csu3q17qmfgqnp9dskbrg9uq1rydpyxp7qy")
                .unwrap();

        let request = build_creator_pointer_request(&creator).unwrap();

        assert_eq!(request.method, "GET");
        assert_eq!(request.path, "/pub/locks.app/config.json");
        assert_eq!(
            request.url.as_str(),
            "https://_pubky.tkrq8zmwb8a3m9k15csu3q17qmfgqnp9dskbrg9uq1rydpyxp7qy/pub/locks.app/config.json"
        );
    }

    #[test]
    fn creator_pointer_request_prepares_browser_fetch_url_and_pubky_host() {
        let creator =
            CreatorPubky::from_str("pubkytkrq8zmwb8a3m9k15csu3q17qmfgqnp9dskbrg9uq1rydpyxp7qy")
                .unwrap();
        let request = build_creator_pointer_request(&creator).unwrap();
        let endpoint = locks_sdk::transport::BrowserEndpoint {
            domain: Some("creator.example".to_owned()),
            port: Some(8443),
            params: std::collections::BTreeMap::new(),
        };

        let prepared =
            prepare_creator_pointer_request_for_endpoint(&request, &endpoint, None).unwrap();

        assert_eq!(prepared.method, "GET");
        assert_eq!(prepared.path, "/pub/locks.app/config.json");
        assert_eq!(
            prepared.url.as_str(),
            "https://creator.example:8443/pub/locks.app/config.json"
        );
        assert_eq!(
            prepared.pubky_host.as_deref(),
            Some("tkrq8zmwb8a3m9k15csu3q17qmfgqnp9dskbrg9uq1rydpyxp7qy")
        );
    }

    #[test]
    fn content_lock_request_uses_public_lock_resource_path_without_auth() {
        let resource = PubkyLockResource::from_str(
            "pubkytkrq8zmwb8a3m9k15csu3q17qmfgqnp9dskbrg9uq1rydpyxp7qy/pub/locks.app/000G40R40M30E209185GR38E1W8124GK2GAHC5RR34D1P70X3RFG.json",
        )
        .unwrap();

        let request = build_content_lock_request(&resource).unwrap();

        assert_eq!(request.method, "GET");
        assert_eq!(
            request.path,
            "/pub/locks.app/000G40R40M30E209185GR38E1W8124GK2GAHC5RR34D1P70X3RFG.json"
        );
        assert_eq!(
            request.url.as_str(),
            "https://_pubky.tkrq8zmwb8a3m9k15csu3q17qmfgqnp9dskbrg9uq1rydpyxp7qy/pub/locks.app/000G40R40M30E209185GR38E1W8124GK2GAHC5RR34D1P70X3RFG.json"
        );
    }

    #[test]
    fn content_lock_request_prepares_browser_fetch_url_and_pubky_host() {
        let resource = PubkyLockResource::from_str(
            "pubkytkrq8zmwb8a3m9k15csu3q17qmfgqnp9dskbrg9uq1rydpyxp7qy/pub/locks.app/000G40R40M30E209185GR38E1W8124GK2GAHC5RR34D1P70X3RFG.json",
        )
        .unwrap();
        let request = build_content_lock_request(&resource).unwrap();
        let endpoint = locks_sdk::transport::BrowserEndpoint {
            domain: Some("creator.example".to_owned()),
            port: Some(8443),
            params: std::collections::BTreeMap::new(),
        };

        let prepared =
            prepare_content_lock_request_for_endpoint(&request, &endpoint, None).unwrap();

        assert_eq!(prepared.method, "GET");
        assert_eq!(
            prepared.path,
            "/pub/locks.app/000G40R40M30E209185GR38E1W8124GK2GAHC5RR34D1P70X3RFG.json"
        );
        assert_eq!(
            prepared.url.as_str(),
            "https://creator.example:8443/pub/locks.app/000G40R40M30E209185GR38E1W8124GK2GAHC5RR34D1P70X3RFG.json"
        );
        assert_eq!(
            prepared.pubky_host.as_deref(),
            Some("tkrq8zmwb8a3m9k15csu3q17qmfgqnp9dskbrg9uq1rydpyxp7qy")
        );
    }

    #[test]
    fn connect_url_includes_return_to_and_state_without_auth_url() {
        let locks =
            Locks::for_server("pubky7ir1ttte48bcp4zjychjyscicrwi1j34mtt91ptsafdbjmr8g9eo").unwrap();

        let url = locks
            .build_connect_url("https://pubky.app/locks/callback", "opaque-state")
            .unwrap();

        assert_eq!(url.path(), "/connect");
        assert_eq!(
            url.query(),
            Some("return_to=https%3A%2F%2Fpubky.app%2Flocks%2Fcallback&state=opaque-state")
        );
        assert!(!url.as_str().contains("authorization_url"));
    }

    #[test]
    fn connect_url_can_be_prepared_for_browser_endpoint() {
        let locks =
            Locks::for_server("pubky7ir1ttte48bcp4zjychjyscicrwi1j34mtt91ptsafdbjmr8g9eo").unwrap();
        let endpoint = locks_sdk::transport::BrowserEndpoint {
            domain: Some("locks.example".to_owned()),
            port: Some(8443),
            params: std::collections::BTreeMap::new(),
        };

        let url = locks
            .build_connect_url_for_endpoint(
                "https://pubky.app/locks/callback",
                "opaque-state",
                &endpoint,
                None,
            )
            .unwrap();

        assert_eq!(url.scheme(), "https");
        assert_eq!(url.host_str(), Some("locks.example"));
        assert_eq!(url.port(), Some(8443));
        assert_eq!(url.path(), "/connect");
        assert_eq!(
            url.query(),
            Some("return_to=https%3A%2F%2Fpubky.app%2Flocks%2Fcallback&state=opaque-state")
        );
    }

    #[test]
    fn parse_connect_callback_extracts_code_and_state() {
        let callback = parse_connect_callback_url(
            "https://pubky.app/locks/callback?code=frontend-code&state=opaque-state",
        )
        .unwrap();

        assert_eq!(callback.code(), "frontend-code");
        assert_eq!(callback.state(), "opaque-state");
    }

    #[test]
    fn parse_connect_callback_rejects_missing_code() {
        let result =
            parse_connect_callback_url("https://pubky.app/locks/callback?state=opaque-state");

        assert!(result.is_err());
    }

    #[test]
    fn exchange_frontend_session_code_request_posts_code_and_state_only() {
        let options = ExchangeFrontendSessionCodeOptions::new(
            "frontend-code".to_owned(),
            "opaque-state".to_owned(),
        );

        let request = exchange_frontend_session_code_request(&options);

        assert_eq!(request.method, "POST");
        assert_eq!(request.path, "/frontend-sessions");
        assert_eq!(
            request.body,
            serde_json::json!({
                "code": "frontend-code",
                "state": "opaque-state"
            })
        );
        assert!(request.body.get("session_token").is_none());
        assert!(request.body.get("creator").is_none());
    }

    #[test]
    fn exchange_frontend_session_code_request_prepares_browser_fetch_url_and_pubky_host() {
        let locks =
            Locks::for_server("pubky7ir1ttte48bcp4zjychjyscicrwi1j34mtt91ptsafdbjmr8g9eo").unwrap();
        let options = ExchangeFrontendSessionCodeOptions::new(
            "frontend-code".to_owned(),
            "opaque-state".to_owned(),
        );
        let request = exchange_frontend_session_code_request(&options);
        let endpoint = locks_sdk::transport::BrowserEndpoint {
            domain: Some("locks.example".to_owned()),
            port: Some(8443),
            params: std::collections::BTreeMap::new(),
        };

        let prepared = locks
            .prepare_exchange_request_for_endpoint(&request, &endpoint, None)
            .unwrap();

        assert_eq!(prepared.method, "POST");
        assert_eq!(prepared.path, "/frontend-sessions");
        assert_eq!(
            prepared.url.as_str(),
            "https://locks.example:8443/frontend-sessions"
        );
        assert_eq!(
            prepared.pubky_host.as_deref(),
            Some("7ir1ttte48bcp4zjychjyscicrwi1j34mtt91ptsafdbjmr8g9eo")
        );
        assert_eq!(prepared.body, request.body);
    }
}
