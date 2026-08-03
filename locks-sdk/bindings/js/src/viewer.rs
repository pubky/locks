#[cfg(any(test, target_arch = "wasm32"))]
use serde_json::Value;
use wasm_bindgen::prelude::*;

use crate::locks::LocksOptions;

#[cfg(target_arch = "wasm32")]
use crate::js_error::{JsResult, invalid_input};
#[cfg(target_arch = "wasm32")]
use crate::json::to_plain_js_value;
#[cfg(target_arch = "wasm32")]
use crate::session::BrowserPkarrResolver;

#[wasm_bindgen]
#[derive(Debug, Clone)]
pub struct BundleId {
    inner: locks_core::ids::BundleId,
}

#[wasm_bindgen]
impl BundleId {
    #[wasm_bindgen(constructor)]
    pub fn new(value: String) -> Result<BundleId, JsValue> {
        Self::parse_value(value).map_err(|err| JsValue::from_str(&err))
    }

    pub fn generate() -> BundleId {
        Self {
            inner: locks_core::ids::BundleId::new_random(),
        }
    }

    #[wasm_bindgen(js_name = toString)]
    pub fn as_string(&self) -> String {
        self.inner.to_string()
    }
}

impl BundleId {
    fn parse_value(value: String) -> Result<BundleId, String> {
        use std::str::FromStr;

        let inner = locks_core::ids::BundleId::from_str(&value)
            .map_err(|err| format!("invalid bundle id: {err}"))?;
        Ok(Self { inner })
    }
}

impl std::fmt::Display for BundleId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.inner.as_str())
    }
}

#[wasm_bindgen]
pub struct VerificationTaskHandleOptions {
    creator: String,
    bundle_id: String,
}

#[wasm_bindgen]
impl VerificationTaskHandleOptions {
    #[wasm_bindgen(constructor)]
    pub fn new(creator: String, bundle_id: String) -> Result<Self, JsValue> {
        Self::parse_values(creator, bundle_id).map_err(|err| JsValue::from_str(&err))
    }

    #[wasm_bindgen(getter)]
    pub fn creator(&self) -> String {
        self.creator.clone()
    }

    #[wasm_bindgen(getter, js_name = bundleId)]
    pub fn bundle_id(&self) -> String {
        self.bundle_id.clone()
    }
}

impl VerificationTaskHandleOptions {
    fn parse_values(creator: String, bundle_id: String) -> Result<Self, String> {
        use std::str::FromStr;

        let creator = locks_core::ids::CreatorPubky::from_str(&creator)
            .map_err(|err| format!("invalid creator pubky: {err}"))?
            .to_string();
        let bundle_id = locks_core::ids::BundleId::from_str(&bundle_id)
            .map_err(|err| format!("invalid bundle id: {err}"))?
            .to_string();
        Ok(Self { creator, bundle_id })
    }
}

#[cfg(any(test, target_arch = "wasm32"))]
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct JsViewerRequestPlan {
    pub method: &'static str,
    pub path: String,
    pub url: url::Url,
    pub authorization: Option<String>,
    pub body: Value,
}

#[cfg(any(test, target_arch = "wasm32"))]
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct JsPreparedViewerRequest {
    pub method: &'static str,
    pub path: String,
    pub url: url::Url,
    pub pubky_host: Option<String>,
    pub authorization: Option<String>,
    pub body: Value,
}

#[wasm_bindgen]
#[derive(Debug, Clone)]
pub struct Viewer {
    #[cfg_attr(not(target_arch = "wasm32"), allow(dead_code))]
    client: locks_sdk::LocksClient,
    #[cfg_attr(not(target_arch = "wasm32"), allow(dead_code))]
    options: LocksOptions,
    #[cfg_attr(not(target_arch = "wasm32"), allow(dead_code))]
    inner: locks_sdk::ViewerLocks,
}

#[wasm_bindgen]
impl Viewer {
    #[cfg(target_arch = "wasm32")]
    #[wasm_bindgen(js_name = submitProofBundle)]
    pub async fn submit_proof_bundle(
        &self,
        submitted_proof_bundle: wasm_bindgen::JsValue,
    ) -> JsResult<wasm_bindgen::JsValue> {
        let submitted_proof_bundle = serde_wasm_bindgen::from_value(submitted_proof_bundle)
            .map_err(|err| invalid_input(format!("invalid submitted proof bundle: {err}")))?;
        let resolver = BrowserPkarrResolver::new_with_options(&self.options)
            .map_err(|err| invalid_input(err.to_string()))?;
        let request = self
            .build_submit_proof_bundle_request(submitted_proof_bundle)
            .map_err(invalid_input)?
            .prepare_with_pkarr_resolver(&resolver, None)
            .await
            .map_err(|err| invalid_input(err.to_string()))?;
        fetch_viewer_lifecycle_json(&request).await
    }

    #[cfg(target_arch = "wasm32")]
    #[wasm_bindgen(js_name = lookupVerificationTask)]
    pub async fn lookup_verification_task(
        &self,
        options: &VerificationTaskHandleOptions,
    ) -> JsResult<wasm_bindgen::JsValue> {
        let resolver = BrowserPkarrResolver::new_with_options(&self.options)
            .map_err(|err| invalid_input(err.to_string()))?;
        let request = self
            .build_lookup_verification_task_request(options)
            .map_err(invalid_input)?
            .prepare_with_pkarr_resolver(&resolver, None)
            .await
            .map_err(|err| invalid_input(err.to_string()))?;
        fetch_viewer_lifecycle_json(&request).await
    }

    #[cfg(target_arch = "wasm32")]
    #[wasm_bindgen(js_name = issueAccessCredential)]
    pub async fn issue_access_credential(
        &self,
        options: &VerificationTaskHandleOptions,
    ) -> JsResult<wasm_bindgen::JsValue> {
        let resolver = BrowserPkarrResolver::new_with_options(&self.options)
            .map_err(|err| invalid_input(err.to_string()))?;
        let request = self
            .build_issue_access_credential_request(options)
            .map_err(invalid_input)?
            .prepare_with_pkarr_resolver(&resolver, None)
            .await
            .map_err(|err| invalid_input(err.to_string()))?;
        fetch_viewer_access_credential_json(&request).await
    }

    #[cfg(target_arch = "wasm32")]
    #[wasm_bindgen(js_name = completeVerificationTask)]
    pub async fn complete_verification_task(
        &self,
        options: &VerificationTaskHandleOptions,
    ) -> JsResult<wasm_bindgen::JsValue> {
        let resolver = BrowserPkarrResolver::new_with_options(&self.options)
            .map_err(|err| invalid_input(err.to_string()))?;
        let request = self
            .build_complete_verification_task_request(options)
            .map_err(invalid_input)?
            .prepare_with_pkarr_resolver(&resolver, None)
            .await
            .map_err(|err| invalid_input(err.to_string()))?;
        fetch_viewer_lifecycle_json(&request).await
    }

    #[cfg(target_arch = "wasm32")]
    #[wasm_bindgen(js_name = proxyReadGuardedResource)]
    pub async fn proxy_read_guarded_resource(
        &self,
        access_credential: &str,
        path: String,
    ) -> JsResult<js_sys::Uint8Array> {
        let resolver = BrowserPkarrResolver::new_with_options(&self.options)
            .map_err(|err| invalid_input(err.to_string()))?;
        let request = self
            .build_proxy_read_guarded_resource_request(access_credential, path)
            .prepare_with_pkarr_resolver(&resolver, None)
            .await
            .map_err(|err| invalid_input(err.to_string()))?;
        fetch_viewer_bytes(&request).await
    }
}

impl Viewer {
    pub(crate) fn new(client: locks_sdk::LocksClient, options: LocksOptions) -> Self {
        Self {
            client,
            options,
            inner: locks_sdk::ViewerLocks::new(),
        }
    }

    #[cfg(any(test, target_arch = "wasm32"))]
    fn request_plan(&self, request: locks_sdk::SdkViewerRequest) -> JsViewerRequestPlan {
        JsViewerRequestPlan {
            method: request.method,
            path: request.path.clone(),
            url: self
                .client
                .transport_url(&request.path)
                .expect("binding request path is a valid Lock Server URL"),
            authorization: request.authorization,
            body: request.body,
        }
    }

    #[cfg(any(test, target_arch = "wasm32"))]
    #[cfg_attr(target_arch = "wasm32", allow(dead_code))]
    pub(crate) fn build_submit_proof_bundle_request(
        &self,
        submitted_proof_bundle: Value,
    ) -> Result<JsViewerRequestPlan, String> {
        let submitted_proof_bundle = serde_json::from_value(submitted_proof_bundle)
            .map_err(|err| format!("invalid submitted proof bundle: {err}"))?;
        Ok(self.request_plan(self.inner.submit_proof_bundle(submitted_proof_bundle)))
    }

    #[cfg(any(test, target_arch = "wasm32"))]
    #[cfg_attr(target_arch = "wasm32", allow(dead_code))]
    pub(crate) fn build_lookup_verification_task_request(
        &self,
        options: &VerificationTaskHandleOptions,
    ) -> Result<JsViewerRequestPlan, String> {
        Ok(self.request_plan(
            self.inner
                .lookup_verification_task(parse_handle_options(options)?),
        ))
    }

    #[cfg(any(test, target_arch = "wasm32"))]
    #[cfg_attr(target_arch = "wasm32", allow(dead_code))]
    pub(crate) fn build_issue_access_credential_request(
        &self,
        options: &VerificationTaskHandleOptions,
    ) -> Result<JsViewerRequestPlan, String> {
        Ok(self.request_plan(
            self.inner
                .issue_access_credential(parse_handle_options(options)?),
        ))
    }

    #[cfg(any(test, target_arch = "wasm32"))]
    #[cfg_attr(target_arch = "wasm32", allow(dead_code))]
    pub(crate) fn build_complete_verification_task_request(
        &self,
        options: &VerificationTaskHandleOptions,
    ) -> Result<JsViewerRequestPlan, String> {
        Ok(self.request_plan(
            self.inner
                .complete_verification_task(parse_handle_options(options)?),
        ))
    }

    #[cfg(any(test, target_arch = "wasm32"))]
    #[cfg_attr(target_arch = "wasm32", allow(dead_code))]
    pub(crate) fn build_proxy_read_guarded_resource_request(
        &self,
        access_credential: &str,
        path: impl Into<String>,
    ) -> JsViewerRequestPlan {
        self.request_plan(
            self.inner
                .proxy_read_guarded_resource(access_credential, path),
        )
    }
}

#[cfg(any(test, target_arch = "wasm32"))]
fn parse_handle_options(
    options: &VerificationTaskHandleOptions,
) -> Result<locks_sdk::VerificationTaskHandleRequest, String> {
    use std::str::FromStr;

    Ok(locks_sdk::VerificationTaskHandleRequest {
        creator: locks_core::ids::CreatorPubky::from_str(&options.creator)
            .map_err(|err| format!("invalid creator pubky: {err}"))?,
        bundle_id: locks_core::ids::BundleId::from_str(&options.bundle_id)
            .map_err(|err| format!("invalid bundle id: {err}"))?,
    })
}

#[cfg(any(test, target_arch = "wasm32"))]
impl JsViewerRequestPlan {
    #[cfg_attr(target_arch = "wasm32", allow(dead_code))]
    pub(crate) fn prepare_for_browser_endpoint(
        &self,
        endpoint: &locks_sdk::transport::BrowserEndpoint,
        testnet_host: Option<&str>,
    ) -> locks_sdk::Result<JsPreparedViewerRequest> {
        let browser_request = locks_sdk::transport::rewrite_browser_request(
            self.url.as_str(),
            endpoint,
            testnet_host,
        )?;
        Ok(JsPreparedViewerRequest {
            method: self.method,
            path: self.path.clone(),
            url: browser_request.url,
            pubky_host: browser_request.pubky_host,
            authorization: self.authorization.clone(),
            body: self.body.clone(),
        })
    }

    #[cfg(target_arch = "wasm32")]
    pub(crate) async fn prepare_with_pkarr_resolver(
        &self,
        resolver: &BrowserPkarrResolver,
        testnet_host: Option<&str>,
    ) -> locks_sdk::Result<JsPreparedViewerRequest> {
        let qname = self
            .url
            .host_str()
            .and_then(|host| host.strip_prefix("_pubky."))
            .ok_or(locks_sdk::LocksSdkError::InvalidTransportUrl)?;
        let endpoint = resolver.resolve_browser_endpoint(qname).await?;
        self.prepare_for_browser_endpoint(&endpoint, testnet_host)
    }
}

#[cfg(any(test, target_arch = "wasm32"))]
fn validate_lifecycle_response_for_tests(value: Value) -> Result<Value, String> {
    locks_sdk::ViewerLocks::parse_lifecycle_response(value.clone())
        .map_err(|err| err.to_string())?;
    Ok(value)
}

#[cfg(any(test, target_arch = "wasm32"))]
fn validate_access_credential_response_for_tests(value: Value) -> Result<Value, String> {
    locks_sdk::ViewerLocks::parse_access_credential_response(value.clone())
        .map_err(|err| err.to_string())?;
    Ok(value)
}

#[cfg(target_arch = "wasm32")]
async fn fetch_viewer_lifecycle_json(
    request: &JsPreparedViewerRequest,
) -> JsResult<wasm_bindgen::JsValue> {
    let value = fetch_viewer_json_value(request).await?;
    let validated = validate_lifecycle_response_for_tests(value).map_err(invalid_input)?;
    to_plain_js_value(&validated)
        .map_err(|err| invalid_input(format!("failed to encode lifecycle response: {err:?}")))
}

#[cfg(target_arch = "wasm32")]
async fn fetch_viewer_access_credential_json(
    request: &JsPreparedViewerRequest,
) -> JsResult<wasm_bindgen::JsValue> {
    let value = fetch_viewer_json_value(request).await?;
    let validated = validate_access_credential_response_for_tests(value).map_err(invalid_input)?;
    to_plain_js_value(&validated).map_err(|err| {
        invalid_input(format!(
            "failed to encode access credential response: {err:?}"
        ))
    })
}

#[cfg(target_arch = "wasm32")]
async fn fetch_viewer_json_value(request: &JsPreparedViewerRequest) -> JsResult<Value> {
    let response = fetch_viewer(request).await?;
    if !response.ok() {
        return Err(invalid_input(format!(
            "Lock Server viewer request failed with HTTP {}",
            response.status()
        )));
    }
    let value = wasm_bindgen_futures::JsFuture::from(
        response
            .json()
            .map_err(|err| invalid_input(format!("failed to read JSON response: {err:?}")))?,
    )
    .await
    .map_err(|err| invalid_input(format!("failed to parse JSON response: {err:?}")))?;
    serde_wasm_bindgen::from_value(value)
        .map_err(|err| invalid_input(format!("failed to decode JSON response: {err}")))
}

#[cfg(target_arch = "wasm32")]
async fn fetch_viewer_bytes(request: &JsPreparedViewerRequest) -> JsResult<js_sys::Uint8Array> {
    let response = fetch_viewer(request).await?;
    if !response.ok() {
        return Err(invalid_input(format!(
            "Lock Server viewer request failed with HTTP {}",
            response.status()
        )));
    }
    let buffer = wasm_bindgen_futures::JsFuture::from(
        response
            .array_buffer()
            .map_err(|err| invalid_input(format!("failed to read byte response: {err:?}")))?,
    )
    .await
    .map_err(|err| invalid_input(format!("failed to read byte response: {err:?}")))?;
    Ok(js_sys::Uint8Array::new(&buffer))
}

#[cfg(target_arch = "wasm32")]
async fn fetch_viewer(request: &JsPreparedViewerRequest) -> JsResult<web_sys::Response> {
    use wasm_bindgen::JsCast;

    let request_init = web_sys::RequestInit::new();
    request_init.set_method(request.method);
    request_init.set_mode(web_sys::RequestMode::Cors);
    if !request.body.is_null() {
        request_init.set_body(&wasm_bindgen::JsValue::from_str(&request.body.to_string()));
    }

    let web_request = web_sys::Request::new_with_str_and_init(request.url.as_str(), &request_init)
        .map_err(|err| invalid_input(format!("failed to build viewer request: {err:?}")))?;
    if let Some(authorization) = &request.authorization {
        web_request
            .headers()
            .set("authorization", authorization)
            .map_err(|err| invalid_input(format!("failed to set authorization header: {err:?}")))?;
    }
    if let Some(pubky_host) = &request.pubky_host {
        web_request
            .headers()
            .set("pubky-host", pubky_host)
            .map_err(|err| invalid_input(format!("failed to set pubky-host header: {err:?}")))?;
    }
    if !request.body.is_null() {
        web_request
            .headers()
            .set("content-type", "application/json")
            .map_err(|err| invalid_input(format!("failed to set content-type header: {err:?}")))?;
    }

    let window = web_sys::window().ok_or_else(|| invalid_input("window is unavailable"))?;
    let response_value =
        wasm_bindgen_futures::JsFuture::from(window.fetch_with_request(&web_request))
            .await
            .map_err(|err| invalid_input(format!("Lock Server viewer request failed: {err:?}")))?;
    response_value
        .dyn_into()
        .map_err(|_| invalid_input("fetch returned a non-Response value"))
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use super::*;
    use serde_json::json;

    const CREATOR: &str = "pubkytkrq8zmwb8a3m9k15csu3q17qmfgqnp9dskbrg9uq1rydpyxp7qy";
    const BUNDLE_ID: &str = "000G40R40M30E209185GR38E1W";
    const LOCK_ID: &str = "000G40R40M30E209185GR38E1W8124GK2GAHC5RR34D1P70X3RFG";

    fn test_viewer() -> Viewer {
        use std::str::FromStr;

        let lock_server = locks_core::ids::LockServerPubky::from_str(
            "pubky7ir1ttte48bcp4zjychjyscicrwi1j34mtt91ptsafdbjmr8g9eo",
        )
        .unwrap();
        Viewer::new(
            locks_sdk::LocksClient::for_server(lock_server),
            LocksOptions::new(),
        )
    }

    fn handle_options() -> VerificationTaskHandleOptions {
        VerificationTaskHandleOptions::new(CREATOR.to_owned(), BUNDLE_ID.to_owned()).unwrap()
    }

    fn submitted_proof_bundle() -> Value {
        json!({
            "version": 1,
            "bundle_id": BUNDLE_ID,
            "pubky_lock_resource": format!("{CREATOR}/pub/locks.app/{LOCK_ID}.json"),
            "proofs": [{
                "criterion_id": "criterion-1",
                "verifier_type": "dev-static",
                "payload": { "satisfied": true }
            }]
        })
    }

    #[test]
    fn bundle_id_constructor_canonicalizes_and_rejects_invalid_values() {
        assert_eq!(
            BundleId::parse_value(BUNDLE_ID.to_lowercase())
                .unwrap()
                .to_string(),
            BUNDLE_ID
        );
        assert!(BundleId::parse_value("bad-bundle-id".to_owned()).is_err());
    }

    #[test]
    fn bundle_id_generate_returns_parseable_bundle_id() {
        let bundle_id = BundleId::generate();

        assert_eq!(bundle_id.to_string().len(), 26);
        assert!(locks_core::ids::BundleId::from_str(&bundle_id.to_string()).is_ok());
    }

    #[test]
    fn verification_task_handle_constructor_validates_and_canonicalizes_inputs() {
        let handle = VerificationTaskHandleOptions::parse_values(
            CREATOR.to_owned(),
            BUNDLE_ID.to_lowercase(),
        )
        .unwrap();

        assert_eq!(handle.creator(), CREATOR);
        assert_eq!(handle.bundle_id(), BUNDLE_ID);
        assert!(
            VerificationTaskHandleOptions::parse_values(
                "not-a-pubky".to_owned(),
                BUNDLE_ID.to_owned()
            )
            .is_err()
        );
        assert!(
            VerificationTaskHandleOptions::parse_values(
                CREATOR.to_owned(),
                "bad-bundle-id".to_owned()
            )
            .is_err()
        );
    }

    #[test]
    fn submit_proof_bundle_request_uses_public_envelope_without_auth() {
        let viewer = test_viewer();

        let request = viewer
            .build_submit_proof_bundle_request(submitted_proof_bundle())
            .unwrap();

        assert_eq!(request.method, "POST");
        assert_eq!(request.path, "/proof-bundles");
        assert_eq!(request.authorization, None);
        assert_eq!(
            request.body,
            json!({ "submitted_proof_bundle": submitted_proof_bundle() })
        );
    }

    #[test]
    fn submit_proof_bundle_request_canonicalizes_bundle_id_before_sending() {
        let viewer = test_viewer();
        let mut proof_bundle = submitted_proof_bundle();
        proof_bundle["bundle_id"] = Value::String(BUNDLE_ID.to_lowercase());

        let request = viewer
            .build_submit_proof_bundle_request(proof_bundle)
            .unwrap();

        assert_eq!(
            request.body["submitted_proof_bundle"]["bundle_id"],
            BUNDLE_ID
        );
    }

    #[test]
    fn verification_task_lookup_request_keeps_bundle_id_in_json_body() {
        let viewer = test_viewer();

        let request = viewer
            .build_lookup_verification_task_request(&handle_options())
            .unwrap();

        assert_eq!(request.method, "POST");
        assert_eq!(request.path, "/verification-task-lookups");
        assert_eq!(request.authorization, None);
        assert_eq!(
            request.body,
            json!({ "creator": CREATOR, "bundle_id": BUNDLE_ID })
        );
        assert!(!request.url.as_str().contains(BUNDLE_ID));
    }

    #[test]
    fn issue_access_credential_request_uses_handle_body_without_auth() {
        let viewer = test_viewer();

        let request = viewer
            .build_issue_access_credential_request(&handle_options())
            .unwrap();

        assert_eq!(request.method, "POST");
        assert_eq!(request.path, "/access-credentials");
        assert_eq!(request.authorization, None);
        assert_eq!(
            request.body,
            json!({ "creator": CREATOR, "bundle_id": BUNDLE_ID })
        );
        assert!(request.body.get("task_id").is_none());
    }

    #[test]
    fn complete_verification_task_request_uses_handle_body_without_auth() {
        let viewer = test_viewer();

        let request = viewer
            .build_complete_verification_task_request(&handle_options())
            .unwrap();

        assert_eq!(request.method, "POST");
        assert_eq!(request.path, "/verification-task-completions");
        assert_eq!(request.authorization, None);
        assert_eq!(
            request.body,
            json!({ "creator": CREATOR, "bundle_id": BUNDLE_ID })
        );
        assert!(request.body.get("task_id").is_none());
    }

    #[test]
    fn proxy_read_guarded_resource_request_uses_bearer_header_only() {
        let viewer = test_viewer();

        let request = viewer.build_proxy_read_guarded_resource_request(
            "raw-access-credential",
            "nested/example file.txt",
        );

        assert_eq!(request.method, "GET");
        assert_eq!(
            request.path,
            "/priv-resources/content/nested/example%20file.txt"
        );
        assert_eq!(
            request.authorization.as_deref(),
            Some("Bearer raw-access-credential")
        );
        assert_eq!(request.body, Value::Null);
        assert!(!request.url.as_str().contains("raw-access-credential"));
    }

    #[test]
    fn lifecycle_response_validation_rejects_internal_task_id() {
        let result = validate_lifecycle_response_for_tests(json!({
            "creator": CREATOR,
            "bundle_id": BUNDLE_ID,
            "status": "pending",
            "submitted_at": "2026-06-01T12:00:00Z",
            "started_at": null,
            "completed_at": null,
            "failure_message": null,
            "task_id": "018fc6ec-2f3d-4f7e-8b7d-6f5c4b3a2d10"
        }));

        assert!(result.is_err());
    }

    #[test]
    fn access_credential_response_validation_rejects_extra_proof_material() {
        let result = validate_access_credential_response_for_tests(json!({
            "credential": "raw-access-credential",
            "expires_at": "2026-06-01T12:15:00Z",
            "submitted_proof_bundle": { "not": "viewer safe" }
        }));

        assert!(result.is_err());
    }

    #[test]
    fn viewer_response_validation_accepts_documented_shapes() {
        let lifecycle = validate_lifecycle_response_for_tests(json!({
            "creator": CREATOR,
            "bundle_id": BUNDLE_ID,
            "status": "completed",
            "submitted_at": "2026-06-01T12:00:00Z",
            "started_at": "2026-06-01T12:00:01Z",
            "completed_at": "2026-06-01T12:00:02Z",
            "failure_message": null
        }))
        .unwrap();
        assert_eq!(lifecycle["status"], "completed");

        let credential = validate_access_credential_response_for_tests(json!({
            "credential": "raw-access-credential",
            "expires_at": "2026-06-01T12:15:00Z"
        }))
        .unwrap();
        assert_eq!(credential["credential"], "raw-access-credential");
    }

    #[test]
    fn viewer_request_prepares_browser_fetch_url_and_pubky_host() {
        let viewer = test_viewer();
        let request = viewer
            .build_lookup_verification_task_request(&handle_options())
            .unwrap();
        let endpoint = locks_sdk::transport::BrowserEndpoint {
            domain: Some("locks.example".to_owned()),
            port: Some(8443),
            params: std::collections::BTreeMap::new(),
        };

        let prepared = request
            .prepare_for_browser_endpoint(&endpoint, None)
            .unwrap();

        assert_eq!(prepared.method, "POST");
        assert_eq!(prepared.path, "/verification-task-lookups");
        assert_eq!(
            prepared.url.as_str(),
            "https://locks.example:8443/verification-task-lookups"
        );
        assert_eq!(
            prepared.pubky_host.as_deref(),
            Some("7ir1ttte48bcp4zjychjyscicrwi1j34mtt91ptsafdbjmr8g9eo")
        );
        assert_eq!(prepared.authorization, None);
    }
}
