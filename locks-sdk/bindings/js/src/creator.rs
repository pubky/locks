use std::cell::RefCell;
use std::rc::Rc;

use crate::json::to_plain_js_value;
use crate::session::Session;
#[cfg(target_arch = "wasm32")]
use crate::session::{BrowserPkarrResolver, fetch_authorized_empty, fetch_authorized_json};
#[cfg(any(test, target_arch = "wasm32"))]
use crate::session::{JsAuthorizedRequestPlan, JsRequestBody};
#[cfg(any(test, target_arch = "wasm32"))]
use locks_core::ids::{LockId, LockServerPubky};
#[cfg(any(test, target_arch = "wasm32"))]
use std::str::FromStr;
use wasm_bindgen::prelude::*;

#[wasm_bindgen]
pub struct RegisterGuardedResourceOptions {
    path: String,
    content_type: String,
    bytes: Vec<u8>,
}

#[wasm_bindgen]
impl RegisterGuardedResourceOptions {
    #[wasm_bindgen(constructor)]
    pub fn new(path: String, content_type: String, bytes: Vec<u8>) -> Self {
        Self {
            path,
            content_type,
            bytes,
        }
    }

    #[wasm_bindgen(getter)]
    pub fn path(&self) -> String {
        self.path.clone()
    }

    #[wasm_bindgen(getter, js_name = contentType)]
    pub fn content_type(&self) -> String {
        self.content_type.clone()
    }

    #[wasm_bindgen(getter)]
    pub fn bytes(&self) -> Vec<u8> {
        self.bytes.clone()
    }
}

#[wasm_bindgen]
pub struct DeleteGuardedResourceOptions {
    path: String,
}

#[wasm_bindgen]
impl DeleteGuardedResourceOptions {
    #[wasm_bindgen(constructor)]
    pub fn new(path: String) -> Self {
        Self { path }
    }

    #[wasm_bindgen(getter)]
    pub fn path(&self) -> String {
        self.path.clone()
    }
}

#[wasm_bindgen]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DeleteContentLockMode {
    DefaultGraceful,
    ExplicitGraceful,
    Force,
}

#[wasm_bindgen]
pub struct DeleteContentLockOptions {
    mode: DeleteContentLockMode,
}

#[wasm_bindgen]
impl DeleteContentLockOptions {
    #[wasm_bindgen(constructor)]
    pub fn new(mode: DeleteContentLockMode) -> Self {
        Self { mode }
    }

    #[wasm_bindgen(getter)]
    pub fn mode(&self) -> DeleteContentLockMode {
        self.mode
    }
}

#[derive(Debug, Clone, Default)]
struct CreateContentLockRequestBuilderState {
    primary_resource: Option<serde_json::Value>,
    secondary_resources: serde_json::Map<String, serde_json::Value>,
    criteria: Option<serde_json::Value>,
    lock_logic: Option<serde_json::Value>,
    access_policy: Option<serde_json::Value>,
    lock_server: Option<serde_json::Value>,
}

#[wasm_bindgen]
#[derive(Debug, Clone, Default)]
pub struct CreateContentLockRequestBuilder {
    state: Rc<RefCell<CreateContentLockRequestBuilderState>>,
}

#[wasm_bindgen]
impl CreateContentLockRequestBuilder {
    #[wasm_bindgen(constructor)]
    pub fn new() -> Self {
        Self::default()
    }

    #[wasm_bindgen(js_name = primaryResource)]
    pub fn primary_resource(
        &self,
        resource: wasm_bindgen::JsValue,
    ) -> crate::js_error::JsResult<Self> {
        self.state.borrow_mut().primary_resource =
            Some(js_value_to_json(resource, "primary resource")?);
        Ok(self.clone())
    }

    #[wasm_bindgen(js_name = secondaryResource)]
    pub fn secondary_resource(
        &self,
        resource: wasm_bindgen::JsValue,
    ) -> crate::js_error::JsResult<Self> {
        let resource = js_value_to_json(resource, "secondary resource")?;
        self.add_secondary_resource(resource)
            .map_err(crate::js_error::invalid_input)?;
        Ok(self.clone())
    }

    #[wasm_bindgen(js_name = secondaryResources)]
    pub fn secondary_resources(
        &self,
        resources: wasm_bindgen::JsValue,
    ) -> crate::js_error::JsResult<Self> {
        let resources = js_value_to_json(resources, "secondary resources")?;
        let resources = resources.as_object().ok_or_else(|| {
            crate::js_error::invalid_input("secondary resources must be an object")
        })?;
        self.state.borrow_mut().secondary_resources = resources.clone();
        Ok(self.clone())
    }

    pub fn criteria(&self, criteria: wasm_bindgen::JsValue) -> crate::js_error::JsResult<Self> {
        self.state.borrow_mut().criteria = Some(js_value_to_json(criteria, "criteria")?);
        Ok(self.clone())
    }

    #[wasm_bindgen(js_name = lockLogic)]
    pub fn lock_logic(&self, lock_logic: wasm_bindgen::JsValue) -> crate::js_error::JsResult<Self> {
        self.state.borrow_mut().lock_logic = Some(js_value_to_json(lock_logic, "lock logic")?);
        Ok(self.clone())
    }

    #[wasm_bindgen(js_name = accessPolicy)]
    pub fn access_policy(
        &self,
        access_policy: wasm_bindgen::JsValue,
    ) -> crate::js_error::JsResult<Self> {
        self.state.borrow_mut().access_policy =
            Some(js_value_to_json(access_policy, "access policy")?);
        Ok(self.clone())
    }

    #[wasm_bindgen(js_name = lockServer)]
    pub fn lock_server(
        &self,
        lock_server: wasm_bindgen::JsValue,
    ) -> crate::js_error::JsResult<Self> {
        self.state.borrow_mut().lock_server = Some(js_value_to_json(lock_server, "lock server")?);
        Ok(self.clone())
    }

    pub fn build(&self) -> crate::js_error::JsResult<wasm_bindgen::JsValue> {
        to_plain_js_value(&self.build_value().map_err(crate::js_error::invalid_input)?).map_err(
            |err| crate::js_error::invalid_input(format!("invalid content lock request: {err:?}")),
        )
    }
}

impl CreateContentLockRequestBuilder {
    fn add_secondary_resource(&self, resource: serde_json::Value) -> Result<(), String> {
        let path = resource_path(&resource, "secondary resource")?;
        let secondary = secondary_descriptor_from_resource(&resource)?;
        let mut state = self.state.borrow_mut();
        if state.secondary_resources.contains_key(&path) {
            return Err(format!("duplicate secondary resource path: {path}"));
        }
        if state
            .primary_resource
            .as_ref()
            .and_then(|primary| resource_path(primary, "primary resource").ok())
            .as_deref()
            == Some(path.as_str())
        {
            return Err(format!(
                "primary resource path also appears as secondary resource: {path}"
            ));
        }
        state.secondary_resources.insert(path, secondary);
        Ok(())
    }

    fn build_value(&self) -> Result<serde_json::Value, String> {
        let state = self.state.borrow();
        let mut body = serde_json::Map::new();
        let primary_path = match &state.primary_resource {
            Some(primary_resource) => {
                let path = resource_path(primary_resource, "primary resource")?;
                body.insert("primary_resource".to_owned(), primary_resource.clone());
                Some(path)
            }
            None => None,
        };
        if state.secondary_resources.is_empty() && primary_path.is_none() {
            return Err("content lock request requires at least one resource".to_owned());
        }
        if let Some(primary_path) = &primary_path
            && state.secondary_resources.contains_key(primary_path)
        {
            return Err(format!(
                "primary resource path also appears as secondary resource: {primary_path}"
            ));
        }
        if !state.secondary_resources.is_empty() {
            body.insert(
                "secondary_resources".to_owned(),
                serde_json::Value::Object(state.secondary_resources.clone()),
            );
        }
        let criteria = state
            .criteria
            .as_ref()
            .ok_or_else(|| "content lock request requires criteria".to_owned())?;
        let typed_criteria: Vec<locks_core::lock_policy::Criterion> =
            serde_json::from_value(criteria.clone())
                .map_err(|err| format!("invalid content lock criteria: {err}"))?;
        for criterion in &typed_criteria {
            criterion
                .validate_params()
                .map_err(|err| format!("invalid content lock criterion: {err}"))?;
        }
        body.insert("criteria".to_owned(), criteria.clone());
        let lock_logic = state
            .lock_logic
            .as_ref()
            .ok_or_else(|| "content lock request requires lock logic".to_owned())?;
        body.insert("lock_logic".to_owned(), lock_logic.clone());
        let access_policy = state
            .access_policy
            .as_ref()
            .ok_or_else(|| "content lock request requires access policy".to_owned())?;
        body.insert("access_policy".to_owned(), access_policy.clone());
        let lock_server = state
            .lock_server
            .as_ref()
            .ok_or_else(|| "content lock request requires lock server".to_owned())?;
        body.insert("lock_server".to_owned(), lock_server.clone());
        Ok(serde_json::Value::Object(body))
    }
}

fn resource_path(resource: &serde_json::Value, label: &str) -> Result<String, String> {
    resource
        .get("path")
        .and_then(serde_json::Value::as_str)
        .filter(|path| !path.is_empty())
        .map(str::to_owned)
        .ok_or_else(|| format!("{label} requires non-empty path"))
}

fn secondary_descriptor_from_resource(
    resource: &serde_json::Value,
) -> Result<serde_json::Value, String> {
    let hash = resource
        .get("hash")
        .cloned()
        .ok_or_else(|| "secondary resource requires hash".to_owned())?;
    let content_type = resource
        .get("content_type")
        .cloned()
        .ok_or_else(|| "secondary resource requires content_type".to_owned())?;
    let size = resource
        .get("size")
        .cloned()
        .ok_or_else(|| "secondary resource requires size".to_owned())?;
    Ok(serde_json::json!({
        "hash": hash,
        "content_type": content_type,
        "size": size,
    }))
}

fn js_value_to_json(
    value: wasm_bindgen::JsValue,
    label: &str,
) -> crate::js_error::JsResult<serde_json::Value> {
    serde_wasm_bindgen::from_value(value)
        .map_err(|err| crate::js_error::invalid_input(format!("invalid {label}: {err}")))
}

#[wasm_bindgen]
pub struct SetLockServicePointerOptions {
    default_lock_server: String,
}

#[wasm_bindgen]
impl SetLockServicePointerOptions {
    #[wasm_bindgen(constructor)]
    pub fn new(default_lock_server: String) -> Self {
        Self {
            default_lock_server,
        }
    }

    #[wasm_bindgen(getter, js_name = defaultLockServer)]
    pub fn default_lock_server(&self) -> String {
        self.default_lock_server.clone()
    }
}

#[wasm_bindgen]
#[derive(Debug, Clone)]
pub struct Creator {
    session: Session,
}

#[wasm_bindgen]
impl Creator {
    #[wasm_bindgen(js_name = exportSessionSecretForTests)]
    pub fn export_session_secret_for_tests(&self) -> String {
        self.session.export_secret()
    }

    #[cfg(target_arch = "wasm32")]
    #[wasm_bindgen(js_name = registerGuardedResource)]
    pub async fn register_guarded_resource(
        &self,
        options: &RegisterGuardedResourceOptions,
    ) -> crate::js_error::JsResult<wasm_bindgen::JsValue> {
        let resolver = BrowserPkarrResolver::new_with_options(self.session.options())
            .map_err(|err| crate::js_error::invalid_input(err.to_string()))?;
        let request = self
            .build_register_guarded_resource_request(options)
            .prepare_with_pkarr_resolver(&resolver, None)
            .await
            .map_err(|err| crate::js_error::invalid_input(err.to_string()))?;
        fetch_authorized_json(&request).await
    }

    #[cfg(target_arch = "wasm32")]
    #[wasm_bindgen(js_name = createContentLock)]
    pub async fn create_content_lock(
        &self,
        body: wasm_bindgen::JsValue,
    ) -> crate::js_error::JsResult<wasm_bindgen::JsValue> {
        let body = serde_wasm_bindgen::from_value(body).map_err(|err| {
            crate::js_error::invalid_input(format!("invalid content lock body: {err}"))
        })?;
        let resolver = BrowserPkarrResolver::new_with_options(self.session.options())
            .map_err(|err| crate::js_error::invalid_input(err.to_string()))?;
        let request = self
            .create_build_content_lock_request(body)
            .map_err(crate::js_error::invalid_input)?
            .prepare_with_pkarr_resolver(&resolver, None)
            .await
            .map_err(|err| crate::js_error::invalid_input(err.to_string()))?;
        fetch_authorized_json(&request).await
    }

    #[cfg(target_arch = "wasm32")]
    #[wasm_bindgen(js_name = deleteGuardedResource)]
    pub async fn delete_guarded_resource(
        &self,
        options: &DeleteGuardedResourceOptions,
    ) -> crate::js_error::JsResult<()> {
        let resolver = BrowserPkarrResolver::new_with_options(self.session.options())
            .map_err(|err| crate::js_error::invalid_input(err.to_string()))?;
        let request = self
            .build_delete_guarded_resource_request(options)
            .prepare_with_pkarr_resolver(&resolver, None)
            .await
            .map_err(|err| crate::js_error::invalid_input(err.to_string()))?;
        fetch_authorized_empty(&request).await
    }

    #[cfg(target_arch = "wasm32")]
    #[wasm_bindgen(js_name = deleteContentLock)]
    pub async fn delete_content_lock(
        &self,
        lock_id: String,
        options: Option<DeleteContentLockOptions>,
    ) -> crate::js_error::JsResult<wasm_bindgen::JsValue> {
        let resolver = BrowserPkarrResolver::new_with_options(self.session.options())
            .map_err(|err| crate::js_error::invalid_input(err.to_string()))?;
        let request = self
            .build_delete_content_lock_request(&lock_id, options.as_ref())
            .map_err(crate::js_error::invalid_input)?
            .prepare_with_pkarr_resolver(&resolver, None)
            .await
            .map_err(|err| crate::js_error::invalid_input(err.to_string()))?;
        fetch_authorized_json(&request).await
    }

    #[cfg(target_arch = "wasm32")]
    #[wasm_bindgen(js_name = contentLockDeletionStatus)]
    pub async fn content_lock_deletion_status(
        &self,
        lock_id: String,
    ) -> crate::js_error::JsResult<wasm_bindgen::JsValue> {
        let resolver = BrowserPkarrResolver::new_with_options(self.session.options())
            .map_err(|err| crate::js_error::invalid_input(err.to_string()))?;
        let request = self
            .build_content_lock_deletion_status_request(&lock_id)
            .map_err(crate::js_error::invalid_input)?
            .prepare_with_pkarr_resolver(&resolver, None)
            .await
            .map_err(|err| crate::js_error::invalid_input(err.to_string()))?;
        fetch_authorized_json(&request).await
    }

    #[cfg(target_arch = "wasm32")]
    #[wasm_bindgen(js_name = setLockServicePointer)]
    pub async fn set_lock_service_pointer(
        &self,
        options: &SetLockServicePointerOptions,
    ) -> crate::js_error::JsResult<()> {
        let resolver = BrowserPkarrResolver::new_with_options(self.session.options())
            .map_err(|err| crate::js_error::invalid_input(err.to_string()))?;
        let request = self
            .build_set_lock_service_pointer_request(options)
            .map_err(crate::js_error::invalid_input)?
            .prepare_with_pkarr_resolver(&resolver, None)
            .await
            .map_err(|err| crate::js_error::invalid_input(err.to_string()))?;
        fetch_authorized_empty(&request).await
    }

    #[cfg(target_arch = "wasm32")]
    #[wasm_bindgen(js_name = paykitSetupStatus)]
    pub async fn paykit_setup_status(&self) -> crate::js_error::JsResult<wasm_bindgen::JsValue> {
        let resolver = BrowserPkarrResolver::new_with_options(self.session.options())
            .map_err(|err| crate::js_error::invalid_input(err.to_string()))?;
        let request = self
            .build_paykit_setup_status_request()
            .prepare_with_pkarr_resolver(&resolver, None)
            .await
            .map_err(|err| crate::js_error::invalid_input(err.to_string()))?;
        let value = fetch_authorized_json(&request).await?;
        let value = serde_wasm_bindgen::from_value(value)
            .map_err(|_| crate::js_error::invalid_input("invalid paykit setup status response"))?;
        let validated = validate_paykit_setup_status_response_for_tests(value)
            .map_err(crate::js_error::invalid_input)?;
        to_plain_js_value(&validated)
            .map_err(|_| crate::js_error::invalid_input("invalid paykit setup status response"))
    }
}

impl Creator {
    pub(crate) fn new(session: Session) -> Self {
        Self { session }
    }

    #[cfg(any(test, target_arch = "wasm32"))]
    #[cfg_attr(target_arch = "wasm32", allow(dead_code))]
    pub(crate) fn build_register_guarded_resource_request(
        &self,
        options: &RegisterGuardedResourceOptions,
    ) -> JsAuthorizedRequestPlan {
        let request = self.session.inner().creator().register_guarded_resource(
            locks_sdk::RegisterGuardedResourceRequest {
                path: options.path.clone(),
                content_type: options.content_type.clone(),
                bytes: options.bytes.clone(),
            },
        );
        self.authorized_request_plan(request)
    }

    #[cfg(any(test, target_arch = "wasm32"))]
    #[cfg_attr(target_arch = "wasm32", allow(dead_code))]
    pub(crate) fn build_delete_guarded_resource_request(
        &self,
        options: &DeleteGuardedResourceOptions,
    ) -> JsAuthorizedRequestPlan {
        let request = self.session.inner().creator().delete_guarded_resource(
            locks_sdk::DeleteGuardedResourceRequest {
                path: options.path.clone(),
            },
        );
        self.authorized_request_plan(request)
    }

    #[cfg(any(test, target_arch = "wasm32"))]
    pub(crate) fn build_delete_content_lock_request(
        &self,
        lock_id: &str,
        options: Option<&DeleteContentLockOptions>,
    ) -> Result<JsAuthorizedRequestPlan, String> {
        let lock_id = LockId::from_str(lock_id).map_err(|err| format!("invalid lock id: {err}"))?;
        let mode = match options.map(|options| options.mode) {
            None | Some(DeleteContentLockMode::DefaultGraceful) => {
                locks_sdk::DeleteContentLockMode::DefaultGraceful
            }
            Some(DeleteContentLockMode::ExplicitGraceful) => {
                locks_sdk::DeleteContentLockMode::ExplicitGraceful
            }
            Some(DeleteContentLockMode::Force) => locks_sdk::DeleteContentLockMode::Force,
        };
        Ok(self.authorized_request_plan(
            self.session
                .inner()
                .creator()
                .delete_content_lock(locks_sdk::DeleteContentLockRequest { lock_id, mode }),
        ))
    }

    #[cfg(any(test, target_arch = "wasm32"))]
    pub(crate) fn build_content_lock_deletion_status_request(
        &self,
        lock_id: &str,
    ) -> Result<JsAuthorizedRequestPlan, String> {
        let lock_id = LockId::from_str(lock_id).map_err(|err| format!("invalid lock id: {err}"))?;
        Ok(self.authorized_request_plan(
            self.session
                .inner()
                .creator()
                .get_content_lock_deletion(lock_id),
        ))
    }

    #[cfg(any(test, target_arch = "wasm32"))]
    #[cfg_attr(target_arch = "wasm32", allow(dead_code))]
    pub(crate) fn build_set_lock_service_pointer_request(
        &self,
        options: &SetLockServicePointerOptions,
    ) -> Result<JsAuthorizedRequestPlan, String> {
        let default_lock_server = LockServerPubky::from_str(&options.default_lock_server)
            .map_err(|err| format!("invalid default lock server pubky: {err}"))?;
        let request = self.session.inner().creator().set_lock_service_pointer(
            locks_sdk::SetLockServicePointerRequest {
                default_lock_server,
            },
        );
        Ok(self.authorized_request_plan(request))
    }

    #[cfg(any(test, target_arch = "wasm32"))]
    #[cfg_attr(target_arch = "wasm32", allow(dead_code))]
    pub(crate) fn build_paykit_setup_status_request(&self) -> JsAuthorizedRequestPlan {
        self.authorized_request_plan(self.session.inner().creator().paykit_setup_status())
    }

    #[cfg(any(test, target_arch = "wasm32"))]
    #[cfg_attr(target_arch = "wasm32", allow(dead_code))]
    pub(crate) fn create_build_content_lock_request(
        &self,
        body: serde_json::Value,
    ) -> Result<JsAuthorizedRequestPlan, String> {
        let request = serde_json::from_value::<locks_sdk::CreateContentLockRequest>(body)
            .map_err(|err| format!("invalid content lock request: {err}"))?;
        Ok(self
            .authorized_request_plan(self.session.inner().creator().create_content_lock(request)))
    }

    #[cfg(any(test, target_arch = "wasm32"))]
    fn authorized_request_plan(&self, request: locks_sdk::SdkRequest) -> JsAuthorizedRequestPlan {
        let content_type = if request.content_type.is_empty() {
            None
        } else {
            Some(request.content_type)
        };
        JsAuthorizedRequestPlan {
            method: request.method,
            path: request.path.clone(),
            url: self.session.transport_url(&request.path),
            authorization: request.authorization,
            content_type,
            body: match request.body {
                locks_sdk::SdkRequestBody::Bytes(bytes) => JsRequestBody::Bytes(bytes),
                locks_sdk::SdkRequestBody::Json(body) => JsRequestBody::Json(body),
                locks_sdk::SdkRequestBody::Empty => JsRequestBody::Empty,
            },
        }
    }
}

#[cfg(any(test, target_arch = "wasm32"))]
fn validate_paykit_setup_status_response_for_tests(
    value: serde_json::Value,
) -> Result<serde_json::Value, String> {
    let status = locks_sdk::CreatorLocks::parse_paykit_setup_status_response(value)
        .map_err(|_| "invalid paykit setup status response".to_owned())?;
    serde_json::to_value(status).map_err(|_| "invalid paykit setup status response".to_owned())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_session() -> Session {
        let lock_server =
            LockServerPubky::from_str("pubky7ir1ttte48bcp4zjychjyscicrwi1j34mtt91ptsafdbjmr8g9eo")
                .unwrap();
        let client = locks_sdk::LocksClient::for_server(lock_server);
        Session::new(
            client.restore_session("frontend-session-secret"),
            client,
            crate::locks::LocksOptions::new(),
        )
    }

    fn complete_builder() -> CreateContentLockRequestBuilder {
        let builder = CreateContentLockRequestBuilder::new();
        builder.state.borrow_mut().criteria = Some(serde_json::json!([]));
        builder.state.borrow_mut().lock_logic = Some(serde_json::json!({
            "type": "all",
            "criteria": []
        }));
        builder.state.borrow_mut().access_policy = Some(serde_json::json!({
            "requested_credential_ttl_seconds": 900
        }));
        builder.state.borrow_mut().lock_server = Some(serde_json::json!({
            "override": null
        }));
        builder
    }

    fn resource(path: &str, hash: &str, size: u64) -> serde_json::Value {
        serde_json::json!({
            "path": path,
            "hash": hash,
            "content_type": "text/plain",
            "size": size
        })
    }

    #[test]
    fn register_guarded_resource_request_builds_raw_put() {
        let creator = Creator::new(test_session());
        let options = RegisterGuardedResourceOptions::new(
            "images/example file.txt".to_owned(),
            "text/plain".to_owned(),
            b"guarded bytes".to_vec(),
        );

        let request = creator.build_register_guarded_resource_request(&options);

        assert_eq!(request.method, "PUT");
        assert_eq!(
            request.path,
            "/creator/priv-resources/content/images/example%20file.txt"
        );
        assert_eq!(request.authorization, "Bearer frontend-session-secret");
        assert_eq!(request.content_type, Some("text/plain".to_owned()));
        assert_eq!(
            request.body,
            JsRequestBody::Bytes(b"guarded bytes".to_vec())
        );
    }

    #[test]
    fn delete_guarded_resource_request_builds_delete() {
        let creator = Creator::new(test_session());
        let options = DeleteGuardedResourceOptions::new("images/example file.txt".to_owned());

        let request = creator.build_delete_guarded_resource_request(&options);

        assert_eq!(request.method, "DELETE");
        assert_eq!(
            request.path,
            "/creator/priv-resources/content/images/example%20file.txt"
        );
        assert_eq!(request.authorization, "Bearer frontend-session-secret");
        assert_eq!(request.content_type, None);
    }

    #[test]
    fn content_lock_deletion_requests_delegate_to_closed_rust_sdk_routes() {
        let creator = Creator::new(test_session());
        let lock_id = LockId::from_hash(locks_core::ids::LockHash::from_bytes([9; 32]));

        let graceful = creator
            .build_delete_content_lock_request(&lock_id.to_string(), None)
            .unwrap();
        assert_eq!(graceful.method, "DELETE");
        assert_eq!(graceful.path, format!("/creator/content-locks/{lock_id}"));
        assert_eq!(graceful.authorization, "Bearer frontend-session-secret");

        let explicit_graceful = creator
            .build_delete_content_lock_request(
                &lock_id.to_string(),
                Some(&DeleteContentLockOptions::new(
                    DeleteContentLockMode::ExplicitGraceful,
                )),
            )
            .unwrap();
        assert_eq!(
            explicit_graceful.path,
            format!("/creator/content-locks/{lock_id}?graceful=true")
        );

        let force = creator
            .build_delete_content_lock_request(
                &lock_id.to_string(),
                Some(&DeleteContentLockOptions::new(DeleteContentLockMode::Force)),
            )
            .unwrap();
        assert_eq!(
            force.path,
            format!("/creator/content-locks/{lock_id}?force=true")
        );

        let status = creator
            .build_content_lock_deletion_status_request(&lock_id.to_string())
            .unwrap();
        assert_eq!(status.method, "GET");
        assert_eq!(
            status.path,
            format!("/creator/content-locks/{lock_id}/deletion")
        );
    }

    #[test]
    fn create_content_lock_request_builder_primary_only_build_succeeds() {
        let builder = complete_builder();
        let primary = resource("/priv/locks.app/content/example.txt", "hash", 13);
        builder.state.borrow_mut().primary_resource = Some(primary.clone());

        let body = builder.build_value().unwrap();

        assert_eq!(body["primary_resource"], primary);
        assert!(body.get("secondary_resources").is_none());
        assert!(body.get("creator").is_none());
    }

    #[test]
    fn create_content_lock_request_builder_secondary_only_build_succeeds() {
        let builder = complete_builder();
        builder
            .add_secondary_resource(resource(
                "/priv/locks.app/content/secondary.txt",
                "secondary-hash",
                7,
            ))
            .unwrap();

        let body = builder.build_value().unwrap();

        assert!(body.get("primary_resource").is_none());
        assert_eq!(
            body["secondary_resources"]["/priv/locks.app/content/secondary.txt"],
            serde_json::json!({
                "hash": "secondary-hash",
                "content_type": "text/plain",
                "size": 7
            })
        );
    }

    #[test]
    fn create_content_lock_request_builder_primary_and_secondary_build_succeeds() {
        let builder = complete_builder();
        builder.state.borrow_mut().primary_resource = Some(resource(
            "/priv/locks.app/content/example.txt",
            "primary-hash",
            13,
        ));
        builder
            .add_secondary_resource(resource(
                "/priv/locks.app/content/secondary.txt",
                "secondary-hash",
                7,
            ))
            .unwrap();

        let body = builder.build_value().unwrap();

        assert!(body.get("primary_resource").is_some());
        assert!(body["secondary_resources"].is_object());
    }

    #[test]
    fn create_content_lock_request_builder_rejects_no_resources() {
        let builder = complete_builder();

        let err = builder.build_value().unwrap_err();

        assert!(format!("{err:?}").contains("at least one resource"));
    }

    #[test]
    fn create_content_lock_request_builder_rejects_missing_non_resource_fields() {
        let builder = CreateContentLockRequestBuilder::new();
        builder.state.borrow_mut().primary_resource =
            Some(resource("/priv/locks.app/content/example.txt", "hash", 13));

        let err = builder.build_value().unwrap_err();

        assert!(format!("{err:?}").contains("criteria"));
    }

    #[test]
    fn create_content_lock_request_builder_rejects_invalid_paykit_payment_in() {
        let builder = complete_builder();
        builder.state.borrow_mut().primary_resource =
            Some(resource("/priv/locks.app/content/example.txt", "hash", 13));
        builder.state.borrow_mut().criteria = Some(serde_json::json!([{
            "criterion_id": "payment",
            "verifier_type": "paykit-payment",
            "params": {
                "recipient_pubky": "pubkytkrq8zmwb8a3m9k15csu3q17qmfgqnp9dskbrg9uq1rydpyxp7qy",
                "amount": "50000",
                "asset": "BTC",
                "payment_in": 0
            }
        }]));
        builder.state.borrow_mut().lock_logic = Some(serde_json::json!({
            "type": "all",
            "criteria": ["payment"]
        }));

        let err = builder.build_value().unwrap_err();

        assert!(err.contains("payment_in"));
    }

    #[test]
    fn create_content_lock_request_builder_rejects_duplicate_secondary_path() {
        let builder = complete_builder();
        let secondary = resource("/priv/locks.app/content/secondary.txt", "hash", 7);
        builder.add_secondary_resource(secondary.clone()).unwrap();

        let err = builder.add_secondary_resource(secondary).unwrap_err();

        assert!(format!("{err:?}").contains("duplicate secondary resource path"));
    }

    #[test]
    fn create_content_lock_request_builder_rejects_primary_secondary_duplicate_path() {
        let builder = complete_builder();
        builder.state.borrow_mut().primary_resource = Some(resource(
            "/priv/locks.app/content/example.txt",
            "primary-hash",
            13,
        ));

        let err = builder
            .add_secondary_resource(resource(
                "/priv/locks.app/content/example.txt",
                "secondary-hash",
                7,
            ))
            .unwrap_err();

        assert!(format!("{err:?}").contains("also appears as secondary"));
    }

    #[test]
    fn create_content_lock_request_builder_build_snapshots_do_not_mutate() {
        let builder = complete_builder();
        let primary = resource("/priv/locks.app/content/example.txt", "hash", 13);
        builder.state.borrow_mut().primary_resource = Some(primary.clone());
        let first = builder.build_value().unwrap();
        builder
            .add_secondary_resource(resource(
                "/priv/locks.app/content/secondary.txt",
                "secondary-hash",
                7,
            ))
            .unwrap();
        let second = builder.build_value().unwrap();

        assert_eq!(first["primary_resource"], primary);
        assert!(first.get("secondary_resources").is_none());
        assert!(second["secondary_resources"].is_object());
    }

    #[test]
    fn set_lock_service_pointer_request_body_omits_creator() {
        let creator = Creator::new(test_session());
        let options = SetLockServicePointerOptions::new(
            "pubky7ir1ttte48bcp4zjychjyscicrwi1j34mtt91ptsafdbjmr8g9eo".to_owned(),
        );

        let request = creator
            .build_set_lock_service_pointer_request(&options)
            .unwrap();

        assert_eq!(request.method, "POST");
        assert_eq!(request.path, "/creator/lock-service-config");
        assert_eq!(request.authorization, "Bearer frontend-session-secret");
        let JsRequestBody::Json(body) = request.body else {
            panic!("set lock service pointer request should be JSON");
        };
        assert_eq!(
            body,
            serde_json::json!({
                "default_lock_server": "pubky7ir1ttte48bcp4zjychjyscicrwi1j34mtt91ptsafdbjmr8g9eo"
            })
        );
        assert!(body.get("creator").is_none());
    }

    #[test]
    fn paykit_setup_status_request_uses_current_session_without_creator_argument() {
        let creator = Creator::new(test_session());

        let request = creator.build_paykit_setup_status_request();

        assert_eq!(request.method, "GET");
        assert_eq!(request.path, "/creator/paykit/setup-status");
        assert_eq!(request.authorization, "Bearer frontend-session-secret");
        assert_eq!(request.content_type, None);
        assert_eq!(request.body, JsRequestBody::Empty);
    }

    #[test]
    fn paykit_setup_status_response_projects_closed_plain_status() {
        for status in ["ready", "setup_required", "unavailable"] {
            assert_eq!(
                validate_paykit_setup_status_response_for_tests(serde_json::json!({
                    "status": status
                }))
                .unwrap(),
                serde_json::json!({ "status": status })
            );
        }

        for invalid in [
            serde_json::json!({ "status": "future" }),
            serde_json::json!({ "status": "ready", "extra": true }),
            serde_json::json!({}),
        ] {
            assert!(validate_paykit_setup_status_response_for_tests(invalid).is_err());
        }
    }

    #[test]
    fn create_content_lock_request_body_omits_creator() {
        let creator = Creator::new(test_session());
        let body = serde_json::json!({
            "primary_resource": {
                "path": "/priv/locks.app/content/example.txt",
                "hash": "0W3GE1R70W3GE1R70W3GE1R70W3GE1R70W3GE1R70W3GE1R70W3G",
                "content_type": "text/plain",
                "size": 13
            },
            "criteria": [],
            "lock_logic": { "type": "all", "criteria": [] },
            "access_policy": { "requested_credential_ttl_seconds": 3600 },
            "lock_server": { "override": null }
        });

        let request = creator
            .create_build_content_lock_request(body.clone())
            .unwrap();

        assert_eq!(request.method, "POST");
        assert_eq!(request.path, "/creator/content-locks");
        assert_eq!(request.authorization, "Bearer frontend-session-secret");
        assert_eq!(request.body, JsRequestBody::Json(body.clone()));
        assert!(body.get("creator").is_none());
    }
}
