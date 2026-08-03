#[cfg(target_arch = "wasm32")]
use serde::Serialize;
use serde_json::Value;
use wasm_bindgen::JsValue;

pub(crate) fn to_plain_js_value(value: &Value) -> Result<JsValue, String> {
    value_to_plain_js_value(value)
}

#[cfg(target_arch = "wasm32")]
pub(crate) fn serializable_to_plain_js_value(value: &impl Serialize) -> Result<JsValue, String> {
    let value = serde_json::to_value(value)
        .map_err(|err| format!("failed to convert value to JSON: {err}"))?;
    value_to_plain_js_value(&value)
}

#[cfg(target_arch = "wasm32")]
fn value_to_plain_js_value(value: &Value) -> Result<JsValue, String> {
    js_sys::JSON::parse(&value.to_string())
        .map_err(|err| format!("failed to parse JSON-compatible JS object: {err:?}"))
}

#[cfg(not(target_arch = "wasm32"))]
fn value_to_plain_js_value(value: &Value) -> Result<JsValue, String> {
    serde_wasm_bindgen::to_value(value)
        .map_err(|err| format!("failed to serialize JSON-compatible JS value: {err}"))
}
