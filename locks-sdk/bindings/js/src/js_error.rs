use wasm_bindgen::prelude::*;

pub type JsResult<T> = Result<T, JsValue>;

#[cfg(any(test, target_arch = "wasm32"))]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ReadinessErrorKind {
    Resolution,
    Fetch,
}

#[cfg(any(test, target_arch = "wasm32"))]
pub(crate) const fn readiness_error_name(kind: ReadinessErrorKind) -> &'static str {
    match kind {
        ReadinessErrorKind::Resolution => "LockServerResolutionFailed",
        ReadinessErrorKind::Fetch => "LockServerFetchFailed",
    }
}

pub fn invalid_input(message: impl AsRef<str>) -> JsValue {
    js_sys_error("InvalidInput", message.as_ref())
}

#[cfg(target_arch = "wasm32")]
pub(crate) fn lock_server_resolution_failed() -> JsValue {
    js_sys_error(
        readiness_error_name(ReadinessErrorKind::Resolution),
        "Lock Server endpoint resolution failed",
    )
}

#[cfg(target_arch = "wasm32")]
pub(crate) fn lock_server_fetch_failed() -> JsValue {
    js_sys_error(
        readiness_error_name(ReadinessErrorKind::Fetch),
        "Lock Server readiness request failed",
    )
}

#[cfg(target_arch = "wasm32")]
pub fn paykit_data_lookup_failed() -> JsValue {
    js_sys_error("PaykitDataLookupFailed", "Paykit data lookup failed")
}

fn js_sys_error(name: &str, message: &str) -> JsValue {
    #[cfg(target_arch = "wasm32")]
    {
        let error = js_sys::Error::new(message);
        error.set_name(name);
        error.into()
    }

    #[cfg(not(target_arch = "wasm32"))]
    {
        JsValue::from_str(&format!("{name}: {message}"))
    }
}
