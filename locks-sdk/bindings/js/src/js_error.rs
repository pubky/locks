use wasm_bindgen::prelude::*;

pub type JsResult<T> = Result<T, JsValue>;

pub fn invalid_input(message: impl AsRef<str>) -> JsValue {
    js_sys_error("InvalidInput", message.as_ref())
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
