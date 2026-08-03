use axum::Json;
use axum::extract::rejection::{JsonRejection, QueryRejection};
use axum::extract::{Path, Query, State};
use axum::http::{HeaderMap, HeaderValue, StatusCode, Uri, header};
use axum::response::{Html, IntoResponse, Response};
use locks_service::application::use_cases::complete_creator_connect_flow::{
    CompleteCreatorConnectFlowRequest, complete_creator_connect_flow,
};
use locks_service::application::use_cases::exchange_frontend_session_code::exchange_frontend_session_code;
use locks_service::application::use_cases::get_creator_authority_status::{
    GetCreatorAuthorityStatusRequest, get_creator_authority_status,
};
use locks_service::application::use_cases::start_creator_connect_flow::{
    StartCreatorConnectFlowRequest, start_creator_connect_flow,
};
use locks_service::application::use_cases::validate_frontend_session::{
    ValidateFrontendSessionRequest, validate_frontend_session,
};
use qrcode::QrCode;
use qrcode::render::svg;
use serde::{Deserialize, Serialize};

use crate::api::auth::parse_frontend_session_token;
use crate::api::dtos::{
    CreatorAuthorityStatusHttpResponse, ExchangeFrontendSessionCodeHttpRequest,
    ExchangeFrontendSessionCodeHttpResponse,
};
use crate::api::errors::{ApiError, ApiErrorCode};
use crate::api::extractors::parse_json;
use crate::app_state::AppState;

pub(super) async fn creator_authority_status_route(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<Json<CreatorAuthorityStatusHttpResponse>, ApiError> {
    let session_token = parse_frontend_session_token(&headers)?;
    let status = get_creator_authority_status(
        state.frontend_sessions().as_ref(),
        state.creator_authorities().as_ref(),
        state.clock().as_ref(),
        GetCreatorAuthorityStatusRequest { session_token },
    )
    .await?;

    Ok(Json(CreatorAuthorityStatusHttpResponse::from(status)))
}

/// Postmessage message `type` published to embedding apps. Embedders MUST validate this string.
const POSTMESSAGE_CALLBACK_TYPE: &str = "locks-auth-callback";

/// Postmessage `type` the embed shell posts on load and whenever its content height changes, so the
/// embedder can size the iframe to fit (QR panel vs the shorter mobile Authorize button). Payload:
/// `{ type, height }` where `height` is CSS pixels. Embedders MAY ignore it and keep a fixed height.
const POSTMESSAGE_RESIZE_TYPE: &str = "locks-auth-resize";

/// Delivery mode for the connect result. `Redirect` is the default so existing consumers are
/// unaffected; `PostMessage` is opt-in via `?delivery=postmessage` and posts `{ state, code }` to
/// the embedding parent window instead of navigating anywhere.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ConnectDeliveryMode {
    Redirect,
    PostMessage,
}

impl ConnectDeliveryMode {
    fn from_query(delivery: Option<&str>) -> Self {
        match delivery {
            Some("postmessage") => Self::PostMessage,
            _ => Self::Redirect,
        }
    }
}

#[derive(Debug, Deserialize)]
pub(super) struct ConnectShellStartQuery {
    return_to: String,
    state: String,
    delivery: Option<String>,
}

pub(super) async fn connect_shell_start(
    State(state): State<AppState>,
    query: Result<Query<ConnectShellStartQuery>, QueryRejection>,
) -> Result<Response, ApiError> {
    let Query(query) =
        query.map_err(|_| ApiError::new(ApiErrorCode::InvalidRequest, "invalid request"))?;
    let allowed_origins = &state
        .config()
        .creator_authority_acquisition
        .legacy_connect
        .allowed_return_origins;
    let return_to = validate_return_to_url(&query.return_to, allowed_origins)?;
    let delivery = ConnectDeliveryMode::from_query(query.delivery.as_deref());
    // Guaranteed `Some` because `validate_return_to_url` already parsed the origin.
    let target_origin = origin_from_return_to(&return_to).ok_or_else(invalid_return_to_url)?;

    let response = start_creator_connect_flow(
        state.creator_connect_flows().as_ref(),
        state.legacy_creator_connect_flow_client().as_ref(),
        state.creator_connect_flow_id_generator().as_ref(),
        state.clock().as_ref(),
        StartCreatorConnectFlowRequest {
            return_to,
            state: query.state,
        },
    )
    .await?;

    // Local dev only: the shell shows the Pubky authorization URL solely as a QR, which a desktop
    // tester without a phone cannot use. Emit the secret-bearing URL to the server log so it can be
    // opened directly. Gated to Development so it never lands in a shared staging/production log.
    if state.config().runtime.environment.is_development() {
        tracing::info!(
            authorization_url = response.authorization_url.expose_url(),
            "dev: legacy-connect authorization URL"
        );
    }

    let html = render_connect_shell_html(
        response.flow_id.as_str(),
        response.authorization_url.expose_url(),
        delivery,
        &target_origin,
    );

    // Scope framing to this flow's validated return origin — the exact origin that will also
    // receive the postMessage. Narrower than the full allowlist: a different allowlisted app
    // cannot frame a shell scoped to another app's origin.
    let csp = HeaderValue::from_str(&format!("frame-ancestors {target_origin}"))
        .map_err(|_| ApiError::new(ApiErrorCode::InvalidRequest, "invalid return_to"))?;
    Ok(([(header::CONTENT_SECURITY_POLICY, csp)], Html(html)).into_response())
}

/// Self-contained styles for the connect shell (Figma "Dialog / Lock / EnableLocks"). Inline because
/// the shell must load with no external CSS/font/image (CSP + iframe embedding).
///
/// Two layouts: `body.embed` (postmessage) renders ONLY the QR panel + caption on a transparent
/// background — the embedding app (pubky.app) already provides the card, title, description, and
/// close. Plain `body` (redirect / standalone full page) draws its own card chrome.
const SHELL_CSS: &str = r#"
*{box-sizing:border-box}
body{margin:0;min-height:100vh;display:flex;flex-direction:column;align-items:center;justify-content:center;padding:16px;color:#fff;font-family:'Inter Tight',Inter,system-ui,-apple-system,sans-serif}
body:not(.embed){background:#05050a}
/* Embedded: paint the card colour (base/card) so the iframe is seamless with the parent modal.
   A transparent body makes browsers fall back to a white iframe canvas. Keep the base centering
   (fill + center) so the content sits centered inside the embedder's iframe — embedders give it a
   fixed height (e.g. pubky.app's 420px), so top-aligning would float the QR near the top. */
body.embed{background:#1d1d20}
.card{width:100%;max-width:400px;display:flex;flex-direction:column;gap:24px;padding:32px;background:#1d1d20;border-top:1px solid #c8ff00;border-bottom:1px solid #c8ff00;border-radius:16px;box-shadow:0 50px 100px 0 rgba(5,5,10,.75)}
.title{margin:0;font-size:24px;line-height:32px;font-weight:700}
.desc{margin:0;font-size:16px;line-height:24px;letter-spacing:-0.5px;color:#d4d4db}
.desc strong{color:#fff;font-weight:700}
.scan{display:flex;flex-direction:column;align-items:center;gap:24px}
.qr{position:relative;display:block;width:192px;height:192px;padding:10px;background:#fff;border-radius:8px;overflow:hidden}
.qr>svg{width:172px;height:172px;display:block}
.qr-badge{position:absolute;left:50%;top:50%;transform:translate(-50%,-50%);width:40px;height:40px;display:flex;align-items:center;justify-content:center;border-radius:999px;background:#05050a}
.qr-badge svg{width:20px;height:20px}
.caption{font-size:12px;line-height:16px;letter-spacing:1.2px;text-transform:uppercase;color:#89898f;text-align:center}
.approve-btn{width:100%;padding:12px 16px;border:0;border-radius:8px;background:#c8ff00;color:#05050a;font:inherit;font-weight:700;font-size:14px;cursor:pointer}
/* Touch devices swap the scannable QR for a Pubky Ring wordmark + Authorize deep-link button
   (Figma "Breakpoint=Mobile, Step=PubkyRing"). Keyed on the primary input (hover/pointer), not
   viewport width: the shell renders inside a small parent modal iframe, so a width breakpoint would
   always read as "narrow" and show the button even on desktop. `hover:none`+`pointer:coarse` reflect
   the real device (touch phone/tablet = has Pubky Ring app -> deep link; mouse desktop = scan QR)
   and are unaffected by the iframe's box size. */
.ring-logo,.authorize-btn{display:none}
.ring-logo{width:146.88px;height:32px}
.ring-logo svg{display:block;width:100%;height:100%}
.authorize-btn{align-items:center;justify-content:center;gap:8px;width:100%;height:60px;padding:20px 32px;border:1px solid #c8ff00;border-radius:9999px;background:rgba(200,255,0,.16);color:#c8ff00;font:inherit;font-weight:700;font-size:14px;line-height:20px;text-decoration:none;box-shadow:0 1px 1px 0 rgba(5,5,10,.2)}
.authorize-btn svg{display:block;width:16px;height:16px}
@media (hover:none) and (pointer:coarse){.qr{display:none}.ring-logo{display:block}.authorize-btn{display:flex}}
"#;

/// Padlock glyph shown at the QR center (decorative; QR uses high error correction so it stays
/// scannable under the badge).
const LOCK_SVG: &str = r##"<svg viewBox="0 0 24 24" fill="none" aria-hidden="true"><path d="M8 10V7a4 4 0 0 1 8 0v3" stroke="#fff" stroke-width="2" stroke-linecap="round"/><rect x="5.5" y="10" width="13" height="9.5" rx="2.5" fill="#fff"/></svg>"##;

/// Pubky Ring wordmark shown above the mobile authorize button (Figma "Pubky Ring Logo").
const RING_LOGO_SVG: &str = r##"<svg viewBox="0 0 146.88 32.0036" fill="none" aria-hidden="true"><path d="M78.5564 25.9317H83.6893L78.6323 19.029L82.8321 14.1075L87.7657 25.7294L87.3611 26.7661C87.1588 27.3055 86.9144 27.7185 86.6278 28.005C86.3413 28.3085 85.8777 28.4602 85.2371 28.4602C85.0348 28.4602 84.8073 28.4349 84.5544 28.3843C84.3184 28.3506 84.0993 28.3 83.897 28.2326L83.4419 31.7219C83.7453 31.8062 84.0993 31.8736 84.5039 31.9241C84.9253 31.9747 85.3299 32 85.7176 32C86.5098 32 87.1925 31.9073 87.7657 31.7219C88.3557 31.5365 88.8614 31.2583 89.2828 30.8875C89.7211 30.5335 90.0919 30.0952 90.3954 29.5727C90.7156 29.067 91.0022 28.4855 91.2551 27.8281L96.8937 13.34H92.494L89.9908 21.4311H89.9149L87.1083 13.34H83.487L83.4871 13.34H78.4553L74.4349 18.4475H74.359V6.81658H70.1869V25.9317H74.359V19.8887H74.4349L78.5564 25.9317Z" fill="#fff"/><path d="M68.8351 19.5853C68.8351 20.4618 68.7003 21.3046 68.4306 22.1137C68.1608 22.9228 67.7647 23.6392 67.2421 24.2629C66.7364 24.8697 66.1127 25.3586 65.371 25.7294C64.6293 26.1003 63.7865 26.2857 62.8425 26.2857C61.9996 26.2857 61.1989 26.1171 60.4404 25.78C59.6987 25.426 59.1255 24.9287 58.721 24.2882H58.6704V25.9317H54.8523V6.81658H58.9991V14.6801H59.0497C59.4037 14.2587 59.9009 13.871 60.5415 13.517C61.1821 13.163 61.9659 12.986 62.893 12.986C63.8033 12.986 64.6209 13.163 65.3457 13.517C66.0874 13.871 66.7111 14.3514 67.2168 14.9582C67.7394 15.565 68.1356 16.273 68.4053 17.0821C68.6918 17.8744 68.8351 18.7087 68.8351 19.5853ZM64.84 19.5853C64.84 19.1807 64.7726 18.7846 64.6377 18.3969C64.5197 18.0092 64.3343 17.6721 64.0815 17.3855C63.8286 17.0821 63.5168 16.8377 63.1459 16.6523C62.775 16.4668 62.3452 16.3741 61.8563 16.3741C61.3844 16.3741 60.9629 16.4668 60.5921 16.6523C60.2212 16.8377 59.9009 17.0821 59.6312 17.3855C59.3784 17.6889 59.1761 18.0345 59.0244 18.4222C58.8895 18.8099 58.8221 19.206 58.8221 19.6106C58.8221 20.0151 58.8895 20.4112 59.0244 20.7989C59.1761 21.1866 59.3784 21.5322 59.6312 21.8356C59.9009 22.139 60.2212 22.3834 60.5921 22.5688C60.9629 22.7543 61.3844 22.847 61.8563 22.847C62.3452 22.847 62.775 22.7543 63.1459 22.5688C63.5168 22.3834 63.8286 22.139 64.0815 21.8356C64.3343 21.5322 64.5197 21.1866 64.6377 20.7989C64.7726 20.3944 64.84 19.9898 64.84 19.5853Z" fill="#fff"/><path d="M48.8712 25.9317V24.1871H48.8206C48.6689 24.4737 48.4666 24.7434 48.2138 24.9962C47.9778 25.249 47.6912 25.4682 47.3541 25.6536C47.0338 25.839 46.6714 25.9907 46.2668 26.1087C45.8791 26.2267 45.4661 26.2857 45.0278 26.2857C44.185 26.2857 43.4686 26.134 42.8786 25.8306C42.3054 25.5272 41.8334 25.1395 41.4626 24.6675C41.1086 24.1787 40.8473 23.6308 40.6787 23.024C40.527 22.4003 40.4512 21.7682 40.4512 21.1277V13.34H44.6233V20.1669C44.6233 20.5208 44.6485 20.858 44.6991 21.1782C44.7497 21.4985 44.8424 21.7851 44.9772 22.0379C45.129 22.2908 45.3228 22.493 45.5588 22.6447C45.7948 22.7796 46.1067 22.847 46.4944 22.847C47.2192 22.847 47.7671 22.5773 48.1379 22.0379C48.5256 21.4985 48.7195 20.8664 48.7195 20.1416V13.34H52.8663V25.9317H48.8712Z" fill="#fff"/><path d="M39.8115 13.5423C39.8115 14.5874 39.6177 15.4723 39.23 16.1971C38.8423 16.9051 38.3197 17.4782 37.6623 17.9165C37.0049 18.3548 36.2463 18.675 35.3866 18.8773C34.5269 19.0796 33.6335 19.1807 32.7063 19.1807H30.5571V25.9317H26.2333V8.03024H32.8075C33.7852 8.03024 34.6955 8.13138 35.5383 8.33366C36.398 8.51908 37.1397 8.83092 37.7634 9.26918C38.404 9.69059 38.9013 10.2553 39.2553 10.9632C39.6261 11.6544 39.8115 12.514 39.8115 13.5423ZM35.4877 13.5676C35.4877 13.1461 35.4035 12.8006 35.2349 12.5309C35.0663 12.2612 34.8387 12.0505 34.5522 11.8988C34.2656 11.7471 33.9369 11.6459 33.566 11.5954C33.2121 11.5448 32.8412 11.5195 32.4535 11.5195H30.5571V15.742H32.3776C32.7822 15.742 33.1699 15.7083 33.5408 15.6409C33.9116 15.5735 34.2403 15.4555 34.5269 15.2869C34.8303 15.1183 35.0663 14.8992 35.2349 14.6295C35.4035 14.3429 35.4877 13.989 35.4877 13.5676Z" fill="#fff"/><path d="M135.721 28.3126C136.193 28.9193 136.833 29.4334 137.642 29.8547C138.468 30.2761 139.336 30.4867 140.246 30.4867C141.089 30.4867 141.805 30.3603 142.395 30.1075C142.985 29.8716 143.457 29.5429 143.811 29.1216C144.182 28.7002 144.451 28.203 144.62 27.63C144.788 27.0738 144.873 26.4755 144.873 25.8351V23.6104H144.822C144.367 24.3519 143.718 24.925 142.875 25.3295C142.05 25.7339 141.207 25.9362 140.347 25.9362C139.42 25.9362 138.578 25.7845 137.819 25.4811C137.078 25.1778 136.437 24.7648 135.898 24.2424C135.375 23.7031 134.963 23.0626 134.659 22.321C134.373 21.5795 134.229 20.7789 134.229 19.9194C134.229 19.0767 134.373 18.2845 134.659 17.543C134.963 16.8014 135.375 16.1525 135.898 15.5963C136.437 15.0402 137.078 14.602 137.819 14.2817C138.578 13.9615 139.42 13.8014 140.347 13.8014C141.207 13.8014 142.05 14.0037 142.875 14.4081C143.701 14.8126 144.35 15.3941 144.822 16.1525H144.873V14.1048H146.592V25.8351C146.592 26.4755 146.499 27.1581 146.314 27.8828C146.145 28.6075 145.816 29.2733 145.328 29.88C144.856 30.4867 144.207 30.9923 143.381 31.3968C142.555 31.8013 141.485 32.0036 140.17 32.0036C139.075 32.0036 138.03 31.7929 137.036 31.3716C136.041 30.9502 135.19 30.3688 134.482 29.6272L135.721 28.3126ZM136.05 19.8688C136.05 20.4924 136.151 21.0823 136.353 21.6385C136.555 22.1946 136.842 22.6834 137.213 23.1047C137.6 23.5261 138.072 23.8632 138.628 24.116C139.184 24.3519 139.816 24.4699 140.524 24.4699C141.182 24.4699 141.788 24.3604 142.345 24.1413C142.901 23.9222 143.381 23.6104 143.786 23.2059C144.19 22.8014 144.502 22.321 144.721 21.7649C144.957 21.1918 145.075 20.5598 145.075 19.8688C145.075 19.2452 144.957 18.6553 144.721 18.0991C144.502 17.543 144.19 17.0542 143.786 16.6329C143.381 16.2115 142.901 15.8744 142.345 15.6216C141.788 15.3688 141.182 15.2424 140.524 15.2424C139.816 15.2424 139.184 15.3688 138.628 15.6216C138.072 15.8744 137.6 16.2115 137.213 16.6329C136.842 17.0542 136.555 17.543 136.353 18.0991C136.151 18.6553 136.05 19.2452 136.05 19.8688Z" fill="#fff"/><path d="M122.953 14.1048C122.986 14.425 123.012 14.7789 123.028 15.1666C123.045 15.5542 123.054 15.8744 123.054 16.1272H123.104C123.441 15.4362 123.989 14.8801 124.748 14.4587C125.506 14.0205 126.307 13.8014 127.149 13.8014C128.649 13.8014 129.77 14.248 130.512 15.1413C131.27 16.0345 131.649 17.2143 131.649 18.6806V25.9362H129.93V19.3885C129.93 18.7649 129.88 18.2003 129.778 17.6946C129.677 17.189 129.5 16.7593 129.248 16.4053C129.012 16.0345 128.683 15.748 128.262 15.5458C127.857 15.3435 127.343 15.2424 126.719 15.2424C126.264 15.2424 125.818 15.3351 125.38 15.5205C124.958 15.7059 124.579 15.9924 124.242 16.38C123.905 16.7508 123.635 17.2312 123.433 17.8211C123.231 18.3941 123.13 19.0767 123.13 19.8688V25.9362H121.41V16.7087C121.41 16.3885 121.402 15.9756 121.385 15.4699C121.368 14.9643 121.343 14.5093 121.309 14.1048H122.953Z" fill="#fff"/><path d="M117.846 25.9362H116.126V14.1048H117.846V25.9362ZM118.199 9.55422C118.199 9.90815 118.073 10.2031 117.82 10.439C117.567 10.6581 117.289 10.7677 116.986 10.7677C116.683 10.7677 116.405 10.6581 116.152 10.439C115.899 10.2031 115.772 9.90815 115.772 9.55422C115.772 9.20029 115.899 8.91377 116.152 8.69467C116.405 8.45872 116.683 8.34074 116.986 8.34074C117.289 8.34074 117.567 8.45872 117.82 8.69467C118.073 8.91377 118.199 9.20029 118.199 9.55422Z" fill="#fff"/><path d="M103.9 25.9362H102.08V8.03738H107.338C108.215 8.03738 109.024 8.12165 109.765 8.29019C110.524 8.45873 111.173 8.73682 111.712 9.12446C112.268 9.49524 112.698 9.98401 113.001 10.5907C113.305 11.1975 113.456 11.9391 113.456 12.8155C113.456 13.4728 113.338 14.0711 113.102 14.6104C112.866 15.1497 112.546 15.6216 112.142 16.0261C111.737 16.4138 111.257 16.7256 110.701 16.9615C110.145 17.1975 109.538 17.3491 108.881 17.4166L114.114 25.9362H111.889L106.934 17.5682H103.9V25.9362ZM103.9 16.0008H107.06C108.527 16.0008 109.656 15.7396 110.448 15.2171C111.24 14.6778 111.636 13.8772 111.636 12.8155C111.636 12.2256 111.527 11.7284 111.307 11.3239C111.088 10.9194 110.785 10.5907 110.397 10.3379C110.01 10.0851 109.538 9.89974 108.982 9.78176C108.425 9.66378 107.81 9.60479 107.136 9.60479H103.9V16.0008Z" fill="#fff"/><path d="M8.48926 7.73047C12.0802 7.73067 14.9912 10.64 14.9912 14.2285C14.9912 15.7813 14.4458 17.2055 13.5391 18.3213L16.0264 25.9229H0.951172L3.43945 18.3213C2.53263 17.2055 1.98636 15.7814 1.98633 14.2285C1.98633 10.6399 4.89818 7.73047 8.48926 7.73047ZM8.48926 10.791C6.58933 10.791 5.04883 12.3303 5.04883 14.2285C5.04886 15.544 5.78881 16.6868 6.87598 17.2646L6.9873 17.3242L5.1748 22.8623H11.8027L9.99023 17.3242L10.1016 17.2646C11.1887 16.6868 11.9287 15.544 11.9287 14.2285C11.9287 12.3304 10.389 10.7912 8.48926 10.791ZM8.49121 0.00292969V0.00390625L10.541 2.68848L13.1836 1.02539L13.9111 3.82617L16.9775 3.18262L14.1885 7.73633C12.6676 6.4018 10.6733 5.59246 8.49121 5.5918H8.48633C6.30428 5.59245 4.31089 6.40182 2.79004 7.73633L0 3.18262L3.06738 3.82617L3.79395 1.02539L6.43652 2.68848L8.48926 0L8.49121 0.00292969Z" fill="#fff"/></svg>"##;

/// KeyRound glyph inside the mobile authorize button (Figma "Icon / KeyRound", lucide key-round).
const KEY_ICON_SVG: &str = r##"<svg viewBox="0 0 16 16" fill="none" aria-hidden="true"><path d="M1.33333 11.9999V13.9999C1.33333 14.3999 1.6 14.6666 2 14.6666H4.66667V12.6666H6.66667V10.6666H8L8.93333 9.73328C9.85989 10.056 10.8686 10.0548 11.7943 9.72978C12.7201 9.40475 13.5081 8.77517 14.0296 7.94403C14.551 7.11289 14.7749 6.12939 14.6647 5.15444C14.5545 4.17948 14.1167 3.27078 13.423 2.57699C12.7292 1.8832 11.8205 1.4454 10.8455 1.3352C9.87055 1.22501 8.88706 1.44894 8.05592 1.97037C7.22478 2.4918 6.59519 3.27986 6.27017 4.20562C5.94514 5.13139 5.9439 6.14005 6.26667 7.06661L1.33333 11.9999Z" stroke="#c8ff00" stroke-width="1.33" stroke-linecap="round" stroke-linejoin="round"/><path d="M11 5.33333C11.1841 5.33333 11.3333 5.18409 11.3333 5C11.3333 4.8159 11.1841 4.66667 11 4.66667C10.8159 4.66667 10.6667 4.8159 10.6667 5C10.6667 5.18409 10.8159 5.33333 11 5.33333Z" stroke="#c8ff00" stroke-width="1.33" stroke-linecap="round" stroke-linejoin="round"/></svg>"##;

fn render_connect_shell_html(
    flow_id: &str,
    authorization_url: &str,
    delivery: ConnectDeliveryMode,
    target_origin: &str,
) -> String {
    let escaped_flow_id = escape_html(flow_id);
    let escaped_authorization_url = escape_html(authorization_url);
    let qr_svg = render_authorization_qr_svg(authorization_url);
    // Desktop: a plain (non-interactive) QR to scan from a separate device. Touch devices swap it for
    // the Authorize deep-link button below — the same-device deep link lives only on that button, so
    // the QR itself is not a link (per the team decision: desktop QR is scan-only).
    let scan = format!(
        r#"<div class="scan">
      <span class="qr" data-testid="pubky-auth-qr">
        {qr_svg}
        <span class="qr-badge">{LOCK_SVG}</span>
      </span>
      <span class="ring-logo" aria-hidden="true">{RING_LOGO_SVG}</span>
      <a class="authorize-btn" href="{escaped_authorization_url}">
        {KEY_ICON_SVG}
        <span>Authorize</span>
      </a>
      <span class="caption">LOCKS.PUBKY.APP</span>
    </div>"#
    );
    let head = format!(
        r#"<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Enable Locks</title>
  <style>{SHELL_CSS}</style>
</head>"#
    );
    match delivery {
        // Embedded in the parent app's modal: render only the QR, transparent, no card/title/close.
        ConnectDeliveryMode::PostMessage => {
            let script = render_postmessage_script(flow_id, target_origin);
            format!(
                r#"<!doctype html>
<html lang="en">
{head}
<body class="embed">
  {scan}
  {script}
</body>
</html>"#
            )
        }
        // Standalone full page: draw the card chrome the embedder would otherwise provide.
        ConnectDeliveryMode::Redirect => format!(
            r#"<!doctype html>
<html lang="en">
{head}
<body>
  <main class="card">
    <h1 class="title">Enable Locks</h1>
    <p class="desc">Use <strong>Pubky Ring</strong> to authorize Locks server to manage your Locks data.</p>
    {scan}
    <form method="post" action="/connect/{escaped_flow_id}/complete">
      <button class="approve-btn" type="submit">I approved this connection</button>
    </form>
  </main>
</body>
</html>"#
        ),
    }
}

/// Shell JS for postmessage delivery. Long-polls `POST /complete` (the server blocks until Ring
/// approval). Before approval the endpoint is effectively idempotent (the pending flow still
/// exists), so transient failures — a dropped connection or a gateway timeout from a proxy that
/// capped the idle long-poll — are retried with capped exponential backoff. A definitive error
/// (expired/rejected flow) is surfaced to the parent as `{ type, error }` so the embedder is never
/// left hanging. On success it posts `{ type, state, code }` and stops.
fn render_postmessage_script(flow_id: &str, target_origin: &str) -> String {
    let flow_id_js = js_string_literal(flow_id);
    let target_origin_js = js_string_literal(target_origin);
    let type_js = js_string_literal(POSTMESSAGE_CALLBACK_TYPE);
    let resize_type_js = js_string_literal(POSTMESSAGE_RESIZE_TYPE);
    format!(
        r#"<script>
    (async () => {{
      const TARGET_ORIGIN = {target_origin_js};
      const CALLBACK_TYPE = {type_js};
      const RESIZE_TYPE = {resize_type_js};
      const COMPLETE_URL = "/connect/" + encodeURIComponent({flow_id_js}) + "/complete?delivery=postmessage";
      // Report content height so the embedder can size the iframe to fit (QR vs mobile button).
      // Re-fires on layout changes (media-query flip, font load) via ResizeObserver.
      const reportHeight = () => window.parent.postMessage(
        {{ type: RESIZE_TYPE, height: Math.ceil(document.documentElement.getBoundingClientRect().height) }},
        TARGET_ORIGIN,
      );
      reportHeight();
      if (window.ResizeObserver) {{ new ResizeObserver(reportHeight).observe(document.documentElement); }}
      // Statuses that mean "not done yet, try again" (proxy/gateway timeouts, rate limiting).
      const RETRYABLE = new Set([408, 425, 429, 502, 503, 504]);
      const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));
      const post = (payload) => window.parent.postMessage(payload, TARGET_ORIGIN);
      let delay = 500;
      const backoff = async () => {{ await sleep(delay); delay = Math.min(delay * 2, 5000); }};
      while (true) {{
        let res;
        try {{
          res = await fetch(COMPLETE_URL, {{ method: "POST" }});
        }} catch (_e) {{
          // Connection dropped (long-poll cut before a response) — retry with backoff.
          await backoff();
          continue;
        }}
        if (res.ok) {{
          try {{
            const {{ state, code }} = await res.json();
            post({{ type: CALLBACK_TYPE, state, code }});
          }} catch (_e) {{
            post({{ type: CALLBACK_TYPE, error: "invalid-response" }});
          }}
          return;
        }}
        if (RETRYABLE.has(res.status)) {{
          await backoff();
          continue;
        }}
        // Definitive server error (flow expired/rejected) — tell the parent instead of hanging.
        post({{ type: CALLBACK_TYPE, error: "connect-failed-" + res.status }});
        return;
      }}
    }})();
    </script>"#
    )
}

/// Serializes a string as a JavaScript string literal safe to inline inside an HTML `<script>`.
/// `serde_json` handles quotes/backslashes/control chars; escaping `<`/`>` additionally prevents a
/// `</script>` (or `<!--`) sequence in a value from terminating the script element in the HTML
/// tokenizer, which JS-string escaping alone does not cover.
fn js_string_literal(value: &str) -> String {
    serde_json::to_string(value)
        .unwrap_or_else(|_| "\"\"".to_owned())
        .replace('<', "\\u003c")
        .replace('>', "\\u003e")
}

fn render_authorization_qr_svg(authorization_url: &str) -> String {
    // High error correction (~30% recovery) so the centered brand badge does not break scanning.
    QrCode::with_error_correction_level(authorization_url.as_bytes(), qrcode::EcLevel::H)
        .expect("legacy Pubky authorization URL fits QR capacity")
        .render::<svg::Color>()
        .min_dimensions(192, 192)
        // No built-in quiet zone: the white panel's padding is the margin, so the modules fill the
        // panel edge-to-edge like the design (instead of a small pattern floating in white).
        .quiet_zone(false)
        .dark_color(svg::Color("#111111"))
        .light_color(svg::Color("transparent"))
        .build()
        // Strip the XML prolog — this SVG is inlined into HTML, not served as a standalone document.
        .replace("<?xml version=\"1.0\" standalone=\"yes\"?>", "")
        .replace(
            "<svg",
            "<svg aria-label=\"Pubky authorization QR code\" role=\"img\"",
        )
}

pub(super) fn escape_html(value: &str) -> String {
    let mut escaped = String::with_capacity(value.len());
    for ch in value.chars() {
        match ch {
            '&' => escaped.push_str("&amp;"),
            '<' => escaped.push_str("&lt;"),
            '>' => escaped.push_str("&gt;"),
            '"' => escaped.push_str("&quot;"),
            '\'' => escaped.push_str("&#x27;"),
            _ => escaped.push(ch),
        }
    }
    escaped
}

#[derive(Debug, Deserialize)]
pub(super) struct ConnectShellCompleteQuery {
    delivery: Option<String>,
}

/// Postmessage-mode JSON body for `/connect/{flow_id}/complete`. Carries only the one-time `code`
/// and opaque `state` — never the secret-bearing authorization URL or session secret.
#[derive(Debug, Serialize)]
struct ConnectCompletePostMessageResponse {
    state: String,
    code: String,
}

pub(super) async fn connect_shell_complete(
    State(state): State<AppState>,
    Path(flow_id): Path<String>,
    query: Result<Query<ConnectShellCompleteQuery>, QueryRejection>,
) -> Result<Response, ApiError> {
    let Query(query) =
        query.map_err(|_| ApiError::new(ApiErrorCode::InvalidRequest, "invalid request"))?;
    let delivery = ConnectDeliveryMode::from_query(query.delivery.as_deref());
    let response = complete_creator_connect_flow(
        state.creator_connect_flows().as_ref(),
        state.creator_authorities().as_ref(),
        state.frontend_session_codes().as_ref(),
        state.legacy_creator_connect_flow_client().as_ref(),
        state.frontend_session_code_generator().as_ref(),
        state.clock().as_ref(),
        CompleteCreatorConnectFlowRequest {
            flow_id: locks_service::application::models::CreatorConnectFlowId::new(flow_id),
        },
    )
    .await?;
    // Validate even in postmessage mode: the shell already targets this origin, but re-checking
    // keeps the allowlist the single source of truth for both delivery paths.
    let return_to = validate_return_to_url(
        &response.return_to,
        &state
            .config()
            .creator_authority_acquisition
            .legacy_connect
            .allowed_return_origins,
    )?;

    if delivery == ConnectDeliveryMode::PostMessage {
        return Ok(Json(ConnectCompletePostMessageResponse {
            state: response.state,
            code: response.code.expose_code().to_owned(),
        })
        .into_response());
    }

    let location =
        append_connect_callback_params(&return_to, &response.state, response.code.expose_code());
    let location = HeaderValue::from_str(&location)
        .map_err(|_| ApiError::new(ApiErrorCode::InvalidRequest, "invalid return_to"))?;

    Ok((StatusCode::SEE_OTHER, [(header::LOCATION, location)]).into_response())
}

fn append_connect_callback_params(return_to: &str, state: &str, code: &str) -> String {
    let separator = if return_to.contains('?') { '&' } else { '?' };
    format!(
        "{return_to}{separator}state={}&code={}",
        percent_encode_query_value(state),
        percent_encode_query_value(code)
    )
}

fn percent_encode_query_value(value: &str) -> String {
    let mut encoded = String::new();
    for byte in value.bytes() {
        match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                encoded.push(byte as char);
            }
            _ => encoded.push_str(&format!("%{byte:02X}")),
        }
    }
    encoded
}

/// Exchanges a one-time frontend code for a browser session token.
pub(super) async fn exchange_frontend_session_code_route(
    State(state): State<AppState>,
    request: Result<Json<ExchangeFrontendSessionCodeHttpRequest>, JsonRejection>,
) -> Result<Json<ExchangeFrontendSessionCodeHttpResponse>, ApiError> {
    let request = parse_json(request)?;
    let response = exchange_frontend_session_code(
        state.frontend_session_codes().as_ref(),
        state.frontend_sessions().as_ref(),
        state.frontend_session_token_generator().as_ref(),
        state.clock().as_ref(),
        request.into(),
    )
    .await?;

    Ok(Json(ExchangeFrontendSessionCodeHttpResponse::from(
        response,
    )))
}

pub(super) async fn frontend_session_signout_route(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<StatusCode, ApiError> {
    let session_token = parse_frontend_session_token(&headers)?;
    validate_frontend_session(
        state.frontend_sessions().as_ref(),
        state.clock().as_ref(),
        ValidateFrontendSessionRequest {
            session_token: session_token.clone(),
        },
    )
    .await?;
    state
        .frontend_sessions()
        .delete_frontend_session(&session_token)
        .await?;

    Ok(StatusCode::NO_CONTENT)
}

fn invalid_return_to_url() -> ApiError {
    ApiError::new(ApiErrorCode::InvalidRequest, "invalid return_to")
}

pub(super) fn validate_return_to_url(
    return_to: &str,
    allowed_origins: &[String],
) -> Result<String, ApiError> {
    let origin = origin_from_return_to(return_to).ok_or_else(invalid_return_to_url)?;
    if allowed_origins
        .iter()
        .any(|allowed| allowed == "*" || allowed == &origin)
    {
        return Ok(return_to.to_owned());
    }
    Err(invalid_return_to_url())
}

fn origin_from_return_to(return_to: &str) -> Option<String> {
    let uri: Uri = return_to.parse().ok()?;
    let scheme = uri.scheme_str()?;
    if scheme != "http" && scheme != "https" {
        return None;
    }
    let authority = uri.authority()?;
    Some(format!("{scheme}://{authority}"))
}
