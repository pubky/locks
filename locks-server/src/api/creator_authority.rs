use axum::Json;
use axum::extract::rejection::{JsonRejection, QueryRejection};
use axum::extract::{Path, Query, State};
use axum::http::{HeaderMap, HeaderValue, StatusCode, Uri, header};
use axum::response::{Html, IntoResponse, Response};
use locks_service::application::use_cases::complete_creator_connect_flow::{
    CompleteCreatorConnectFlowRequest, SelectedCreatorConnectFlowClient,
    complete_creator_connect_flow,
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
use crate::config::{CreatorAuthorityAcquisitionMethod, RuntimeEnvironment};

fn connect_allowed_return_origins(state: &AppState) -> &[String] {
    let acquisition = &state.config().creator_authority_acquisition;
    match acquisition.method {
        CreatorAuthorityAcquisitionMethod::LegacyConnect => {
            &acquisition.legacy_connect.allowed_return_origins
        }
        CreatorAuthorityAcquisitionMethod::GrantConnect => {
            &acquisition
                .grant_connect
                .as_ref()
                .expect("validated grant-connect config")
                .allowed_return_origins
        }
    }
}

pub(super) async fn creator_authority_status_route(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<Json<CreatorAuthorityStatusHttpResponse>, ApiError> {
    let session_token = parse_frontend_session_token(&headers)?;
    let status = get_creator_authority_status(
        state.frontend_sessions().as_ref(),
        state.creator_authority_manager().as_ref(),
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
    let allowed_origins = connect_allowed_return_origins(&state);
    let return_to = validate_return_to_url(&query.return_to, allowed_origins)?;
    let delivery = ConnectDeliveryMode::from_query(query.delivery.as_deref());
    let callback_state = query.state.clone();
    // Guaranteed `Some` because `validate_return_to_url` already parsed the origin.
    let target_origin = origin_from_return_to(&return_to).ok_or_else(invalid_return_to_url)?;

    let response = start_creator_connect_flow(
        state.creator_connect_flows().as_ref(),
        state.legacy_creator_connect_flow_client().as_ref(),
        state
            .grant_creator_connect_flow_client()
            .map(|client| client.as_ref()),
        state.creator_connect_flow_id_generator().as_ref(),
        state.clock().as_ref(),
        StartCreatorConnectFlowRequest {
            return_to,
            state: query.state,
        },
    )
    .await?;

    let html = render_connect_shell_html(
        response.flow_id.as_str(),
        response.authorization_url.expose_url(),
        state.config().runtime.environment,
        delivery,
        &target_origin,
        &callback_state,
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
.qr-badge{position:absolute;left:50%;top:50%;transform:translate(-50%,-50%);width:39px;height:39px;display:flex;align-items:center;justify-content:center;border-radius:999px;background:#05050a}
.qr-badge svg{width:14.37px;height:21.94px}
.signer-logos{display:flex;align-items:center;gap:14px}
.ring-logo{width:101px;height:22px}
.bitkit-logo{width:75px;height:22px}
.signer-logos svg{display:block;width:100%;height:100%}
.caption{font-size:12px;line-height:16px;letter-spacing:1.2px;text-transform:uppercase;color:#89898f;text-align:center}
.approve-btn{width:100%;padding:12px 16px;border:0;border-radius:8px;background:#c8ff00;color:#05050a;font:inherit;font-weight:700;font-size:14px;cursor:pointer}
/* Touch devices swap the scannable QR and the signer logos for one deep-link button per signer app
   (Figma "Pubky / Dialog / LocksActivate", mobile). Keyed on the primary pointer, not viewport
   width: the shell renders inside a small parent modal iframe, so a width breakpoint would always
   read as "narrow" and show the buttons even on desktop. `pointer:coarse` reflects the real device
   (touch phone/tablet = has a signer app -> deep link; mouse desktop = scan QR), is unaffected by the
   iframe's box size, and is the query pubky.app sizes this frame with; a different query would put
   the 192px QR in the frame's 132px button box. */
.desc-use,.authorize-btn{display:none}
.authorize-btn{align-self:stretch;align-items:center;justify-content:center;gap:8px;height:60px;padding:20px 32px;border:0;border-radius:9999px;background:#303034;box-shadow:0 1px 2px rgba(5,5,10,.2);color:#d4d4db;font:inherit;font-weight:700;font-size:14px;line-height:20px;text-decoration:none}
.authorize-btn svg{display:block;width:20px;height:20px}
@media (pointer:coarse){.scan{gap:12px}.qr,.signer-logos,.desc-scan{display:none}.desc-use{display:inline}.authorize-btn{display:flex}}
/* Embedded, the parent modal draws the signer logos below this frame, so the shell's own copy would
   be a second one. */
body.embed .signer-logos,body.embed .caption{display:none}
body.embed{padding:0}
body.embed .scan{width:100%}
"#;

/// Pubky brand mark (Figma "Pubky Brand Mark"), drawn on the QR center badge and in the Pubky Ring
/// button. Decorative; the QR uses high error correction so it stays scannable under the badge.
const PUBKY_MARK_PATH: &str = r##"<path d="M7.18262 6.54395C10.2212 6.54395 12.6846 9.00635 12.6846 12.0439C12.6845 13.3581 12.2231 14.5634 11.4561 15.5078L13.5605 21.9424H0.804688L2.91016 15.5078C2.14295 14.5634 1.68073 13.3583 1.68066 12.0439C1.68066 9.00641 4.14415 6.54405 7.18262 6.54395ZM7.18262 9.13379C5.5751 9.13389 4.27246 10.4373 4.27246 12.0439C4.27255 13.1573 4.8977 14.1251 5.81738 14.6143L5.91211 14.6641L4.37891 19.3525H9.98633L8.45312 14.6641L8.54785 14.6143C9.4677 14.1251 10.0937 13.1574 10.0938 12.0439C10.0938 10.4372 8.79022 9.13379 7.18262 9.13379ZM7.18555 0.00292969L8.91895 2.27539L11.1553 0.868164L11.7705 3.23828L14.3652 2.69336L12.0049 6.54785C10.7182 5.41849 9.03154 4.73408 7.18555 4.7334H7.18066C5.33438 4.73395 3.6472 5.41828 2.36035 6.54785L0 2.69336L2.59473 3.23828L3.21094 0.868164L5.44629 2.27539L7.18262 0L7.18555 0.00292969Z" fill="white"/>"##;

/// Bitkit brand mark inside the Bitkit button (Figma "Bitkit Brand Mark").
const BITKIT_MARK_SVG: &str = r##"<svg viewBox="0 -1.84 20 20" fill="none" aria-hidden="true"><path d="M14.1943 11.2158C14.1943 15.1315 11.02 18.3058 7.1043 18.3058C3.1886 18.3058 0.0142969 15.1315 0.0142969 11.2158C0.0142969 7.30008 3.1886 4.12578 7.1043 4.12578C11.02 4.12578 14.1943 7.30008 14.1943 11.2158ZM7.1043 16.5858C10.0701 16.5858 12.4743 14.1816 12.4743 11.2158C12.4743 8.25001 10.0701 5.84578 7.1043 5.84578C4.13853 5.84578 1.7343 8.25001 1.7343 11.2158C1.7343 14.1816 4.13853 16.5858 7.1043 16.5858Z" fill-rule="evenodd" clip-rule="evenodd" fill="black"/><path d="M6.36 7.20692L8.30406 0H15.1318C17.6762 0 19.98 1.93956 19.98 4.5728C19.98 5.54895 19.7999 6.59339 19.2333 7.49633C19.1697 7.59776 19.1022 7.69563 19.0311 7.78989C19.1091 7.89987 19.1822 8.01473 19.2503 8.13453C19.7969 9.09652 19.9108 10.1909 19.8072 11.2454C19.6917 12.4199 19.2165 13.5604 18.3092 14.4125C17.3907 15.2752 16.1593 15.72 14.7693 15.72H10.8958L11.5448 13.5581C11.6685 13.1458 11.7351 12.7098 11.7351 12.2568C11.7351 10.1062 10.2029 8.21468 7.9984 7.63657L6.36 7.20692ZM17.7468 8.89824C17.6902 8.80539 17.6275 8.71678 17.5586 8.63252C17.2369 8.23872 16.7805 7.94017 16.1776 7.74957L16.1773 7.74947C16.1718 7.74775 16.1664 7.74605 16.1609 7.74435C16.1596 7.74392 16.1582 7.7435 16.1569 7.74308C16.156 7.74281 16.1551 7.74254 16.1543 7.74227C16.1471 7.74161 16.1416 7.73565 16.1416 7.72851C16.1416 7.72481 16.1431 7.72143 16.1455 7.71893C16.1477 7.71662 16.1508 7.71507 16.1543 7.71475C16.1551 7.71456 16.156 7.71436 16.1569 7.71416C16.1591 7.71367 16.1613 7.71317 16.1635 7.71267C16.7914 7.56983 17.2616 7.28357 17.5984 6.88796C17.6654 6.80925 17.7271 6.7262 17.7837 6.6391C18.141 6.08951 18.2956 5.37827 18.2956 4.5728C18.2956 2.99495 16.8791 1.68007 15.1318 1.68007H9.59514L8.42668 6.01172C8.45264 6.01853 8.47854 6.02548 8.50438 6.03257C11.4002 6.69798 13.4195 9.0757 13.4195 12.2568C13.4195 12.2912 13.4192 12.3256 13.4186 12.3599C13.4091 12.9417 13.319 13.505 13.1584 14.0399H14.7693C14.8135 14.0399 14.8572 14.0393 14.9005 14.0381C16.862 13.9843 17.9696 12.7217 18.1308 11.0815C18.2164 10.2109 18.0976 9.47333 17.7468 8.89824Z" fill-rule="evenodd" clip-rule="evenodd" fill="black"/><path d="M17.5586 8.63259C17.6275 8.71684 17.6902 8.80546 17.7468 8.89831C18.0976 9.4734 18.2164 10.211 18.1308 11.0815C17.9696 12.7218 16.862 13.9844 14.9005 14.0382C14.8572 14.0394 14.8135 14.04 14.7693 14.04H13.1584C13.319 13.505 13.4091 12.9418 13.4186 12.3599C13.4192 12.3256 13.4195 12.2913 13.4195 12.2569C13.4195 9.07577 11.4002 6.69804 8.50438 6.03264C8.47855 6.02554 8.45265 6.01859 8.42669 6.01178L9.59515 1.68013H15.1318C16.8791 1.68013 18.2956 2.99501 18.2956 4.57286C18.2956 5.37833 18.141 6.08958 17.7837 6.63916C17.7271 6.72627 17.6654 6.80931 17.5984 6.88802C17.2616 7.28363 16.7914 7.56989 16.1635 7.71273L16.1596 7.71362L16.1569 7.71423L16.1543 7.71481C16.1508 7.71513 16.1477 7.71668 16.1455 7.719C16.1431 7.72149 16.1416 7.72488 16.1416 7.72857C16.1416 7.73571 16.1471 7.74167 16.1543 7.74233L16.1569 7.74314L16.1609 7.74441C16.1664 7.74611 16.1718 7.74782 16.1773 7.74953L16.1776 7.74963C16.7805 7.94023 17.2369 8.23878 17.5586 8.63259Z" fill="white"/><path d="M12.4743 11.2158C12.4743 14.1816 10.0701 16.5858 7.10429 16.5858C4.13852 16.5858 1.73429 14.1816 1.73429 11.2158C1.73429 8.25001 4.13852 5.84578 7.10429 5.84578C10.0701 5.84578 12.4743 8.25001 12.4743 11.2158Z" fill="#FF4400"/></svg>"##;

/// Bitkit wordmark beside the Pubky Ring one below the QR (Figma "BITKIT Logo").
const BITKIT_LOGO_SVG: &str = r##"<svg viewBox="0 0 75 22" fill="none" aria-hidden="true"><path d="M74.9903 0.000183556L22.9949 0.000186815L22.5084 1.8022C21.4136 0.679118 19.8469 0.000186815 18.1961 0.000186815H9.96767L8.62031 5.00593C8.57814 5.00531 8.5359 5.005 8.49359 5.005C3.80271 5.005 0 8.80624 0 13.4953C0 18.1844 3.80271 21.9856 8.49359 21.9856C11.0898 21.9856 13.414 20.8448 14.972 19.0099H44.3662L44.6341 17.9404L45.4557 19.0099H66.3747L69.0104 8.49048H72.8413L74.9903 0.000183556ZM16.2514 16.9571H17.7592C17.8124 16.9571 17.8651 16.9564 17.9174 16.955C18.0933 16.9501 18.2635 16.9372 18.428 16.9166C18.7808 16.8724 19.1072 16.793 19.4066 16.6823C19.8295 16.526 20.1988 16.3074 20.5132 16.0381C20.6374 15.9317 20.7531 15.8173 20.8601 15.6958L20.5132 16.9808H25.3674L28.4294 4.73801L29.0246 6.46135H32.1138L29.2739 16.9808H34.1281L36.7638 6.46135L42.3344 6.46116L40.3576 7.38654L37.9277 16.9807H42.782L43.698 13.3245L46.4682 16.9806L53.4998 16.9808L48.0432 10.1191L55.8618 2.02913H49.8868L45.4594 6.46116L46.5282 2.02913H24.5496L22.4465 9.81958C22.4539 9.8473 22.461 9.87521 22.468 9.9033C22.5846 10.3714 22.6496 10.8915 22.6496 11.4394C22.6496 11.5724 22.6458 11.7038 22.6383 11.8332C22.6339 11.9101 22.6282 11.9862 22.6212 12.0615C22.6206 12.0685 22.6199 12.0754 22.6192 12.0823C22.6102 12.1766 22.5993 12.2698 22.5864 12.3615C22.5854 12.3687 22.5844 12.3759 22.5834 12.383C22.5723 12.4602 22.5599 12.5363 22.5463 12.6114C22.5111 12.8049 22.4674 12.9913 22.416 13.1692C22.2128 13.873 21.8893 14.443 21.4956 14.7879C21.6857 14.3015 21.8094 13.7404 21.8435 13.1368C21.8546 12.941 21.8559 12.748 21.8479 12.559C21.8425 12.4295 21.8328 12.302 21.8189 12.1768L21.8186 12.1737C21.8178 12.167 21.8171 12.1604 21.8163 12.1537L21.8107 12.1746C21.81 12.1679 21.8092 12.1613 21.8084 12.1547L21.808 12.1515C21.7922 12.0194 21.771 11.8909 21.7442 11.7663C21.72 11.6543 21.6914 11.5453 21.6582 11.4394C21.5802 11.191 21.477 10.9599 21.3475 10.7472C21.2793 10.635 21.2037 10.528 21.1208 10.4263C21.0628 10.3552 21.0013 10.2867 20.936 10.2208C20.7682 10.0513 20.576 9.89909 20.3587 9.76512C20.0953 9.60275 19.795 9.46714 19.4564 9.35985L19.456 9.35974C19.4495 9.35766 19.4429 9.3556 19.4363 9.35355L19.4283 9.35104C19.4196 9.35024 19.413 9.34304 19.413 9.33442C19.413 9.32996 19.4148 9.32587 19.4177 9.32286C19.4204 9.32006 19.4241 9.31818 19.4283 9.3178L19.4314 9.31709L19.4394 9.31529C19.8989 9.21054 20.2883 9.04194 20.6142 8.81869C20.825 8.67425 21.0092 8.50693 21.1687 8.31923C21.2495 8.2241 21.3239 8.12374 21.3922 8.01846C21.8227 7.35472 22.0089 6.49578 22.0089 5.52306C22.0089 5.13327 21.9375 4.75677 21.8059 4.40443C21.2939 3.03417 19.8711 2.02913 18.1961 2.02913H11.5236L10.6485 5.28065C14.2958 6.23406 16.9872 9.5505 16.9872 13.4953C16.9872 14.7283 16.7242 15.8999 16.2514 16.9571ZM8.07073 7.04774L7.6248 8.70446L9.59931 9.22338C12.2561 9.9216 14.1026 12.2061 14.1026 14.8035C14.1026 15.3507 14.0223 15.8772 13.8732 16.3752L13.5184 17.5599C12.3333 19.0219 10.5226 19.9565 8.49359 19.9565C4.9238 19.9565 2.02991 17.0637 2.02991 13.4953C2.02991 10.0689 4.69804 7.26545 8.07073 7.04774ZM55.8683 2.02913H72.3829L71.2611 6.46135H67.4262L64.7905 16.9808H59.9363L62.7762 6.46135H59.5886L59.0278 4.73801L55.9764 16.9808H53.5268L52.1058 13.3371L54.4317 4.72192L55.8683 2.02913Z" fill-rule="evenodd" clip-rule="evenodd" fill="black"/><path d="M45.4594 6.4614L49.8868 2.02913H55.8618L48.0432 10.1192L51.7094 14.7503L53.4998 16.9806L46.4682 16.9806L43.6981 13.3246L42.782 16.9806H37.9277L40.3576 7.3867L42.3344 6.4614L36.7638 6.46151L34.1281 16.9806H29.2739L32.1138 6.46151H29.0246L28.4294 4.73818L25.3674 16.9806H20.5132L20.8601 15.6959C20.1746 16.4747 19.134 16.9806 17.7592 16.9806H15.8178C16.0227 16.2964 16.1325 15.5506 16.1325 14.8037C16.1325 11.2044 13.5845 8.17279 10.1155 7.2611L11.5236 2.02913H18.1961C20.3019 2.02913 22.0089 3.61754 22.0089 5.52322C22.0089 7.4289 21.294 8.89786 19.4283 9.31796C19.4241 9.31834 19.4204 9.32021 19.4177 9.32301C19.4148 9.32603 19.413 9.33011 19.413 9.33458C19.413 9.3432 19.4196 9.3504 19.4283 9.3512C20.8969 9.8069 21.6581 10.7687 21.8163 12.1539C21.8525 12.469 21.8627 12.7988 21.8435 13.137C21.8094 13.7405 21.6857 14.3017 21.4956 14.788C22.1782 14.1901 22.6496 12.9154 22.6496 11.4396C22.6496 10.8588 22.5766 10.3092 22.4465 9.81974L24.5496 2.02913H46.5282L45.4594 6.4614ZM53.5268 16.9806H55.9764L59.0278 4.73818L59.5886 6.46151H62.7762L59.9363 16.9806H64.7905L67.4262 6.46151H71.2611L72.3829 2.02913H72.4626H55.8683L54.4317 4.72209L52.1058 13.3373L53.5268 16.9806Z" fill-rule="evenodd" clip-rule="evenodd" fill="white"/><ellipse cx="8.49359" cy="13.4951" rx="6.46368" ry="6.46116" fill="#FF4400"/></svg>"##;

/// Pubky Ring wordmark below the QR, beside the Bitkit one in grant mode (Figma "Pubky Ring Logo").
const RING_LOGO_SVG: &str = r##"<svg viewBox="0 0 146.88 32.0036" fill="none" aria-hidden="true"><path d="M78.5564 25.9317H83.6893L78.6323 19.029L82.8321 14.1075L87.7657 25.7294L87.3611 26.7661C87.1588 27.3055 86.9144 27.7185 86.6278 28.005C86.3413 28.3085 85.8777 28.4602 85.2371 28.4602C85.0348 28.4602 84.8073 28.4349 84.5544 28.3843C84.3184 28.3506 84.0993 28.3 83.897 28.2326L83.4419 31.7219C83.7453 31.8062 84.0993 31.8736 84.5039 31.9241C84.9253 31.9747 85.3299 32 85.7176 32C86.5098 32 87.1925 31.9073 87.7657 31.7219C88.3557 31.5365 88.8614 31.2583 89.2828 30.8875C89.7211 30.5335 90.0919 30.0952 90.3954 29.5727C90.7156 29.067 91.0022 28.4855 91.2551 27.8281L96.8937 13.34H92.494L89.9908 21.4311H89.9149L87.1083 13.34H83.487L83.4871 13.34H78.4553L74.4349 18.4475H74.359V6.81658H70.1869V25.9317H74.359V19.8887H74.4349L78.5564 25.9317Z" fill="#fff"/><path d="M68.8351 19.5853C68.8351 20.4618 68.7003 21.3046 68.4306 22.1137C68.1608 22.9228 67.7647 23.6392 67.2421 24.2629C66.7364 24.8697 66.1127 25.3586 65.371 25.7294C64.6293 26.1003 63.7865 26.2857 62.8425 26.2857C61.9996 26.2857 61.1989 26.1171 60.4404 25.78C59.6987 25.426 59.1255 24.9287 58.721 24.2882H58.6704V25.9317H54.8523V6.81658H58.9991V14.6801H59.0497C59.4037 14.2587 59.9009 13.871 60.5415 13.517C61.1821 13.163 61.9659 12.986 62.893 12.986C63.8033 12.986 64.6209 13.163 65.3457 13.517C66.0874 13.871 66.7111 14.3514 67.2168 14.9582C67.7394 15.565 68.1356 16.273 68.4053 17.0821C68.6918 17.8744 68.8351 18.7087 68.8351 19.5853ZM64.84 19.5853C64.84 19.1807 64.7726 18.7846 64.6377 18.3969C64.5197 18.0092 64.3343 17.6721 64.0815 17.3855C63.8286 17.0821 63.5168 16.8377 63.1459 16.6523C62.775 16.4668 62.3452 16.3741 61.8563 16.3741C61.3844 16.3741 60.9629 16.4668 60.5921 16.6523C60.2212 16.8377 59.9009 17.0821 59.6312 17.3855C59.3784 17.6889 59.1761 18.0345 59.0244 18.4222C58.8895 18.8099 58.8221 19.206 58.8221 19.6106C58.8221 20.0151 58.8895 20.4112 59.0244 20.7989C59.1761 21.1866 59.3784 21.5322 59.6312 21.8356C59.9009 22.139 60.2212 22.3834 60.5921 22.5688C60.9629 22.7543 61.3844 22.847 61.8563 22.847C62.3452 22.847 62.775 22.7543 63.1459 22.5688C63.5168 22.3834 63.8286 22.139 64.0815 21.8356C64.3343 21.5322 64.5197 21.1866 64.6377 20.7989C64.7726 20.3944 64.84 19.9898 64.84 19.5853Z" fill="#fff"/><path d="M48.8712 25.9317V24.1871H48.8206C48.6689 24.4737 48.4666 24.7434 48.2138 24.9962C47.9778 25.249 47.6912 25.4682 47.3541 25.6536C47.0338 25.839 46.6714 25.9907 46.2668 26.1087C45.8791 26.2267 45.4661 26.2857 45.0278 26.2857C44.185 26.2857 43.4686 26.134 42.8786 25.8306C42.3054 25.5272 41.8334 25.1395 41.4626 24.6675C41.1086 24.1787 40.8473 23.6308 40.6787 23.024C40.527 22.4003 40.4512 21.7682 40.4512 21.1277V13.34H44.6233V20.1669C44.6233 20.5208 44.6485 20.858 44.6991 21.1782C44.7497 21.4985 44.8424 21.7851 44.9772 22.0379C45.129 22.2908 45.3228 22.493 45.5588 22.6447C45.7948 22.7796 46.1067 22.847 46.4944 22.847C47.2192 22.847 47.7671 22.5773 48.1379 22.0379C48.5256 21.4985 48.7195 20.8664 48.7195 20.1416V13.34H52.8663V25.9317H48.8712Z" fill="#fff"/><path d="M39.8115 13.5423C39.8115 14.5874 39.6177 15.4723 39.23 16.1971C38.8423 16.9051 38.3197 17.4782 37.6623 17.9165C37.0049 18.3548 36.2463 18.675 35.3866 18.8773C34.5269 19.0796 33.6335 19.1807 32.7063 19.1807H30.5571V25.9317H26.2333V8.03024H32.8075C33.7852 8.03024 34.6955 8.13138 35.5383 8.33366C36.398 8.51908 37.1397 8.83092 37.7634 9.26918C38.404 9.69059 38.9013 10.2553 39.2553 10.9632C39.6261 11.6544 39.8115 12.514 39.8115 13.5423ZM35.4877 13.5676C35.4877 13.1461 35.4035 12.8006 35.2349 12.5309C35.0663 12.2612 34.8387 12.0505 34.5522 11.8988C34.2656 11.7471 33.9369 11.6459 33.566 11.5954C33.2121 11.5448 32.8412 11.5195 32.4535 11.5195H30.5571V15.742H32.3776C32.7822 15.742 33.1699 15.7083 33.5408 15.6409C33.9116 15.5735 34.2403 15.4555 34.5269 15.2869C34.8303 15.1183 35.0663 14.8992 35.2349 14.6295C35.4035 14.3429 35.4877 13.989 35.4877 13.5676Z" fill="#fff"/><path d="M135.721 28.3126C136.193 28.9193 136.833 29.4334 137.642 29.8547C138.468 30.2761 139.336 30.4867 140.246 30.4867C141.089 30.4867 141.805 30.3603 142.395 30.1075C142.985 29.8716 143.457 29.5429 143.811 29.1216C144.182 28.7002 144.451 28.203 144.62 27.63C144.788 27.0738 144.873 26.4755 144.873 25.8351V23.6104H144.822C144.367 24.3519 143.718 24.925 142.875 25.3295C142.05 25.7339 141.207 25.9362 140.347 25.9362C139.42 25.9362 138.578 25.7845 137.819 25.4811C137.078 25.1778 136.437 24.7648 135.898 24.2424C135.375 23.7031 134.963 23.0626 134.659 22.321C134.373 21.5795 134.229 20.7789 134.229 19.9194C134.229 19.0767 134.373 18.2845 134.659 17.543C134.963 16.8014 135.375 16.1525 135.898 15.5963C136.437 15.0402 137.078 14.602 137.819 14.2817C138.578 13.9615 139.42 13.8014 140.347 13.8014C141.207 13.8014 142.05 14.0037 142.875 14.4081C143.701 14.8126 144.35 15.3941 144.822 16.1525H144.873V14.1048H146.592V25.8351C146.592 26.4755 146.499 27.1581 146.314 27.8828C146.145 28.6075 145.816 29.2733 145.328 29.88C144.856 30.4867 144.207 30.9923 143.381 31.3968C142.555 31.8013 141.485 32.0036 140.17 32.0036C139.075 32.0036 138.03 31.7929 137.036 31.3716C136.041 30.9502 135.19 30.3688 134.482 29.6272L135.721 28.3126ZM136.05 19.8688C136.05 20.4924 136.151 21.0823 136.353 21.6385C136.555 22.1946 136.842 22.6834 137.213 23.1047C137.6 23.5261 138.072 23.8632 138.628 24.116C139.184 24.3519 139.816 24.4699 140.524 24.4699C141.182 24.4699 141.788 24.3604 142.345 24.1413C142.901 23.9222 143.381 23.6104 143.786 23.2059C144.19 22.8014 144.502 22.321 144.721 21.7649C144.957 21.1918 145.075 20.5598 145.075 19.8688C145.075 19.2452 144.957 18.6553 144.721 18.0991C144.502 17.543 144.19 17.0542 143.786 16.6329C143.381 16.2115 142.901 15.8744 142.345 15.6216C141.788 15.3688 141.182 15.2424 140.524 15.2424C139.816 15.2424 139.184 15.3688 138.628 15.6216C138.072 15.8744 137.6 16.2115 137.213 16.6329C136.842 17.0542 136.555 17.543 136.353 18.0991C136.151 18.6553 136.05 19.2452 136.05 19.8688Z" fill="#fff"/><path d="M122.953 14.1048C122.986 14.425 123.012 14.7789 123.028 15.1666C123.045 15.5542 123.054 15.8744 123.054 16.1272H123.104C123.441 15.4362 123.989 14.8801 124.748 14.4587C125.506 14.0205 126.307 13.8014 127.149 13.8014C128.649 13.8014 129.77 14.248 130.512 15.1413C131.27 16.0345 131.649 17.2143 131.649 18.6806V25.9362H129.93V19.3885C129.93 18.7649 129.88 18.2003 129.778 17.6946C129.677 17.189 129.5 16.7593 129.248 16.4053C129.012 16.0345 128.683 15.748 128.262 15.5458C127.857 15.3435 127.343 15.2424 126.719 15.2424C126.264 15.2424 125.818 15.3351 125.38 15.5205C124.958 15.7059 124.579 15.9924 124.242 16.38C123.905 16.7508 123.635 17.2312 123.433 17.8211C123.231 18.3941 123.13 19.0767 123.13 19.8688V25.9362H121.41V16.7087C121.41 16.3885 121.402 15.9756 121.385 15.4699C121.368 14.9643 121.343 14.5093 121.309 14.1048H122.953Z" fill="#fff"/><path d="M117.846 25.9362H116.126V14.1048H117.846V25.9362ZM118.199 9.55422C118.199 9.90815 118.073 10.2031 117.82 10.439C117.567 10.6581 117.289 10.7677 116.986 10.7677C116.683 10.7677 116.405 10.6581 116.152 10.439C115.899 10.2031 115.772 9.90815 115.772 9.55422C115.772 9.20029 115.899 8.91377 116.152 8.69467C116.405 8.45872 116.683 8.34074 116.986 8.34074C117.289 8.34074 117.567 8.45872 117.82 8.69467C118.073 8.91377 118.199 9.20029 118.199 9.55422Z" fill="#fff"/><path d="M103.9 25.9362H102.08V8.03738H107.338C108.215 8.03738 109.024 8.12165 109.765 8.29019C110.524 8.45873 111.173 8.73682 111.712 9.12446C112.268 9.49524 112.698 9.98401 113.001 10.5907C113.305 11.1975 113.456 11.9391 113.456 12.8155C113.456 13.4728 113.338 14.0711 113.102 14.6104C112.866 15.1497 112.546 15.6216 112.142 16.0261C111.737 16.4138 111.257 16.7256 110.701 16.9615C110.145 17.1975 109.538 17.3491 108.881 17.4166L114.114 25.9362H111.889L106.934 17.5682H103.9V25.9362ZM103.9 16.0008H107.06C108.527 16.0008 109.656 15.7396 110.448 15.2171C111.24 14.6778 111.636 13.8772 111.636 12.8155C111.636 12.2256 111.527 11.7284 111.307 11.3239C111.088 10.9194 110.785 10.5907 110.397 10.3379C110.01 10.0851 109.538 9.89974 108.982 9.78176C108.425 9.66378 107.81 9.60479 107.136 9.60479H103.9V16.0008Z" fill="#fff"/><path d="M8.48926 7.73047C12.0802 7.73067 14.9912 10.64 14.9912 14.2285C14.9912 15.7813 14.4458 17.2055 13.5391 18.3213L16.0264 25.9229H0.951172L3.43945 18.3213C2.53263 17.2055 1.98636 15.7814 1.98633 14.2285C1.98633 10.6399 4.89818 7.73047 8.48926 7.73047ZM8.48926 10.791C6.58933 10.791 5.04883 12.3303 5.04883 14.2285C5.04886 15.544 5.78881 16.6868 6.87598 17.2646L6.9873 17.3242L5.1748 22.8623H11.8027L9.99023 17.3242L10.1016 17.2646C11.1887 16.6868 11.9287 15.544 11.9287 14.2285C11.9287 12.3304 10.389 10.7912 8.48926 10.791ZM8.49121 0.00292969V0.00390625L10.541 2.68848L13.1836 1.02539L13.9111 3.82617L16.9775 3.18262L14.1885 7.73633C12.6676 6.4018 10.6733 5.59246 8.49121 5.5918H8.48633C6.30428 5.59245 4.31089 6.40182 2.79004 7.73633L0 3.18262L3.06738 3.82617L3.79395 1.02539L6.43652 2.68848L8.48926 0L8.49121 0.00292969Z" fill="#fff"/></svg>"##;

const PUBKY_RING_ANDROID_PACKAGE: &str = "to.pubky.ring";
const BITKIT_ANDROID_DEVELOPMENT_PACKAGE: &str = "to.bitkit.dev";
const BITKIT_ANDROID_PRODUCTION_PACKAGE: &str = "to.bitkit";

fn bitkit_android_package(environment: RuntimeEnvironment) -> &'static str {
    match environment {
        RuntimeEnvironment::Development | RuntimeEnvironment::Staging => {
            BITKIT_ANDROID_DEVELOPMENT_PACKAGE
        }
        RuntimeEnvironment::Production => BITKIT_ANDROID_PRODUCTION_PACKAGE,
    }
}

/// Each signer button opens its own app. Android gets an `intent://` link naming the app's package,
/// because the plain `pubkyauth://` link opens whichever app the chooser or a saved default picks;
/// Firefox for Android keeps the plain link, as its `intent://` handling is less reliable. Bitkit iOS
/// registers only `bitkit://`. Embedded on iOS, Mobile Safari ignores a custom-scheme navigation of
/// this frame itself, so the links target the top frame, which a user tap may send to the app.
const SIGNER_LINKS_SCRIPT: &str = r#"<script>
    {
      const userAgent = navigator.userAgent;
      const isIos = /iPhone|iPad|iPod/i.test(userAgent) || (/Macintosh/i.test(userAgent) && navigator.maxTouchPoints > 1);
      const isAndroid = /Android/i.test(userAgent) && !/Firefox\//.test(userAgent);
      const isEmbedded = document.body.classList.contains("embed");
      for (const button of document.querySelectorAll(".authorize-btn")) {
        const href = isIos ? button.dataset.iosHref : isAndroid ? button.dataset.androidHref : undefined;
        if (href) {
          button.href = href;
        }
        if (isIos && isEmbedded) {
          button.target = "_top";
        }
      }
    }
  </script>"#;

fn render_connect_shell_html(
    flow_id: &str,
    authorization_url: &str,
    environment: RuntimeEnvironment,
    delivery: ConnectDeliveryMode,
    target_origin: &str,
    callback_state: &str,
) -> String {
    let escaped_flow_id = escape_html(flow_id);
    let qr_svg = render_authorization_qr_svg(authorization_url);
    let badge_mark_svg = format!(
        r#"<svg viewBox="0 0 14.3656 21.9429" fill="none" aria-hidden="true">{PUBKY_MARK_PATH}</svg>"#
    );
    // Figma draws the mark 18.33px tall at the top of the 20px icon box.
    let button_mark_svg = format!(
        r#"<svg viewBox="0 0 20 20" fill="none" aria-hidden="true"><g transform="translate(3.9985 0) scale(0.83551)">{PUBKY_MARK_PATH}</g></svg>"#
    );
    let ring_link = render_authorize_button(
        &button_mark_svg,
        "Continue with Pubky Ring",
        authorization_url,
        android_intent_url(authorization_url, PUBKY_RING_ANDROID_PACKAGE),
        None,
    );
    // Bitkit accepts only grant auth, so a legacy link names only Pubky Ring.
    let (bitkit_link, bitkit_logo, bitkit_copy) = match bitkit_signin_url(authorization_url) {
        Some(ios_url) => (
            render_authorize_button(
                BITKIT_MARK_SVG,
                "Continue with Bitkit",
                authorization_url,
                android_intent_url(authorization_url, bitkit_android_package(environment)),
                Some(ios_url),
            ),
            format!(r#"<span class="bitkit-logo">{BITKIT_LOGO_SVG}</span>"#),
            " or <strong>Bitkit</strong>",
        ),
        None => (String::new(), String::new(), ""),
    };
    // Desktop: a plain (non-interactive) QR to scan from a separate device. Touch devices swap it for
    // the deep-link buttons below — the same-device deep link lives only on those buttons, so the QR
    // itself is not a link (per the team decision: desktop QR is scan-only).
    let scan = format!(
        r#"<div class="scan">
      <span class="qr" data-testid="pubky-auth-qr">
        {qr_svg}
        <span class="qr-badge">{badge_mark_svg}</span>
      </span>
      <span class="signer-logos" aria-hidden="true">
        <span class="ring-logo">{RING_LOGO_SVG}</span>{bitkit_logo}
      </span>{ring_link}{bitkit_link}
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
            let script = render_postmessage_script(flow_id, target_origin, callback_state);
            format!(
                r#"<!doctype html>
<html lang="en">
{head}
<body class="embed">
  {scan}
  {SIGNER_LINKS_SCRIPT}
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
    <p class="desc"><span class="desc-scan">Scan this QR with </span><span class="desc-use">Use </span><strong>Pubky Ring</strong>{bitkit_copy} to authorize Locks server to manage your data. Use the same pubky you signed in with.</p>
    {scan}
    <form method="post" action="/connect/{escaped_flow_id}/complete">
      <button class="approve-btn" type="submit">I approved this connection</button>
    </form>
  </main>
  {SIGNER_LINKS_SCRIPT}
</body>
</html>"#
        ),
    }
}

/// Shell JS for postmessage delivery. Long-polls `POST /complete` (the server blocks until Ring
/// approval). Before approval the endpoint is effectively idempotent (the pending flow still
/// exists), so transient failures — a dropped connection or a gateway timeout from a proxy that
/// capped the idle long-poll — are retried with capped exponential backoff. Application-level
/// creator-authority failures and other definitive errors are surfaced as a closed
/// `{ type, state, error }` message so the embedder is never left hanging. On success it posts
/// `{ type, state, code }` and stops.
fn render_postmessage_script(flow_id: &str, target_origin: &str, callback_state: &str) -> String {
    let flow_id_js = js_string_literal(flow_id);
    let target_origin_js = js_string_literal(target_origin);
    let type_js = js_string_literal(POSTMESSAGE_CALLBACK_TYPE);
    let resize_type_js = js_string_literal(POSTMESSAGE_RESIZE_TYPE);
    let callback_state_js = js_string_literal(callback_state);
    format!(
        r#"<script>
    (async () => {{
      const TARGET_ORIGIN = {target_origin_js};
      const CALLBACK_TYPE = {type_js};
      const RESIZE_TYPE = {resize_type_js};
      const CALLBACK_STATE = {callback_state_js};
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
      // A non-JSON 503 remains retryable as a likely proxy response, while the stable Locks
      // creator-authority error is terminal because its one-shot approval may be consumed.
      const RETRYABLE = new Set([408, 425, 429, 502, 503, 504]);
      const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));
      const post = (payload) => window.parent.postMessage(payload, TARGET_ORIGIN);
      const isTerminalApplicationError = async (res) => {{
        if (res.status !== 503) return false;
        try {{
          const error = await res.clone().json();
          return error?.error?.code === "creator_authority_unavailable";
        }} catch (_e) {{
          return false;
        }}
      }};
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
            post({{ type: CALLBACK_TYPE, state: CALLBACK_STATE, error: "invalid-response" }});
          }}
          return;
        }}
        if (await isTerminalApplicationError(res)) {{
          post({{ type: CALLBACK_TYPE, state: CALLBACK_STATE, error: "connect-failed" }});
          return;
        }}
        if (RETRYABLE.has(res.status)) {{
          await backoff();
          continue;
        }}
        // Definitive server error (flow expired/rejected) — tell the parent instead of hanging.
        post({{ type: CALLBACK_TYPE, state: CALLBACK_STATE, error: "connect-failed" }});
        return;
      }}
    }})();
    </script>"#
    )
}

/// A signer app's deep-link button. `href` is the plain `pubkyauth://` link; `SIGNER_LINKS_SCRIPT`
/// swaps in the platform link when the button has one.
fn render_authorize_button(
    icon_svg: &str,
    label: &str,
    href: &str,
    android_href: Option<String>,
    ios_href: Option<String>,
) -> String {
    let mut attributes = format!(r#"href="{}""#, escape_html(href));
    if let Some(url) = android_href {
        attributes.push_str(&format!(r#" data-android-href="{}""#, escape_html(&url)));
    }
    if let Some(url) = ios_href {
        attributes.push_str(&format!(r#" data-ios-href="{}""#, escape_html(&url)));
    }
    format!(
        r#"
      <a class="authorize-btn" {attributes}>
        {icon_svg}
        <span>{label}</span>
      </a>"#
    )
}

/// Bitkit's iOS link for a grant sign-in: `bitkit://pubky-auth/signin` with the same query, which
/// Bitkit maps back to `pubkyauth://signin_grant`. Legacy `pubkyauth://signin` links get none,
/// because Bitkit accepts only grant auth.
fn bitkit_signin_url(authorization_url: &str) -> Option<String> {
    authorization_url
        .strip_prefix("pubkyauth://signin_grant?")
        .filter(|query| !query.is_empty())
        .map(|query| format!("bitkit://pubky-auth/signin?{query}"))
}

/// Android `intent://` form of a grant sign-in naming the signer app's package, the same rule as
/// pubky-app's `toAndroidAuthIntentUrl`: everything after `pubkyauth://` is kept byte for byte, and
/// a link with a fragment or whitespace gets none, so it cannot carry its own `#Intent;package=…`.
/// Legacy `pubkyauth://signin` links get none and keep the plain link.
pub(super) fn android_intent_url(authorization_url: &str, package: &str) -> Option<String> {
    authorization_url
        .strip_prefix("pubkyauth://")
        .filter(|rest| {
            rest.strip_prefix("signin_grant?")
                .is_some_and(|query| !query.is_empty())
                && !rest.contains(|c: char| c == '#' || c.is_whitespace())
        })
        .map(|rest| format!("intent://{rest}#Intent;scheme=pubkyauth;package={package};end"))
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
    let connect_client = match state.grant_creator_connect_flow_client() {
        Some(client) => SelectedCreatorConnectFlowClient::Grant(client.as_ref()),
        None => SelectedCreatorConnectFlowClient::Legacy(
            state.legacy_creator_connect_flow_client().as_ref(),
        ),
    };
    let response = complete_creator_connect_flow(
        state.creator_connect_flows().as_ref(),
        state.creator_authorities().as_ref(),
        state.frontend_session_codes().as_ref(),
        connect_client,
        state.frontend_session_code_generator().as_ref(),
        state.clock().as_ref(),
        CompleteCreatorConnectFlowRequest {
            flow_id: locks_service::application::models::CreatorConnectFlowId::new(flow_id),
        },
    )
    .await?;
    // Validate even in postmessage mode: the shell already targets this origin, but re-checking
    // keeps the allowlist the single source of truth for both delivery paths.
    let return_to =
        validate_return_to_url(&response.return_to, connect_allowed_return_origins(&state))?;

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
