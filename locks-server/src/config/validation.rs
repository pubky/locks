use super::schema::{PkdnsConfig, RuntimeEnvironment};

/// Derives the canonical lock server origin from PKDNS config.
///
/// In development with loopback domains, produces an `http://` origin.
/// In production, produces an `https://` origin.
/// Returns `None` when the PKDNS config lacks a usable ICANN domain.
pub(crate) fn canonical_lock_server_origin(
    pkdns: &PkdnsConfig,
    environment: RuntimeEnvironment,
) -> Option<String> {
    let domain = pkdns.icann_domain.as_deref()?;
    if domain.is_empty() {
        return None;
    }
    let is_loopback = domain == "localhost"
        || domain == "127.0.0.1"
        || domain == "::1"
        || domain.ends_with(".localhost");
    let (scheme, default_port) = if is_loopback && environment.is_development() {
        ("http", 80)
    } else {
        ("https", 443)
    };
    let port = pkdns.public_icann_http_port.unwrap_or(default_port);
    if port == default_port {
        Some(format!("{scheme}://{domain}"))
    } else {
        Some(format!("{scheme}://{domain}:{port}"))
    }
}

pub(crate) fn canonical_grant_origin(value: &str) -> Option<String> {
    let parsed = url::Url::parse(value).ok()?;
    let origin = parsed.origin().ascii_serialization();
    if parsed.scheme() != "https"
        || parsed.host_str().is_none()
        || !parsed.username().is_empty()
        || parsed.password().is_some()
        || parsed.path() != "/"
        || parsed.query().is_some()
        || parsed.fragment().is_some()
        || origin != value
    {
        return None;
    }
    Some(origin)
}
