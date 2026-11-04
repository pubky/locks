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
