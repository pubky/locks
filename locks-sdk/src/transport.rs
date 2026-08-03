use std::collections::BTreeMap;

use crate::error::{LocksSdkError, Result};
use url::Url;

pub const HTTP_PORT_PARAM: u16 = pubky_common::constants::reserved_param_keys::HTTP_PORT;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BrowserEndpoint {
    pub domain: Option<String>,
    pub port: Option<u16>,
    pub params: BTreeMap<u16, u16>,
}

pub fn select_first_domain_endpoint(endpoints: &[BrowserEndpoint]) -> Result<&BrowserEndpoint> {
    endpoints
        .iter()
        .find(|endpoint| endpoint.domain.is_some())
        .ok_or(LocksSdkError::MissingBrowserDomainEndpoint)
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BrowserRequest {
    pub url: Url,
    pub pubky_host: Option<String>,
}

pub fn rewrite_browser_request(
    original_url: &str,
    endpoint: &BrowserEndpoint,
    testnet_host: Option<&str>,
) -> Result<BrowserRequest> {
    let mut url = Url::parse(original_url).map_err(|_| LocksSdkError::InvalidTransportUrl)?;
    let pubky_host = pubky_host_for_url(&url);
    apply_endpoint_to_url(&mut url, endpoint, testnet_host)?;
    Ok(BrowserRequest { url, pubky_host })
}

fn pubky_host_for_url(url: &Url) -> Option<String> {
    url.host_str()
        .and_then(|host| host.strip_prefix("_pubky."))
        .map(ToOwned::to_owned)
}

fn apply_endpoint_to_url(
    url: &mut Url,
    endpoint: &BrowserEndpoint,
    testnet_host: Option<&str>,
) -> Result<()> {
    let domain = endpoint
        .domain
        .as_deref()
        .ok_or(LocksSdkError::MissingBrowserEndpointDomain)?;
    let is_testnet_domain = domain == "localhost" || testnet_host == Some(domain);

    if is_testnet_domain {
        url.set_scheme("http")
            .map_err(|_| LocksSdkError::InvalidTransportUrl)?;
        let http_port = endpoint
            .params
            .get(&HTTP_PORT_PARAM)
            .copied()
            .ok_or(LocksSdkError::MissingHttpPortParam)?;
        url.set_port(Some(http_port))
            .map_err(|_| LocksSdkError::InvalidTransportUrl)?;
    } else if let Some(port) = endpoint.port {
        url.set_port(Some(port))
            .map_err(|_| LocksSdkError::InvalidTransportUrl)?;
    }

    url.set_host(Some(domain))
        .map_err(|_| LocksSdkError::InvalidTransportUrl)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn browser_endpoint_selection_errors_for_empty_endpoint_list() {
        let result = select_first_domain_endpoint(&[]);

        assert!(result.is_err());
    }

    #[test]
    fn browser_endpoint_selection_ignores_endpoints_without_domain() {
        let endpoints = vec![
            BrowserEndpoint {
                domain: None,
                port: Some(443),
                params: BTreeMap::new(),
            },
            BrowserEndpoint {
                domain: Some("locks.example".to_owned()),
                port: Some(8443),
                params: BTreeMap::new(),
            },
        ];

        let selected = select_first_domain_endpoint(&endpoints).unwrap();

        assert_eq!(selected.domain.as_deref(), Some("locks.example"));
        assert_eq!(selected.port, Some(8443));
    }

    #[test]
    fn browser_endpoint_selection_uses_first_endpoint_with_domain() {
        let endpoints = vec![
            BrowserEndpoint {
                domain: Some("first.example".to_owned()),
                port: Some(9443),
                params: BTreeMap::new(),
            },
            BrowserEndpoint {
                domain: Some("second.example".to_owned()),
                port: Some(8443),
                params: BTreeMap::new(),
            },
        ];

        let selected = select_first_domain_endpoint(&endpoints).unwrap();

        assert_eq!(selected.domain.as_deref(), Some("first.example"));
        assert_eq!(selected.port, Some(9443));
    }

    #[test]
    fn browser_request_rewrite_preserves_path_query_and_adds_pubky_host() {
        let endpoint = BrowserEndpoint {
            domain: Some("locks.example".to_owned()),
            port: Some(8443),
            params: BTreeMap::new(),
        };

        let request = rewrite_browser_request(
            "https://_pubky.pubky7ir1ttte48bcp4zjychjyscicrwi1j34mtt91ptsafdbjmr8g9eo/.well-known/locks-server?x=1",
            &endpoint,
            None,
        )
        .unwrap();

        assert_eq!(
            request.url.as_str(),
            "https://locks.example:8443/.well-known/locks-server?x=1"
        );
        assert_eq!(
            request.pubky_host.as_deref(),
            Some("pubky7ir1ttte48bcp4zjychjyscicrwi1j34mtt91ptsafdbjmr8g9eo")
        );
    }

    #[test]
    fn browser_request_rewrite_uses_http_port_for_localhost_endpoint() {
        let mut params = BTreeMap::new();
        params.insert(HTTP_PORT_PARAM, 55433);
        let endpoint = BrowserEndpoint {
            domain: Some("localhost".to_owned()),
            port: Some(443),
            params,
        };

        let request = rewrite_browser_request(
            "https://_pubky.pubky7ir1ttte48bcp4zjychjyscicrwi1j34mtt91ptsafdbjmr8g9eo/connect",
            &endpoint,
            None,
        )
        .unwrap();

        assert_eq!(request.url.as_str(), "http://localhost:55433/connect");
    }

    #[test]
    fn browser_request_rewrite_requires_http_port_for_localhost_endpoint() {
        let endpoint = BrowserEndpoint {
            domain: Some("localhost".to_owned()),
            port: Some(443),
            params: BTreeMap::new(),
        };

        let result = rewrite_browser_request(
            "https://_pubky.pubky7ir1ttte48bcp4zjychjyscicrwi1j34mtt91ptsafdbjmr8g9eo/connect",
            &endpoint,
            None,
        );

        assert!(result.is_err());
    }

    #[test]
    fn browser_request_rewrite_uses_http_port_for_configured_testnet_host() {
        let mut params = BTreeMap::new();
        params.insert(HTTP_PORT_PARAM, 8080);
        let endpoint = BrowserEndpoint {
            domain: Some("testnet.local".to_owned()),
            port: Some(443),
            params,
        };

        let request = rewrite_browser_request(
            "https://_pubky.pubky7ir1ttte48bcp4zjychjyscicrwi1j34mtt91ptsafdbjmr8g9eo/connect",
            &endpoint,
            Some("testnet.local"),
        )
        .unwrap();

        assert_eq!(request.url.as_str(), "http://testnet.local:8080/connect");
    }
}
