use crate::config::PubkyNetwork;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum PubkyClientConstructor {
    Mainnet,
    Testnet,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum PubkyHttpClientConstructor {
    Mainnet,
    Testnet,
}

pub(super) fn pubky_client_constructor(network: PubkyNetwork) -> PubkyClientConstructor {
    match network {
        PubkyNetwork::Mainnet => PubkyClientConstructor::Mainnet,
        PubkyNetwork::Testnet => PubkyClientConstructor::Testnet,
    }
}

pub(super) fn pubky_http_client_constructor(network: PubkyNetwork) -> PubkyHttpClientConstructor {
    match network {
        PubkyNetwork::Mainnet => PubkyHttpClientConstructor::Mainnet,
        PubkyNetwork::Testnet => PubkyHttpClientConstructor::Testnet,
    }
}

pub(super) fn build_pubky_http_client(network: PubkyNetwork) -> pubky::PubkyHttpClient {
    match pubky_http_client_constructor(network) {
        PubkyHttpClientConstructor::Mainnet => pubky::PubkyHttpClient::new(),
        PubkyHttpClientConstructor::Testnet => pubky::PubkyHttpClient::testnet(),
    }
    .expect("Pubky HTTP client construction must succeed for Pubky runtime composition")
}

pub(super) fn build_pubky_client(network: PubkyNetwork) -> pubky::Pubky {
    match pubky_client_constructor(network) {
        PubkyClientConstructor::Mainnet => pubky::Pubky::new(),
        PubkyClientConstructor::Testnet => pubky::Pubky::testnet(),
    }
    .expect("Pubky client construction must succeed for legacy connect flows")
}

pub(super) fn pubky_auth_relay_for_network(network: PubkyNetwork) -> Option<url::Url> {
    match network {
        PubkyNetwork::Mainnet => None,
        PubkyNetwork::Testnet => Some(
            "http://localhost:15412/inbox/"
                .parse()
                .expect("local testnet auth relay URL must be valid"),
        ),
    }
}
