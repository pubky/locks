use crate::config::{PubkyConfig, PubkyNetwork, PubkyResolution};

const LOCAL_TESTNET_HOST: &str = "127.0.0.1";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum PubkyHttpClientConstructor {
    Mainnet,
    MainnetRelayOnly,
    Testnet(&'static str),
}

pub(super) fn pubky_http_client_constructor(config: &PubkyConfig) -> PubkyHttpClientConstructor {
    match (config.network, config.resolution) {
        (PubkyNetwork::Mainnet, PubkyResolution::Default) => PubkyHttpClientConstructor::Mainnet,
        (PubkyNetwork::Mainnet, PubkyResolution::RelayOnly) => {
            PubkyHttpClientConstructor::MainnetRelayOnly
        }
        (PubkyNetwork::Testnet, _) => PubkyHttpClientConstructor::Testnet(LOCAL_TESTNET_HOST),
    }
}

pub(super) fn build_pubky_http_client(config: &PubkyConfig) -> pubky::PubkyHttpClient {
    match pubky_http_client_constructor(config) {
        PubkyHttpClientConstructor::Mainnet => pubky::PubkyHttpClient::new(),
        PubkyHttpClientConstructor::MainnetRelayOnly => {
            let mut builder = pubky::PubkyHttpClient::builder();
            builder.pkarr(|client| client.no_dht());
            builder.build()
        }
        PubkyHttpClientConstructor::Testnet(host) => {
            let mut builder = pubky::PubkyHttpClient::builder();
            builder.testnet_with_host(host);
            builder.pkarr(|client| client.no_dht());
            builder.build()
        }
    }
    .expect("Pubky HTTP client construction must succeed for Pubky runtime composition")
}

pub(super) fn build_pubky_client(config: &PubkyConfig) -> pubky::Pubky {
    pubky::Pubky::with_client(build_pubky_http_client(config))
}

pub(super) fn pubky_auth_relay_for_network(network: PubkyNetwork) -> Option<url::Url> {
    match network {
        PubkyNetwork::Mainnet => None,
        PubkyNetwork::Testnet => Some(
            "http://127.0.0.1:15412/inbox/"
                .parse()
                .expect("local testnet auth relay URL must be valid"),
        ),
    }
}
