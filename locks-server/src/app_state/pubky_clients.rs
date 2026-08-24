use crate::config::PubkyNetwork;

const LOCAL_TESTNET_HOST: &str = "127.0.0.1";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum PubkyHttpClientConstructor {
    Mainnet,
    Testnet(&'static str),
}

pub(super) fn pubky_http_client_constructor(network: PubkyNetwork) -> PubkyHttpClientConstructor {
    match network {
        PubkyNetwork::Mainnet => PubkyHttpClientConstructor::Mainnet,
        PubkyNetwork::Testnet => PubkyHttpClientConstructor::Testnet(LOCAL_TESTNET_HOST),
    }
}

pub(super) fn build_pubky_http_client(network: PubkyNetwork) -> pubky::PubkyHttpClient {
    match pubky_http_client_constructor(network) {
        PubkyHttpClientConstructor::Mainnet => pubky::PubkyHttpClient::new(),
        PubkyHttpClientConstructor::Testnet(host) => {
            let mut builder = pubky::PubkyHttpClient::builder();
            builder.testnet_with_host(host);
            builder.pkarr(|client| client.no_dht());
            builder.build()
        }
    }
    .expect("Pubky HTTP client construction must succeed for Pubky runtime composition")
}

pub(super) fn build_pubky_client(network: PubkyNetwork) -> pubky::Pubky {
    pubky::Pubky::with_client(build_pubky_http_client(network))
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
