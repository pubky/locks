use std::borrow::Cow;
use std::net::IpAddr;

use pkarr::dns::Name;
use pkarr::dns::rdata::{SVCB, SVCParam};
use pubky_common::crypto::Keypair;

use crate::config::{
    LockServerRuntimeConfig, LockServerSigningKeyError, PkdnsConfig,
    load_lock_server_signing_keypair,
};

#[derive(Debug, thiserror::Error)]
pub enum LockServerKeyRepublisherError {
    #[error("failed to read lock server signing seed: {0}")]
    SigningSeedRead(std::io::Error),
    #[error("lock_server_secret_key must contain keypair-seed:<base64url-no-pad-32-byte-seed>")]
    InvalidSigningSeed,
    #[error("lock_server_public_key does not match lock_server_secret_key signing seed")]
    PublicKeyMismatch,
    #[error("failed to build lock server PKARR packet: {0}")]
    PacketBuild(String),
    #[error("failed to build PKARR client: {0}")]
    ClientBuild(String),
    #[error("failed to publish lock server PKARR packet: {0}")]
    Publish(String),
}

/// Background task that publishes and periodically republishes the Lock Server key's PKARR record.
#[derive(Debug)]
pub struct LockServerKeyRepublisher {
    join_handle: tokio::task::JoinHandle<()>,
}

impl LockServerKeyRepublisher {
    pub async fn start_if_required(
        config: &LockServerRuntimeConfig,
    ) -> Result<Option<Self>, LockServerKeyRepublisherError> {
        if !requires_lock_server_pkarr(config) {
            return Ok(None);
        }

        let keypair = load_lock_server_keypair(&config.credentials)?;
        let signed_packet = create_signed_packet(&config.pkdns, &keypair)?;
        let mut builder = pkarr::Client::builder();
        if !config.pkdns.pkarr_relays.is_empty() {
            builder
                .relays(&config.pkdns.pkarr_relays)
                .map_err(|error| LockServerKeyRepublisherError::ClientBuild(error.to_string()))?;
        }
        let client = builder
            .build()
            .map_err(|error| LockServerKeyRepublisherError::ClientBuild(error.to_string()))?;
        publish_once(&client, &signed_packet).await?;

        let interval_seconds = config.pkdns.key_republisher_interval_seconds;
        let join_handle = tokio::spawn(async move {
            let mut interval =
                tokio::time::interval(std::time::Duration::from_secs(interval_seconds));
            interval.tick().await;
            loop {
                interval.tick().await;
                let _ = publish_once(&client, &signed_packet).await;
            }
        });

        Ok(Some(Self { join_handle }))
    }

    pub fn stop(&self) {
        self.join_handle.abort();
    }
}

impl Drop for LockServerKeyRepublisher {
    fn drop(&mut self) {
        self.stop();
    }
}

fn requires_lock_server_pkarr(config: &LockServerRuntimeConfig) -> bool {
    !config.runtime.environment.is_development() || config.creator_authority_acquisition.enabled
}

fn load_lock_server_keypair(
    credentials: &crate::config::LockServerCredentialsConfig,
) -> Result<Keypair, LockServerKeyRepublisherError> {
    load_lock_server_signing_keypair(credentials).map_err(|error| match error {
        LockServerSigningKeyError::Read(source) => {
            LockServerKeyRepublisherError::SigningSeedRead(source)
        }
        LockServerSigningKeyError::InvalidSeed => LockServerKeyRepublisherError::InvalidSigningSeed,
        LockServerSigningKeyError::PublicKeyMismatch => {
            LockServerKeyRepublisherError::PublicKeyMismatch
        }
    })
}

pub fn create_signed_packet(
    config: &PkdnsConfig,
    keypair: &Keypair,
) -> Result<pkarr::SignedPacket, LockServerKeyRepublisherError> {
    let root_name: Name = "."
        .try_into()
        .expect(". is the root domain and always valid");

    let mut signed_packet_builder = pkarr::SignedPacket::builder();

    let mut svcb = SVCB::new(1, root_name.clone());
    if let Some(port) = config.public_pubky_tls_port {
        svcb.set_port(port);
    }
    match config.public_ip {
        IpAddr::V4(ip) => svcb.set_ipv4hint(&[ip.to_bits()]),
        IpAddr::V6(ip) => svcb.set_ipv6hint(&[ip.to_bits()]),
    };
    signed_packet_builder = signed_packet_builder.https(root_name.clone(), svcb, 60 * 60);

    if let Some(domain) = &config.icann_domain {
        let mut svcb = SVCB::new(10, root_name.clone());
        if let Some(port) = config.public_icann_http_port {
            let http_port_be_bytes = port.to_be_bytes();
            if domain == "localhost"
                || domain
                    .parse::<IpAddr>()
                    .is_ok_and(|address| address.is_loopback())
            {
                svcb.set_param(SVCParam::Unknown(
                    pubky_common::constants::reserved_param_keys::HTTP_PORT,
                    Cow::Owned(http_port_be_bytes.to_vec()),
                ));
            }
        }
        let target: Name =
            domain
                .as_str()
                .try_into()
                .map_err(|error: pkarr::dns::SimpleDnsError| {
                    LockServerKeyRepublisherError::PacketBuild(error.to_string())
                })?;
        svcb.target = target;
        signed_packet_builder = signed_packet_builder.https(root_name.clone(), svcb, 60 * 60);
    }

    signed_packet_builder =
        signed_packet_builder.address(root_name.clone(), config.public_ip, 60 * 60);
    signed_packet_builder
        .build(keypair)
        .map_err(|error| LockServerKeyRepublisherError::PacketBuild(error.to_string()))
}

async fn publish_once(
    client: &pkarr::Client,
    signed_packet: &pkarr::SignedPacket,
) -> Result<(), LockServerKeyRepublisherError> {
    client
        .publish(signed_packet, None)
        .await
        .map_err(|error| LockServerKeyRepublisherError::Publish(error.to_string()))
}

#[cfg(test)]
mod tests {
    use std::net::Ipv4Addr;
    use std::path::PathBuf;
    use std::str::FromStr;

    use base64::Engine;
    use base64::engine::general_purpose::URL_SAFE_NO_PAD;
    use locks_core::ids::LockServerPubky;
    use pkarr::dns::rdata::RData;

    use crate::config::{
        ContentLocksConfig, CreatorAuthorityAcquisitionConfig, DatabaseConfig,
        LockServerCredentialsConfig, LoggingConfig, PubkyConfig, RateLimitsConfig, RuntimeConfig,
        RuntimeEnvironment, SecretsConfig, WorkerConfig,
    };

    use super::*;

    #[test]
    fn create_signed_packet_uses_operator_pkdns_endpoints() {
        let keypair = Keypair::from_secret(&[9_u8; 32]);
        let config = PkdnsConfig {
            public_ip: IpAddr::V4(Ipv4Addr::new(203, 0, 113, 10)),
            public_pubky_tls_port: Some(6287),
            public_icann_http_port: Some(8080),
            icann_domain: Some("localhost".to_owned()),
            pkarr_relays: Vec::new(),
            key_republisher_interval_seconds: 3600,
        };

        let packet = create_signed_packet(&config, &keypair).unwrap();
        let records: Vec<_> = packet.all_resource_records().collect();

        assert_eq!(records.len(), 3);
        assert!(
            records
                .iter()
                .any(|record| matches!(record.rdata, RData::A(_)))
        );
        assert!(records.iter().any(|record| match &record.rdata {
            RData::HTTPS(https) => {
                matches!(
                    https.0.iter_params().find(|param| param.key_code() == 3),
                    Some(SVCParam::Port(6287))
                )
            }
            _ => false,
        }));
        assert!(records.iter().any(|record| match &record.rdata {
            RData::HTTPS(https) => {
                https.0.priority == 10 && https.0.target.to_string().contains("localhost")
            }
            _ => false,
        }));
    }

    #[test]
    fn create_signed_packet_publishes_http_port_for_loopback_ip_domain() {
        let keypair = Keypair::from_secret(&[9_u8; 32]);
        let config = PkdnsConfig {
            public_ip: IpAddr::V4(Ipv4Addr::LOCALHOST),
            public_pubky_tls_port: Some(6287),
            public_icann_http_port: Some(3000),
            icann_domain: Some("127.0.0.1".to_owned()),
            pkarr_relays: Vec::new(),
            key_republisher_interval_seconds: 3600,
        };

        let packet = create_signed_packet(&config, &keypair).unwrap();
        let http_endpoint = packet
            .all_resource_records()
            .find_map(|record| match &record.rdata {
                RData::HTTPS(https) if https.0.target.to_string().contains("127.0.0.1") => {
                    Some(https)
                }
                _ => None,
            })
            .unwrap();

        assert!(matches!(
            http_endpoint
                .0
                .iter_params()
                .find(|param| param.key_code()
                    == pubky_common::constants::reserved_param_keys::HTTP_PORT),
            Some(SVCParam::Unknown(_, value)) if value.as_ref() == 3000_u16.to_be_bytes()
        ));
    }

    #[test]
    fn load_lock_server_keypair_rejects_secret_seed_that_does_not_match_configured_public_key() {
        let temp_dir = tempfile::tempdir().unwrap();
        let secret_path = temp_dir.path().join("server.keypair-seed");
        std::fs::write(
            &secret_path,
            format!("keypair-seed:{}", URL_SAFE_NO_PAD.encode([9_u8; 32])),
        )
        .unwrap();
        let credentials = LockServerCredentialsConfig {
            lock_server_secret_key: secret_path,
            lock_server_public_key: LockServerPubky::from_str(
                "pubky7ir1ttte48bcp4zjychjyscicrwi1j34mtt91ptsafdbjmr8g9eo",
            )
            .unwrap(),
            max_ttl_seconds: 900,
        };

        let error = load_lock_server_keypair(&credentials).unwrap_err();

        assert!(matches!(
            error,
            LockServerKeyRepublisherError::PublicKeyMismatch
        ));
    }

    #[test]
    fn pkarr_republisher_is_required_for_production_lock_server_identity() {
        let mut config = test_config();
        config.creator_authority_acquisition.enabled = false;
        assert!(!requires_lock_server_pkarr(&config));

        config.runtime.environment = RuntimeEnvironment::Production;
        assert!(requires_lock_server_pkarr(&config));

        config.creator_authority_acquisition.enabled = true;
        assert!(requires_lock_server_pkarr(&config));
    }

    #[test]
    fn pkarr_republisher_is_required_for_dev_creator_authority_acquisition() {
        let mut config = test_config();
        config.runtime.environment = RuntimeEnvironment::Development;
        config.creator_authority_acquisition.enabled = true;

        assert!(requires_lock_server_pkarr(&config));
    }

    fn test_config() -> LockServerRuntimeConfig {
        LockServerRuntimeConfig {
            bind_addr: "127.0.0.1:0".parse().unwrap(),
            credentials: LockServerCredentialsConfig {
                lock_server_secret_key: PathBuf::from("/tmp/locks-test-secret"),
                lock_server_public_key: LockServerPubky::from_str(
                    "pubky7ir1ttte48bcp4zjychjyscicrwi1j34mtt91ptsafdbjmr8g9eo",
                )
                .unwrap(),
                max_ttl_seconds: 900,
            },
            database: DatabaseConfig {
                url: "postgres://locks:locks@localhost/locks_test".to_owned(),
                max_connections: 10,
                run_migrations_on_startup: true,
            },
            worker: WorkerConfig {
                enabled: false,
                poll_interval_ms: 250,
                claim_timeout_seconds: 60,
                worker_id: "test-worker".to_owned(),
            },
            runtime: RuntimeConfig {
                environment: RuntimeEnvironment::Development,
            },
            creator_authority_acquisition: CreatorAuthorityAcquisitionConfig::default(),
            secrets: SecretsConfig::default(),
            logging: LoggingConfig::default(),
            pubky: PubkyConfig::default(),
            pkdns: PkdnsConfig::default(),
            rate_limits: RateLimitsConfig::default(),
            content_locks: ContentLocksConfig::default(),
            deletion: crate::config::DeletionConfig::default(),
            paykit: None,
        }
    }
}
