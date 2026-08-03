use std::path::Path;
use std::str::FromStr;

use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use locks_core::ids::LockServerPubky;
use pubky_common::crypto::Keypair;

use super::schema::{ConfigError, LockServerCredentialsConfig};

pub(crate) const LOCK_SERVER_SECRET_PREFIX: &str = "keypair-seed:";

#[derive(Debug, thiserror::Error)]
pub(crate) enum LockServerSigningKeyError {
    #[error("failed to read lock server signing seed: {0}")]
    Read(std::io::Error),
    #[error("lock_server_secret_key must contain keypair-seed:<base64url-no-pad-32-byte-seed>")]
    InvalidSeed,
    #[error("lock_server_public_key does not match lock_server_secret_key signing seed")]
    PublicKeyMismatch,
}

pub trait LockServerIdentityProvider {
    fn generate_secret(&self, secret_path: &Path) -> Result<LockServerPubky, ConfigError>;

    fn derive_public_key(&self, secret_path: &Path) -> Result<LockServerPubky, ConfigError>;
}

#[derive(Debug, Clone, Copy, Default)]
pub struct FilesystemLockServerIdentityProvider;

impl LockServerIdentityProvider for FilesystemLockServerIdentityProvider {
    fn generate_secret(&self, secret_path: &Path) -> Result<LockServerPubky, ConfigError> {
        if let Some(parent) = secret_path.parent() {
            std::fs::create_dir_all(parent).map_err(|source| ConfigError::CreateServiceHome {
                path: parent.to_path_buf(),
                source,
            })?;
        }
        let keypair = Keypair::random();
        let public_key = keypair_public_key(&keypair);
        let token = format!(
            "{LOCK_SERVER_SECRET_PREFIX}{}",
            URL_SAFE_NO_PAD.encode(keypair.secret())
        );
        std::fs::write(secret_path, token).map_err(|source| ConfigError::GenerateSecret {
            path: secret_path.to_path_buf(),
            message: source.to_string(),
        })?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(secret_path, std::fs::Permissions::from_mode(0o600)).map_err(
                |source| ConfigError::GenerateSecret {
                    path: secret_path.to_path_buf(),
                    message: source.to_string(),
                },
            )?;
        }
        Ok(public_key)
    }

    fn derive_public_key(&self, secret_path: &Path) -> Result<LockServerPubky, ConfigError> {
        derive_public_key_from_secret_file(secret_path)
    }
}

pub(super) fn derive_public_key_from_secret_file(
    secret_path: &Path,
) -> Result<LockServerPubky, ConfigError> {
    let secret =
        std::fs::read_to_string(secret_path).map_err(|source| ConfigError::DerivePublicKey {
            path: secret_path.to_path_buf(),
            message: source.to_string(),
        })?;
    if secret.trim().starts_with(LOCK_SERVER_SECRET_PREFIX) {
        let keypair = parse_lock_server_keypair_seed(&secret).map_err(|error| {
            ConfigError::DerivePublicKey {
                path: secret_path.to_path_buf(),
                message: error.to_string(),
            }
        })?;
        return Ok(keypair_public_key(&keypair));
    }
    let public_key = secret
        .split_once(':')
        .map(|(public_key, _)| public_key)
        .ok_or_else(|| ConfigError::DerivePublicKey {
            path: secret_path.to_path_buf(),
            message: "invalid session secret: expected `<pubkey>:<secret>`".to_owned(),
        })?;
    LockServerPubky::from_str(public_key).map_err(|source| ConfigError::DerivePublicKey {
        path: secret_path.to_path_buf(),
        message: source.to_string(),
    })
}

pub(crate) fn load_lock_server_signing_keypair(
    credentials: &LockServerCredentialsConfig,
) -> Result<Keypair, LockServerSigningKeyError> {
    let secret = std::fs::read_to_string(&credentials.lock_server_secret_key)
        .map_err(LockServerSigningKeyError::Read)?;
    let keypair = parse_lock_server_keypair_seed(&secret)?;
    if keypair_public_key(&keypair) != credentials.lock_server_public_key {
        return Err(LockServerSigningKeyError::PublicKeyMismatch);
    }
    Ok(keypair)
}

pub(crate) fn parse_lock_server_keypair_seed(
    secret: &str,
) -> Result<Keypair, LockServerSigningKeyError> {
    let encoded_seed = secret
        .trim()
        .strip_prefix(LOCK_SERVER_SECRET_PREFIX)
        .ok_or(LockServerSigningKeyError::InvalidSeed)?;
    let seed = URL_SAFE_NO_PAD
        .decode(encoded_seed.as_bytes())
        .map_err(|_| LockServerSigningKeyError::InvalidSeed)?;
    let seed: [u8; 32] = seed
        .try_into()
        .map_err(|_| LockServerSigningKeyError::InvalidSeed)?;
    Ok(Keypair::from_secret(&seed))
}

fn keypair_public_key(keypair: &Keypair) -> LockServerPubky {
    LockServerPubky::from_str(&keypair.public_key().to_string())
        .expect("a keypair public key is a valid Pubky identity")
}
