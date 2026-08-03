use std::fs;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result, bail};
use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use clap::{Parser, Subcommand};
use pubky::{Pubky, PublicKey};
use pubky_common::crypto::Keypair;
use pubky_common::recovery_file::{create_recovery_file, decrypt_recovery_file};
use rand::RngCore;
use rand::rngs::OsRng;
use serde::Serialize;
use url::Url;

const PASSPHRASE_FILE: &str = "passphrase";
const RECOVERY_FILE: &str = "recovery_file";

#[derive(Debug, Parser)]
#[command(
    version,
    about = "Dev helper for legacy-connect auth against a local Pubky testnet"
)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Create or reuse a local dev Pubky user and sign it up on the testnet homeserver.
    EnsureUser {
        /// Directory containing generated dev recovery/passphrase material.
        #[arg(long)]
        home: PathBuf,
        /// Testnet homeserver public key, e.g. pubky8pinxx...
        #[arg(long)]
        homeserver: String,
    },
    /// Approve a Lock Server legacy-connect pubkyauth URL with the local dev Pubky user.
    ApproveAuth {
        /// Directory containing generated dev recovery/passphrase material.
        #[arg(long)]
        home: PathBuf,
        /// Testnet homeserver public key, e.g. pubky8pinxx...
        #[arg(long)]
        homeserver: String,
        /// Lock Server legacy-connect pubkyauth URL.
        #[arg(long)]
        auth_url: String,
    },
}

#[derive(Debug, Serialize)]
struct EnsureUserOutput {
    creator: String,
    recovery_file: String,
}

#[derive(Debug, Serialize)]
struct ApproveAuthOutput {
    creator: String,
    approved: bool,
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();

    match cli.command {
        Command::EnsureUser { home, homeserver } => {
            let identity = ensure_identity(&home)?;
            let homeserver = parse_homeserver(&homeserver)?;
            let signer = Pubky::testnet()?.signer(identity.keypair.clone());
            best_effort_signup(&signer, &homeserver).await?;
            write_json(&EnsureUserOutput {
                creator: creator_pubky(&identity.keypair),
                recovery_file: identity.recovery_file.display().to_string(),
            })?;
        }
        Command::ApproveAuth {
            home,
            homeserver,
            auth_url,
        } => {
            validate_pubkyauth_url(&auth_url)?;
            let identity = load_identity(&home)?;
            let homeserver = parse_homeserver(&homeserver)?;
            let signer = Pubky::testnet()?.signer(identity.keypair.clone());
            best_effort_signup(&signer, &homeserver).await?;
            signer
                .approve_auth(&auth_url)
                .await
                .context("failed to approve legacy-connect auth URL")?;
            write_json(&ApproveAuthOutput {
                creator: creator_pubky(&identity.keypair),
                approved: true,
            })?;
        }
    }

    Ok(())
}

#[derive(Debug)]
struct DevIdentity {
    keypair: Keypair,
    recovery_file: PathBuf,
}

fn ensure_identity(home: &Path) -> Result<DevIdentity> {
    ensure_secure_dir(home)?;
    let passphrase_path = home.join(PASSPHRASE_FILE);
    let recovery_file = home.join(RECOVERY_FILE);

    let passphrase = if passphrase_path.exists() {
        read_trimmed(&passphrase_path).context("failed to read dev passphrase")?
    } else {
        let passphrase = random_passphrase();
        write_secret_file(&passphrase_path, passphrase.as_bytes())?;
        eprintln!("Generated dev passphrase at {}", passphrase_path.display());
        passphrase
    };

    if recovery_file.exists() {
        let keypair = read_keypair(&recovery_file, &passphrase)?;
        return Ok(DevIdentity {
            keypair,
            recovery_file,
        });
    }

    let keypair = Keypair::random();
    let recovery_bytes = create_recovery_file(&keypair, &passphrase);
    write_secret_file(&recovery_file, &recovery_bytes)?;
    eprintln!("Generated dev recovery file at {}", recovery_file.display());

    Ok(DevIdentity {
        keypair,
        recovery_file,
    })
}

fn load_identity(home: &Path) -> Result<DevIdentity> {
    let passphrase_path = home.join(PASSPHRASE_FILE);
    let recovery_file = home.join(RECOVERY_FILE);

    if !passphrase_path.exists() || !recovery_file.exists() {
        bail!(
            "missing dev identity under {}; run ensure-user first",
            home.display()
        );
    }

    let passphrase = read_trimmed(&passphrase_path).context("failed to read dev passphrase")?;
    let keypair = read_keypair(&recovery_file, &passphrase)?;
    Ok(DevIdentity {
        keypair,
        recovery_file,
    })
}

fn read_keypair(recovery_file: &Path, passphrase: &str) -> Result<Keypair> {
    let recovery_bytes = fs::read(recovery_file)
        .with_context(|| format!("failed to read {}", recovery_file.display()))?;
    decrypt_recovery_file(&recovery_bytes, passphrase)
        .with_context(|| format!("failed to decrypt {}", recovery_file.display()))
}

fn read_trimmed(path: &Path) -> Result<String> {
    let value = fs::read_to_string(path)?;
    let value = value.trim().to_owned();
    if value.is_empty() {
        bail!("{} is empty", path.display());
    }
    Ok(value)
}

fn random_passphrase() -> String {
    let mut bytes = [0_u8; 32];
    OsRng.fill_bytes(&mut bytes);
    URL_SAFE_NO_PAD.encode(bytes)
}

fn parse_homeserver(homeserver: &str) -> Result<PublicKey> {
    PublicKey::try_from(homeserver)
        .with_context(|| format!("invalid homeserver public key: {homeserver}"))
}

fn validate_pubkyauth_url(auth_url: &str) -> Result<()> {
    let parsed = Url::parse(auth_url).context("auth-url is not a valid URL")?;
    if parsed.scheme() != "pubkyauth" {
        bail!("auth-url must use pubkyauth:// scheme");
    }
    Ok(())
}

fn creator_pubky(keypair: &Keypair) -> String {
    let public_key = keypair.public_key().to_string();
    if public_key.starts_with("pubky") {
        public_key
    } else {
        format!("pubky{public_key}")
    }
}

async fn best_effort_signup(signer: &pubky::PubkySigner, homeserver: &PublicKey) -> Result<()> {
    match signer.signup(homeserver, None).await {
        Ok(_session) => {
            eprintln!("Signed up dev Pubky user on homeserver {homeserver}");
            Ok(())
        }
        Err(error) if is_probably_already_signed_up(&error) => {
            eprintln!("Dev Pubky user already appears signed up on homeserver {homeserver}");
            Ok(())
        }
        Err(error) => Err(error).context("failed to sign up dev Pubky user on testnet homeserver"),
    }
}

fn is_probably_already_signed_up(error: &pubky::Error) -> bool {
    let message = error.to_string().to_ascii_lowercase();
    message.contains("already") || message.contains("409") || message.contains("conflict")
}

fn write_json<T: Serialize>(value: &T) -> Result<()> {
    println!("{}", serde_json::to_string(value)?);
    Ok(())
}

fn ensure_secure_dir(path: &Path) -> Result<()> {
    fs::create_dir_all(path).with_context(|| format!("failed to create {}", path.display()))?;
    set_dir_permissions(path)?;
    Ok(())
}

fn write_secret_file(path: &Path, bytes: &[u8]) -> Result<()> {
    fs::write(path, bytes).with_context(|| format!("failed to write {}", path.display()))?;
    set_file_permissions(path)?;
    Ok(())
}

#[cfg(unix)]
fn set_dir_permissions(path: &Path) -> Result<()> {
    use std::os::unix::fs::PermissionsExt;
    fs::set_permissions(path, fs::Permissions::from_mode(0o700))
        .with_context(|| format!("failed to chmod 0700 {}", path.display()))
}

#[cfg(not(unix))]
fn set_dir_permissions(_path: &Path) -> Result<()> {
    Ok(())
}

#[cfg(unix)]
fn set_file_permissions(path: &Path) -> Result<()> {
    use std::os::unix::fs::PermissionsExt;
    fs::set_permissions(path, fs::Permissions::from_mode(0o600))
        .with_context(|| format!("failed to chmod 0600 {}", path.display()))
}

#[cfg(not(unix))]
fn set_file_permissions(_path: &Path) -> Result<()> {
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn validate_pubkyauth_url_rejects_non_pubkyauth_scheme() {
        assert!(validate_pubkyauth_url("https://example.com").is_err());
        assert!(validate_pubkyauth_url("pubkyauth:///?relay=http://localhost").is_ok());
    }

    #[test]
    fn creator_pubky_has_single_pubky_prefix() {
        let keypair = Keypair::random();
        let creator = creator_pubky(&keypair);
        assert!(creator.starts_with("pubky"));
        assert!(!creator.starts_with("pubkypubky"));
    }

    #[test]
    fn random_passphrase_is_url_safe_and_non_empty() {
        let passphrase = random_passphrase();
        assert!(!passphrase.is_empty());
        assert!(!passphrase.contains('='));
        assert!(
            passphrase
                .chars()
                .all(|char| char.is_ascii_alphanumeric() || char == '-' || char == '_')
        );
    }
}
