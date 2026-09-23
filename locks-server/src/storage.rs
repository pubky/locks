use locks_service::infrastructure::final_credentials::FinalCredentialCipher;
use locks_service::infrastructure::postgres::{
    CreatorAuthoritySecretCipher, PostgresError, run_migrations,
};
use locks_service::infrastructure::runtime_master_key::RuntimeMasterKey;
use sqlx::postgres::PgPoolOptions;

use crate::app_state::AppState;
use crate::config::{DatabaseConfig, LockServerRuntimeConfig, SecretsConfig};

/// Errors raised while composing runtime storage adapters.
#[derive(Debug, thiserror::Error)]
pub enum RuntimeStorageError {
    #[error("failed to connect to postgres runtime database: {0}")]
    Connect(#[from] sqlx::Error),
    #[error("failed to run postgres runtime migrations: {0}")]
    Migrate(#[from] PostgresError),
    #[error("runtime master key env var is not set: {0}")]
    MissingRuntimeMasterKeyEnv(String),
    #[error("invalid runtime master key in env var: {0}")]
    InvalidRuntimeMasterKey(String),
}

/// Builds application state for the production-shaped runtime.
///
/// Postgres backs Lock-Server-private runtime state: verification tasks,
/// verification task claims, access credentials, creator authority, connect
/// flows, one-time frontend session codes, and frontend sessions. Pubky-owned
/// content locks, guarded resources, and entitlement records are behind creator
/// repository ports. When configured with the `pubky-homeserver` creator backend,
/// the server binary composes SDK-backed Pubky repository adapters.
pub async fn build_runtime_state(
    config: LockServerRuntimeConfig,
) -> Result<AppState, RuntimeStorageError> {
    let (creator_authority_cipher, final_credential_cipher) =
        runtime_ciphers_from_env(&config.secrets)?;
    let pool = connect_database(&config.database).await?;
    if config.database.run_migrations_on_startup {
        run_migrations(&pool).await?;
    }

    Ok(AppState::new_with_postgres_runtime(
        config,
        pool,
        creator_authority_cipher,
        final_credential_cipher,
    ))
}

#[cfg(test)]
fn creator_authority_cipher_from_env(
    config: &SecretsConfig,
) -> Result<CreatorAuthoritySecretCipher, RuntimeStorageError> {
    runtime_ciphers_from_env(config).map(|(creator, _)| creator)
}

fn runtime_ciphers_from_env(
    config: &SecretsConfig,
) -> Result<(CreatorAuthoritySecretCipher, FinalCredentialCipher), RuntimeStorageError> {
    let key = std::env::var(&config.runtime_master_key_env).map_err(|_| {
        RuntimeStorageError::MissingRuntimeMasterKeyEnv(config.runtime_master_key_env.clone())
    })?;
    let master_key = RuntimeMasterKey::from_base64url(&key).map_err(|_| {
        RuntimeStorageError::InvalidRuntimeMasterKey(config.runtime_master_key_env.clone())
    })?;
    Ok((
        CreatorAuthoritySecretCipher::new(master_key.creator_authority_key()),
        FinalCredentialCipher::new(master_key.final_credential_key()),
    ))
}

async fn connect_database(config: &DatabaseConfig) -> Result<sqlx::PgPool, sqlx::Error> {
    PgPoolOptions::new()
        .max_connections(config.max_connections)
        .connect(&config.url)
        .await
}

#[cfg(test)]
mod tests {
    use base64::Engine;
    use base64::engine::general_purpose::URL_SAFE_NO_PAD;

    use super::{RuntimeStorageError, build_runtime_state, creator_authority_cipher_from_env};
    use crate::config::SecretsConfig;
    use crate::testing::TestServerApp;

    #[test]
    fn creator_authority_cipher_from_env_accepts_configured_32_byte_base64url_key() {
        let env_name = unique_env_name("VALID");
        unsafe {
            std::env::set_var(&env_name, URL_SAFE_NO_PAD.encode([7u8; 32]));
        }
        let config = SecretsConfig {
            runtime_master_key_env: env_name.clone(),
        };

        let cipher = creator_authority_cipher_from_env(&config).unwrap();

        assert!(format!("{cipher:?}").contains("<redacted>"));
        unsafe {
            std::env::remove_var(env_name);
        }
    }

    #[test]
    fn creator_authority_cipher_from_env_rejects_missing_key_without_raw_secret() {
        let env_name = unique_env_name("MISSING");
        unsafe {
            std::env::remove_var(&env_name);
        }
        let config = SecretsConfig {
            runtime_master_key_env: env_name.clone(),
        };

        let error = creator_authority_cipher_from_env(&config).unwrap_err();

        assert_eq!(
            error.to_string(),
            format!("runtime master key env var is not set: {env_name}")
        );
    }

    #[test]
    fn creator_authority_cipher_from_env_rejects_invalid_key_without_leaking_value() {
        let env_name = unique_env_name("INVALID");
        unsafe {
            std::env::set_var(&env_name, "not-a-valid-key");
        }
        let config = SecretsConfig {
            runtime_master_key_env: env_name.clone(),
        };

        let error = creator_authority_cipher_from_env(&config).unwrap_err();

        assert!(matches!(
            error,
            RuntimeStorageError::InvalidRuntimeMasterKey(ref name) if name == &env_name
        ));
        let debug = format!("{error:?}");
        assert!(!debug.contains("not-a-valid-key"));
        unsafe {
            std::env::remove_var(env_name);
        }
    }

    #[tokio::test]
    async fn server_binary_runtime_accepts_pubky_homeserver_creator_backend_after_sdk_wiring() {
        let env_name = unique_env_name("MISSING_FOR_PUBKY_BACKEND");
        unsafe {
            std::env::remove_var(&env_name);
        }
        let mut config = TestServerApp::default_in_memory_config();
        config.secrets = SecretsConfig {
            runtime_master_key_env: env_name.clone(),
        };

        let error = build_runtime_state(config).await.unwrap_err();

        assert!(matches!(
            error,
            RuntimeStorageError::MissingRuntimeMasterKeyEnv(ref name) if name == &env_name
        ));
        assert!(!error.to_string().contains("postgres://"));
    }

    fn unique_env_name(suffix: &str) -> String {
        format!(
            "LOCKS_TEST_RUNTIME_MASTER_KEY_{}_{}",
            suffix,
            uuid::Uuid::new_v4().simple()
        )
    }
}
