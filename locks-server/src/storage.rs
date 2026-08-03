use locks_service::infrastructure::postgres::{
    CreatorAuthoritySecretCipher, PostgresError, run_migrations,
};
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
    #[error("creator authority encryption key env var is not set: {0}")]
    MissingCreatorAuthorityEncryptionKeyEnv(String),
    #[error("invalid creator authority encryption key in env var: {0}")]
    InvalidCreatorAuthorityEncryptionKey(String),
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
    let creator_authority_cipher = creator_authority_cipher_from_env(&config.secrets)?;
    let pool = connect_database(&config.database).await?;
    if config.database.run_migrations_on_startup {
        run_migrations(&pool).await?;
    }

    Ok(AppState::new_with_postgres_runtime(
        config,
        pool,
        creator_authority_cipher,
    ))
}

fn creator_authority_cipher_from_env(
    config: &SecretsConfig,
) -> Result<CreatorAuthoritySecretCipher, RuntimeStorageError> {
    let key = std::env::var(&config.creator_authority_key_env).map_err(|_| {
        RuntimeStorageError::MissingCreatorAuthorityEncryptionKeyEnv(
            config.creator_authority_key_env.clone(),
        )
    })?;
    CreatorAuthoritySecretCipher::from_base64url_key(&key).map_err(|_| {
        RuntimeStorageError::InvalidCreatorAuthorityEncryptionKey(
            config.creator_authority_key_env.clone(),
        )
    })
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
            creator_authority_key_env: env_name.clone(),
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
            creator_authority_key_env: env_name.clone(),
        };

        let error = creator_authority_cipher_from_env(&config).unwrap_err();

        assert_eq!(
            error.to_string(),
            format!("creator authority encryption key env var is not set: {env_name}")
        );
    }

    #[test]
    fn creator_authority_cipher_from_env_rejects_invalid_key_without_leaking_value() {
        let env_name = unique_env_name("INVALID");
        unsafe {
            std::env::set_var(&env_name, "not-a-valid-key");
        }
        let config = SecretsConfig {
            creator_authority_key_env: env_name.clone(),
        };

        let error = creator_authority_cipher_from_env(&config).unwrap_err();

        assert!(matches!(
            error,
            RuntimeStorageError::InvalidCreatorAuthorityEncryptionKey(ref name) if name == &env_name
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
            creator_authority_key_env: env_name.clone(),
        };

        let error = build_runtime_state(config).await.unwrap_err();

        assert!(matches!(
            error,
            RuntimeStorageError::MissingCreatorAuthorityEncryptionKeyEnv(ref name) if name == &env_name
        ));
        assert!(!error.to_string().contains("postgres://"));
    }

    fn unique_env_name(suffix: &str) -> String {
        format!(
            "LOCKS_TEST_CREATOR_AUTH_KEY_{}_{}",
            suffix,
            uuid::Uuid::new_v4().simple()
        )
    }
}
