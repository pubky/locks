use std::str::FromStr;
use std::{fmt, str};

use async_trait::async_trait;
use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use chacha20poly1305::aead::{Aead, KeyInit};
use chacha20poly1305::{Key, XChaCha20Poly1305, XNonce};
use rand::RngCore;
use rand::rngs::OsRng;
use sqlx::types::Json;
use sqlx::{PgPool, Row};

use locks_core::ids::CreatorPubky;

use crate::application::errors::ApplicationError;
use crate::application::models::{
    CreatorAuthorityAuthKind, CreatorAuthorityRecord, CreatorAuthoritySecret,
};
use crate::application::ports::CreatorAuthorityStore;

/// Postgres-backed store for creator-granted homeserver authority secrets.
#[derive(Debug, Clone)]
pub struct PostgresCreatorAuthorityStore {
    pool: PgPool,
    cipher: Option<CreatorAuthoritySecretCipher>,
}

/// Encrypts/decrypts creator authority secret material before Postgres persistence.
#[derive(Clone)]
pub struct CreatorAuthoritySecretCipher {
    key: [u8; 32],
}

impl fmt::Debug for CreatorAuthoritySecretCipher {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("CreatorAuthoritySecretCipher")
            .field(&"<redacted>")
            .finish()
    }
}

impl CreatorAuthoritySecretCipher {
    /// Creates a cipher from a 32-byte symmetric encryption key.
    pub fn new(key: [u8; 32]) -> Self {
        Self { key }
    }

    /// Parses a 32-byte base64url-without-padding symmetric encryption key.
    pub fn from_base64url_key(value: &str) -> Result<Self, ApplicationError> {
        let key = URL_SAFE_NO_PAD
            .decode(value)
            .map_err(|_| invalid_encryption_key())?;
        let key: [u8; 32] = key.try_into().map_err(|_| invalid_encryption_key())?;
        Ok(Self::new(key))
    }

    fn encrypt(
        &self,
        creator: &CreatorPubky,
        auth_kind: CreatorAuthorityAuthKind,
        secret: &CreatorAuthoritySecret,
    ) -> Result<String, ApplicationError> {
        let cipher = XChaCha20Poly1305::new(Key::from_slice(&self.key));
        let mut nonce_bytes = [0u8; 24];
        OsRng.fill_bytes(&mut nonce_bytes);
        let nonce = XNonce::from_slice(&nonce_bytes);
        let aad = creator_authority_secret_aad(creator, auth_kind);
        let ciphertext = cipher
            .encrypt(
                nonce,
                chacha20poly1305::aead::Payload {
                    msg: secret.expose_secret().as_bytes(),
                    aad: aad.as_bytes(),
                },
            )
            .map_err(|_| ApplicationError::CreatorAuthoritySecret {
                message: "failed to encrypt creator authority secret".to_owned(),
            })?;

        Ok(format!(
            "v1.xchacha20poly1305:{}:{}",
            URL_SAFE_NO_PAD.encode(nonce_bytes),
            URL_SAFE_NO_PAD.encode(ciphertext)
        ))
    }

    fn decrypt(
        &self,
        creator: &CreatorPubky,
        auth_kind: CreatorAuthorityAuthKind,
        envelope: &str,
    ) -> Result<CreatorAuthoritySecret, ApplicationError> {
        let Some(rest) = envelope.strip_prefix("v1.xchacha20poly1305:") else {
            return Err(decrypt_error());
        };
        let Some((nonce, ciphertext)) = rest.split_once(':') else {
            return Err(decrypt_error());
        };
        let nonce = URL_SAFE_NO_PAD.decode(nonce).map_err(|_| decrypt_error())?;
        let ciphertext = URL_SAFE_NO_PAD
            .decode(ciphertext)
            .map_err(|_| decrypt_error())?;
        let nonce: [u8; 24] = nonce.try_into().map_err(|_| decrypt_error())?;
        let cipher = XChaCha20Poly1305::new(Key::from_slice(&self.key));
        let aad = creator_authority_secret_aad(creator, auth_kind);
        let plaintext = cipher
            .decrypt(
                XNonce::from_slice(&nonce),
                chacha20poly1305::aead::Payload {
                    msg: ciphertext.as_ref(),
                    aad: aad.as_bytes(),
                },
            )
            .map_err(|_| decrypt_error())?;
        let plaintext = String::from_utf8(plaintext).map_err(|_| decrypt_error())?;
        Ok(CreatorAuthoritySecret::new(plaintext))
    }
}

impl PostgresCreatorAuthorityStore {
    /// Creates a store backed by the provided migrated Postgres pool.
    pub fn new(pool: PgPool) -> Self {
        Self { pool, cipher: None }
    }

    /// Creates a store that encrypts creator authority secrets before persistence.
    pub fn new_encrypted(pool: PgPool, cipher: CreatorAuthoritySecretCipher) -> Self {
        Self {
            pool,
            cipher: Some(cipher),
        }
    }
}

#[async_trait]
impl CreatorAuthorityStore for PostgresCreatorAuthorityStore {
    async fn upsert_creator_authority(
        &self,
        authority: CreatorAuthorityRecord,
    ) -> Result<(), ApplicationError> {
        let persisted_secret = match &self.cipher {
            Some(cipher) => {
                cipher.encrypt(&authority.creator, authority.auth_kind, &authority.secret)?
            }
            None => authority.secret.expose_secret().to_owned(),
        };

        sqlx::query(
            "INSERT INTO creator_authorities (
                creator,
                auth_kind,
                granted_scopes,
                secret,
                session_expires_at,
                last_revalidated_at
            )
            VALUES ($1, $2, $3, $4, $5, $6)
            ON CONFLICT (creator) DO UPDATE SET
                auth_kind = EXCLUDED.auth_kind,
                granted_scopes = EXCLUDED.granted_scopes,
                secret = EXCLUDED.secret,
                session_expires_at = EXCLUDED.session_expires_at,
                last_revalidated_at = EXCLUDED.last_revalidated_at,
                updated_at = now()",
        )
        .bind(authority.creator.to_string())
        .bind(authority.auth_kind.as_str())
        .bind(Json(authority.granted_scopes))
        .bind(persisted_secret)
        .bind(authority.session_expires_at)
        .bind(authority.last_revalidated_at)
        .execute(&self.pool)
        .await
        .map_err(storage_error)?;

        Ok(())
    }

    async fn get_creator_authority(
        &self,
        creator: &CreatorPubky,
    ) -> Result<Option<CreatorAuthorityRecord>, ApplicationError> {
        let row = sqlx::query(
            "SELECT
                creator,
                auth_kind,
                granted_scopes,
                secret,
                session_expires_at,
                last_revalidated_at
            FROM creator_authorities
            WHERE creator = $1",
        )
        .bind(creator.to_string())
        .fetch_optional(&self.pool)
        .await
        .map_err(storage_error)?;

        row.map(|row| row_to_record(row, self.cipher.as_ref()))
            .transpose()
    }

    async fn delete_creator_authority(
        &self,
        creator: &CreatorPubky,
    ) -> Result<(), ApplicationError> {
        sqlx::query("DELETE FROM creator_authorities WHERE creator = $1")
            .bind(creator.to_string())
            .execute(&self.pool)
            .await
            .map_err(storage_error)?;
        Ok(())
    }
}

fn row_to_record(
    row: sqlx::postgres::PgRow,
    cipher: Option<&CreatorAuthoritySecretCipher>,
) -> Result<CreatorAuthorityRecord, ApplicationError> {
    let creator =
        CreatorPubky::from_str(&row.try_get::<String, _>("creator").map_err(storage_error)?)
            .map_err(|error| ApplicationError::Storage {
                message: format!("invalid creator authority creator stored in Postgres: {error}"),
            })?;
    let auth_kind = CreatorAuthorityAuthKind::from_str(
        &row.try_get::<String, _>("auth_kind")
            .map_err(storage_error)?,
    )?;
    let granted_scopes = row
        .try_get::<Json<Vec<String>>, _>("granted_scopes")
        .map_err(storage_error)?
        .0;
    let persisted_secret = row.try_get::<String, _>("secret").map_err(storage_error)?;
    let secret = match cipher {
        Some(cipher) => cipher.decrypt(&creator, auth_kind, &persisted_secret)?,
        None => CreatorAuthoritySecret::new(persisted_secret),
    };

    Ok(CreatorAuthorityRecord {
        creator,
        auth_kind,
        granted_scopes,
        secret,
        session_expires_at: row.try_get("session_expires_at").map_err(storage_error)?,
        last_revalidated_at: row.try_get("last_revalidated_at").map_err(storage_error)?,
    })
}

fn creator_authority_secret_aad(
    creator: &CreatorPubky,
    auth_kind: CreatorAuthorityAuthKind,
) -> String {
    format!(
        "creator_authority_secret:v1:{creator}:{}",
        auth_kind.as_str()
    )
}

fn decrypt_error() -> ApplicationError {
    ApplicationError::CreatorAuthoritySecret {
        message: "failed to decrypt creator authority secret".to_owned(),
    }
}

fn invalid_encryption_key() -> ApplicationError {
    ApplicationError::CreatorAuthoritySecret {
        message: "invalid creator authority encryption key".to_owned(),
    }
}

fn storage_error(error: sqlx::Error) -> ApplicationError {
    ApplicationError::Storage {
        message: error.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use base64::Engine;
    use base64::engine::general_purpose::URL_SAFE_NO_PAD;
    use sqlx::Row;
    use time::macros::datetime;

    use locks_core::ids::CreatorPubky;

    use super::{CreatorAuthoritySecretCipher, PostgresCreatorAuthorityStore};
    use crate::application::errors::ApplicationError;
    use crate::application::models::{
        CreatorAuthorityAuthKind, CreatorAuthorityRecord, CreatorAuthoritySecret,
    };
    use crate::application::ports::CreatorAuthorityStore;
    use crate::infrastructure::postgres::testing::TestDatabase;

    #[tokio::test]
    async fn upsert_read_delete_and_missing_semantics_match_port_contract() {
        let database = TestDatabase::create().await;
        let store = PostgresCreatorAuthorityStore::new(database.pool().clone());
        let creator = creator();
        let record = creator_authority_record("legacy-cookie-session-secret");

        assert_eq!(store.get_creator_authority(&creator).await.unwrap(), None);

        store
            .upsert_creator_authority(record.clone())
            .await
            .unwrap();
        assert_eq!(
            store.get_creator_authority(&creator).await.unwrap(),
            Some(record.clone())
        );

        let replacement = CreatorAuthorityRecord {
            auth_kind: CreatorAuthorityAuthKind::Grant,
            granted_scopes: vec!["/pub/locks.app/:rw".to_owned()],
            secret: CreatorAuthoritySecret::new("grant-credential-secret"),
            session_expires_at: None,
            last_revalidated_at: Some(datetime!(2026-05-29 12:30:00 UTC)),
            ..record
        };
        store
            .upsert_creator_authority(replacement.clone())
            .await
            .unwrap();
        assert_eq!(
            store.get_creator_authority(&creator).await.unwrap(),
            Some(replacement)
        );

        store.delete_creator_authority(&creator).await.unwrap();
        store.delete_creator_authority(&creator).await.unwrap();
        assert_eq!(store.get_creator_authority(&creator).await.unwrap(), None);

        database.cleanup().await;
    }

    #[tokio::test]
    async fn record_survives_store_recreation_and_debug_output_redacts_secret() {
        let database = TestDatabase::create().await;
        let original_store = PostgresCreatorAuthorityStore::new(database.pool().clone());
        let recreated_store = PostgresCreatorAuthorityStore::new(database.pool().clone());
        let creator = creator();
        let record = creator_authority_record("legacy-cookie-session-secret");

        original_store
            .upsert_creator_authority(record.clone())
            .await
            .unwrap();

        let loaded = recreated_store
            .get_creator_authority(&creator)
            .await
            .unwrap()
            .expect("stored creator authority exists");
        assert_eq!(loaded, record);
        assert_eq!(
            loaded.secret.expose_secret(),
            "legacy-cookie-session-secret"
        );

        let debug = format!("{loaded:?}");
        assert!(debug.contains("CreatorAuthorityRecord"));
        assert!(debug.contains("<redacted>"));
        assert!(!debug.contains("legacy-cookie-session-secret"));

        database.cleanup().await;
    }

    #[test]
    fn creator_authority_secret_cipher_accepts_32_byte_base64url_key() {
        let encoded = URL_SAFE_NO_PAD.encode([7u8; 32]);

        let cipher = CreatorAuthoritySecretCipher::from_base64url_key(&encoded).unwrap();

        let debug = format!("{cipher:?}");
        assert!(debug.contains("<redacted>"));
        assert!(!debug.contains(&encoded));
    }

    #[test]
    fn creator_authority_secret_cipher_rejects_invalid_or_wrong_length_keys_without_leaking_input()
    {
        for value in ["not-base64***", &URL_SAFE_NO_PAD.encode([7u8; 31])] {
            let error = CreatorAuthoritySecretCipher::from_base64url_key(value).unwrap_err();

            assert_eq!(
                error,
                ApplicationError::CreatorAuthoritySecret {
                    message: "invalid creator authority encryption key".to_owned(),
                }
            );
            let debug = format!("{error:?}");
            assert!(!debug.contains(value));
        }
    }

    #[tokio::test]
    async fn encrypted_store_persists_ciphertext_not_plaintext_and_reads_original_secret() {
        let database = TestDatabase::create().await;
        let store = PostgresCreatorAuthorityStore::new_encrypted(
            database.pool().clone(),
            CreatorAuthoritySecretCipher::new([7; 32]),
        );
        let creator = creator();
        let record = creator_authority_record("legacy-cookie-session-secret");

        store
            .upsert_creator_authority(record.clone())
            .await
            .unwrap();

        let stored_secret = raw_stored_secret(database.pool()).await;
        assert_ne!(stored_secret, "legacy-cookie-session-secret");
        assert!(stored_secret.starts_with("v1.xchacha20poly1305:"));
        assert!(!stored_secret.contains("legacy-cookie-session-secret"));

        let loaded = store
            .get_creator_authority(&creator)
            .await
            .unwrap()
            .expect("stored creator authority exists");
        assert_eq!(loaded, record);
        assert_eq!(
            loaded.secret.expose_secret(),
            "legacy-cookie-session-secret"
        );

        database.cleanup().await;
    }

    #[tokio::test]
    async fn encrypted_store_wrong_key_returns_secret_free_error() {
        let database = TestDatabase::create().await;
        let writer = PostgresCreatorAuthorityStore::new_encrypted(
            database.pool().clone(),
            CreatorAuthoritySecretCipher::new([7; 32]),
        );
        let reader = PostgresCreatorAuthorityStore::new_encrypted(
            database.pool().clone(),
            CreatorAuthoritySecretCipher::new([8; 32]),
        );
        let record = creator_authority_record("legacy-cookie-session-secret");

        writer.upsert_creator_authority(record).await.unwrap();
        let stored_secret = raw_stored_secret(database.pool()).await;

        let error = reader
            .get_creator_authority(&creator())
            .await
            .expect_err("wrong key cannot decrypt stored creator authority secret");

        assert_eq!(
            error,
            ApplicationError::CreatorAuthoritySecret {
                message: "failed to decrypt creator authority secret".to_owned(),
            }
        );
        let debug = format!("{error:?}");
        assert!(!debug.contains("legacy-cookie-session-secret"));
        assert!(!debug.contains(&stored_secret));
        assert!(!debug.contains('7'));
        assert!(!debug.contains('8'));

        database.cleanup().await;
    }

    #[tokio::test]
    async fn invalid_auth_kind_in_database_returns_domain_error() {
        let database = TestDatabase::create().await;
        let store = PostgresCreatorAuthorityStore::new(database.pool().clone());
        let creator = creator();
        let record = creator_authority_record("legacy-cookie-session-secret");

        store.upsert_creator_authority(record).await.unwrap();
        sqlx::query("UPDATE creator_authorities SET auth_kind = 'cookie' WHERE creator = $1")
            .bind(creator.to_string())
            .execute(database.pool())
            .await
            .expect("corrupt auth_kind for test");

        assert_eq!(
            store.get_creator_authority(&creator).await,
            Err(ApplicationError::InvalidCreatorAuthorityAuthKind {
                auth_kind: "cookie".to_owned(),
            })
        );

        database.cleanup().await;
    }

    #[tokio::test]
    async fn stores_granted_scopes_as_json_array() {
        let database = TestDatabase::create().await;
        let store = PostgresCreatorAuthorityStore::new(database.pool().clone());
        let record = creator_authority_record("legacy-cookie-session-secret");

        store.upsert_creator_authority(record).await.unwrap();

        let stored_scopes = sqlx::query("SELECT granted_scopes FROM creator_authorities")
            .fetch_one(database.pool())
            .await
            .expect("query stored granted scopes")
            .try_get::<serde_json::Value, _>("granted_scopes")
            .expect("granted_scopes is json");

        assert_eq!(
            stored_scopes,
            serde_json::json!(["/pub/locks.app/:rw", "/priv/locks.app/:rw"])
        );

        database.cleanup().await;
    }

    async fn raw_stored_secret(pool: &sqlx::PgPool) -> String {
        sqlx::query("SELECT secret FROM creator_authorities")
            .fetch_one(pool)
            .await
            .expect("query stored creator authority secret")
            .try_get::<String, _>("secret")
            .expect("secret is text")
    }

    fn creator_authority_record(secret: &str) -> CreatorAuthorityRecord {
        CreatorAuthorityRecord {
            creator: creator(),
            auth_kind: CreatorAuthorityAuthKind::LegacyCookie,
            granted_scopes: vec![
                "/pub/locks.app/:rw".to_owned(),
                "/priv/locks.app/:rw".to_owned(),
            ],
            secret: CreatorAuthoritySecret::new(secret),
            session_expires_at: Some(datetime!(2026-05-29 12:15:00 UTC)),
            last_revalidated_at: Some(datetime!(2026-05-29 12:00:00 UTC)),
        }
    }

    fn creator() -> CreatorPubky {
        CreatorPubky::from_str("pubkytkrq8zmwb8a3m9k15csu3q17qmfgqnp9dskbrg9uq1rydpyxp7qy").unwrap()
    }
}
