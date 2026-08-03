use std::str::FromStr;

use async_trait::async_trait;
use sqlx::{PgPool, Row};

use locks_core::ids::{BundleId, CreatorPubky};

use crate::application::errors::ApplicationError;
use crate::application::models::{AccessCredentialLookupKey, AccessCredentialRecord};
use crate::application::ports::AccessCredentialStore;

/// Postgres-backed store for issued access credential lookup records.
#[derive(Debug, Clone)]
pub struct PostgresAccessCredentialStore {
    pool: PgPool,
}

impl PostgresAccessCredentialStore {
    /// Creates a store backed by the provided migrated Postgres pool.
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }
}

#[async_trait]
impl AccessCredentialStore for PostgresAccessCredentialStore {
    async fn insert_access_credential(
        &self,
        lookup_key: AccessCredentialLookupKey,
        record: AccessCredentialRecord,
    ) -> Result<(), ApplicationError> {
        let result = sqlx::query(
            "INSERT INTO access_credentials (lookup_key, creator, bundle_id, expires_at)
            VALUES ($1, $2, $3, $4)
            ON CONFLICT (lookup_key) DO NOTHING",
        )
        .bind(lookup_key.as_bytes().as_slice())
        .bind(record.creator.to_string())
        .bind(record.bundle_id.to_string())
        .bind(record.expires_at)
        .execute(&self.pool)
        .await
        .map_err(storage_error)?;

        if result.rows_affected() == 0 {
            return Err(ApplicationError::DuplicateRecord {
                record: "access_credential",
            });
        }

        Ok(())
    }

    async fn get_access_credential(
        &self,
        lookup_key: &AccessCredentialLookupKey,
    ) -> Result<Option<AccessCredentialRecord>, ApplicationError> {
        let row = sqlx::query(
            "SELECT creator, bundle_id, expires_at
            FROM access_credentials
            WHERE lookup_key = $1",
        )
        .bind(lookup_key.as_bytes().as_slice())
        .fetch_optional(&self.pool)
        .await
        .map_err(storage_error)?;

        row.map(row_to_record).transpose()
    }

    async fn delete_access_credential(
        &self,
        lookup_key: &AccessCredentialLookupKey,
    ) -> Result<(), ApplicationError> {
        sqlx::query("DELETE FROM access_credentials WHERE lookup_key = $1")
            .bind(lookup_key.as_bytes().as_slice())
            .execute(&self.pool)
            .await
            .map_err(storage_error)?;
        Ok(())
    }
}

fn row_to_record(row: sqlx::postgres::PgRow) -> Result<AccessCredentialRecord, ApplicationError> {
    let creator =
        CreatorPubky::from_str(&row.try_get::<String, _>("creator").map_err(storage_error)?)
            .map_err(|error| ApplicationError::Storage {
                message: format!("invalid access credential creator stored in Postgres: {error}"),
            })?;
    let bundle_id = BundleId::from_str(
        &row.try_get::<String, _>("bundle_id")
            .map_err(storage_error)?,
    )
    .map_err(|error| ApplicationError::Storage {
        message: format!("invalid access credential bundle_id stored in Postgres: {error}"),
    })?;

    Ok(AccessCredentialRecord {
        creator,
        bundle_id,
        expires_at: row.try_get("expires_at").map_err(storage_error)?,
    })
}

fn storage_error(error: sqlx::Error) -> ApplicationError {
    ApplicationError::Storage {
        message: error.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use sqlx::Row;
    use time::macros::datetime;

    use locks_core::ids::{BundleId, CreatorPubky};

    use super::PostgresAccessCredentialStore;
    use crate::application::errors::ApplicationError;
    use crate::application::models::{
        AccessCredential, AccessCredentialLookupKey, AccessCredentialRecord,
    };
    use crate::application::ports::AccessCredentialStore;
    use crate::infrastructure::postgres::testing::TestDatabase;

    #[tokio::test]
    async fn insert_read_delete_and_duplicate_semantics_match_port_contract() {
        let database = TestDatabase::create().await;
        let store = PostgresAccessCredentialStore::new(database.pool().clone());
        let credential = AccessCredential::new("raw-bearer-credential");
        let lookup_key = AccessCredentialLookupKey::derive(&credential);
        let record = access_credential_record();

        assert_eq!(
            store.get_access_credential(&lookup_key).await.unwrap(),
            None
        );

        store
            .insert_access_credential(lookup_key.clone(), record.clone())
            .await
            .unwrap();
        assert_eq!(
            store.get_access_credential(&lookup_key).await.unwrap(),
            Some(record.clone())
        );
        assert_eq!(
            store
                .insert_access_credential(lookup_key.clone(), record)
                .await,
            Err(ApplicationError::DuplicateRecord {
                record: "access_credential",
            })
        );

        store.delete_access_credential(&lookup_key).await.unwrap();
        store.delete_access_credential(&lookup_key).await.unwrap();
        assert_eq!(
            store.get_access_credential(&lookup_key).await.unwrap(),
            None
        );

        database.cleanup().await;
    }

    #[tokio::test]
    async fn record_survives_store_recreation_and_persists_exact_lookup_key_without_raw_credential()
    {
        let database = TestDatabase::create().await;
        let original_store = PostgresAccessCredentialStore::new(database.pool().clone());
        let recreated_store = PostgresAccessCredentialStore::new(database.pool().clone());
        let raw_credential = "raw-bearer-credential";
        let credential = AccessCredential::new(raw_credential);
        let lookup_key = AccessCredentialLookupKey::derive(&credential);
        let record = access_credential_record();

        original_store
            .insert_access_credential(lookup_key.clone(), record.clone())
            .await
            .unwrap();

        assert_eq!(
            recreated_store
                .get_access_credential(&lookup_key)
                .await
                .unwrap(),
            Some(record)
        );
        assert_stored_lookup_key_is_exact(database.pool(), &lookup_key).await;
        assert_raw_credential_not_stored(database.pool(), raw_credential).await;

        database.cleanup().await;
    }

    async fn assert_stored_lookup_key_is_exact(
        pool: &sqlx::PgPool,
        lookup_key: &AccessCredentialLookupKey,
    ) {
        let stored_lookup_key = sqlx::query("SELECT lookup_key FROM access_credentials")
            .fetch_one(pool)
            .await
            .expect("query stored lookup key")
            .try_get::<Vec<u8>, _>("lookup_key")
            .expect("lookup_key is bytes");

        assert_eq!(stored_lookup_key.as_slice(), lookup_key.as_bytes());
    }

    async fn assert_raw_credential_not_stored(pool: &sqlx::PgPool, raw_credential: &str) {
        let raw_credential_present = sqlx::query(
            "SELECT EXISTS (
                SELECT 1
                FROM access_credentials
                WHERE creator = $1 OR bundle_id = $1 OR encode(lookup_key, 'escape') = $1
            )",
        )
        .bind(raw_credential)
        .fetch_one(pool)
        .await
        .expect("query raw credential absence")
        .try_get::<bool, _>(0)
        .expect("exists result is bool");

        assert!(!raw_credential_present);
    }

    fn access_credential_record() -> AccessCredentialRecord {
        AccessCredentialRecord {
            creator: CreatorPubky::from_str(
                "pubkytkrq8zmwb8a3m9k15csu3q17qmfgqnp9dskbrg9uq1rydpyxp7qy",
            )
            .unwrap(),
            bundle_id: BundleId::from_str("000G40R40M30E209185GR38E1W").unwrap(),
            expires_at: datetime!(2026-05-29 12:15:00 UTC),
        }
    }
}
