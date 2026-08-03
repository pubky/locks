use async_trait::async_trait;
use sqlx::types::Json;
use sqlx::{PgPool, Row};

use crate::application::errors::ApplicationError;
use crate::application::models::{
    CreatorConnectAuthorizationUrl, CreatorConnectFlowId, PendingCreatorConnectFlowRecord,
};
use crate::application::ports::CreatorConnectFlowStore;

/// Postgres-backed store for short-lived pending legacy creator connect flows.
#[derive(Debug, Clone)]
pub struct PostgresCreatorConnectFlowStore {
    pool: PgPool,
}

impl PostgresCreatorConnectFlowStore {
    /// Creates a store backed by the provided migrated Postgres pool.
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }
}

#[async_trait]
impl CreatorConnectFlowStore for PostgresCreatorConnectFlowStore {
    async fn insert_pending_creator_connect_flow(
        &self,
        record: PendingCreatorConnectFlowRecord,
    ) -> Result<(), ApplicationError> {
        let result = sqlx::query(
            "INSERT INTO pending_creator_connect_flows (
                flow_id,
                return_to,
                state,
                authorization_url,
                requested_scopes,
                created_at,
                expires_at
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7)
            ON CONFLICT (flow_id) DO NOTHING",
        )
        .bind(record.flow_id.as_str())
        .bind(record.return_to)
        .bind(record.state)
        .bind(record.authorization_url.expose_url())
        .bind(Json(record.requested_scopes))
        .bind(record.created_at)
        .bind(record.expires_at)
        .execute(&self.pool)
        .await
        .map_err(storage_error)?;

        if result.rows_affected() == 0 {
            return Err(ApplicationError::DuplicateRecord {
                record: "pending_creator_connect_flow",
            });
        }

        Ok(())
    }

    async fn get_pending_creator_connect_flow(
        &self,
        flow_id: &CreatorConnectFlowId,
    ) -> Result<Option<PendingCreatorConnectFlowRecord>, ApplicationError> {
        let row = sqlx::query(
            "SELECT
                flow_id,
                return_to,
                state,
                authorization_url,
                requested_scopes,
                created_at,
                expires_at
            FROM pending_creator_connect_flows
            WHERE flow_id = $1",
        )
        .bind(flow_id.as_str())
        .fetch_optional(&self.pool)
        .await
        .map_err(storage_error)?;

        row.map(row_to_record).transpose()
    }

    async fn delete_pending_creator_connect_flow(
        &self,
        flow_id: &CreatorConnectFlowId,
    ) -> Result<(), ApplicationError> {
        sqlx::query("DELETE FROM pending_creator_connect_flows WHERE flow_id = $1")
            .bind(flow_id.as_str())
            .execute(&self.pool)
            .await
            .map_err(storage_error)?;
        Ok(())
    }
}

fn row_to_record(
    row: sqlx::postgres::PgRow,
) -> Result<PendingCreatorConnectFlowRecord, ApplicationError> {
    let flow_id =
        CreatorConnectFlowId::new(row.try_get::<String, _>("flow_id").map_err(storage_error)?);
    let requested_scopes = row
        .try_get::<Json<Vec<String>>, _>("requested_scopes")
        .map_err(storage_error)?
        .0;

    Ok(PendingCreatorConnectFlowRecord {
        flow_id,
        return_to: row.try_get("return_to").map_err(storage_error)?,
        state: row.try_get("state").map_err(storage_error)?,
        authorization_url: CreatorConnectAuthorizationUrl::new(
            row.try_get::<String, _>("authorization_url")
                .map_err(storage_error)?,
        ),
        requested_scopes,
        created_at: row.try_get("created_at").map_err(storage_error)?,
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
    use super::PostgresCreatorConnectFlowStore;
    use crate::application::models::{
        CreatorConnectAuthorizationUrl, CreatorConnectFlowId, PendingCreatorConnectFlowRecord,
    };
    use crate::application::ports::CreatorConnectFlowStore;
    use crate::infrastructure::postgres::testing::TestDatabase;
    use time::macros::datetime;

    #[tokio::test]
    async fn insert_get_delete_and_missing_semantics_match_port_contract() {
        let database = TestDatabase::create().await;
        let store = PostgresCreatorConnectFlowStore::new(database.pool().clone());
        let flow_id = CreatorConnectFlowId::new("flow-123");
        let record = pending_flow_record(flow_id.clone());

        assert_eq!(
            store
                .get_pending_creator_connect_flow(&flow_id)
                .await
                .unwrap(),
            None
        );

        store
            .insert_pending_creator_connect_flow(record.clone())
            .await
            .unwrap();
        assert_eq!(
            store
                .get_pending_creator_connect_flow(&flow_id)
                .await
                .unwrap(),
            Some(record.clone())
        );

        store
            .delete_pending_creator_connect_flow(&flow_id)
            .await
            .unwrap();
        store
            .delete_pending_creator_connect_flow(&flow_id)
            .await
            .unwrap();
        assert_eq!(
            store
                .get_pending_creator_connect_flow(&flow_id)
                .await
                .unwrap(),
            None
        );

        database.cleanup().await;
    }

    #[tokio::test]
    async fn record_survives_store_recreation_and_debug_output_redacts_authorization_url() {
        let database = TestDatabase::create().await;
        let original_store = PostgresCreatorConnectFlowStore::new(database.pool().clone());
        let recreated_store = PostgresCreatorConnectFlowStore::new(database.pool().clone());
        let flow_id = CreatorConnectFlowId::new("flow-123");
        let authorization_url = "pubkyauth://secret-flow-token";
        let record = PendingCreatorConnectFlowRecord {
            authorization_url: CreatorConnectAuthorizationUrl::new(authorization_url),
            ..pending_flow_record(flow_id.clone())
        };

        original_store
            .insert_pending_creator_connect_flow(record.clone())
            .await
            .unwrap();

        let loaded = recreated_store
            .get_pending_creator_connect_flow(&flow_id)
            .await
            .unwrap()
            .expect("stored pending flow");
        assert_eq!(loaded, record);
        assert_eq!(loaded.authorization_url.expose_url(), authorization_url);
        assert!(!format!("{loaded:?}").contains(authorization_url));

        database.cleanup().await;
    }

    fn pending_flow_record(flow_id: CreatorConnectFlowId) -> PendingCreatorConnectFlowRecord {
        PendingCreatorConnectFlowRecord {
            flow_id,
            return_to: "https://app.example/locks/callback".to_owned(),
            state: "state-123".to_owned(),
            authorization_url: CreatorConnectAuthorizationUrl::new("pubkyauth://secret-flow-token"),
            requested_scopes: vec![
                "/pub/locks.app/:rw".to_owned(),
                "/priv/locks.app/:rw".to_owned(),
            ],
            created_at: datetime!(2026-05-29 12:00:00 UTC),
            expires_at: datetime!(2026-05-29 12:05:00 UTC),
        }
    }
}
