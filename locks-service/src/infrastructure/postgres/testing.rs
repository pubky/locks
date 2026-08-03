use std::env;

use sqlx::postgres::PgPoolOptions;
use sqlx::{Connection, Executor, PgConnection, PgPool};

use super::run_migrations;

const TEST_DATABASE_URL_ENV: &str = "TEST_DATABASE_URL";

pub(crate) struct TestDatabase {
    pool: PgPool,
    schema_name: String,
}

impl TestDatabase {
    pub(crate) async fn create() -> Self {
        let database_url = test_database_url();
        let schema_name = unique_schema_name();
        let mut admin_connection = PgConnection::connect(&database_url)
            .await
            .expect("connect to TEST_DATABASE_URL");

        create_schema(&mut admin_connection, &schema_name).await;
        let pool = isolated_schema_pool(&database_url, &schema_name).await;
        run_migrations(&pool)
            .await
            .expect("run migrations in isolated test schema");

        Self { pool, schema_name }
    }

    pub(crate) async fn admin_pool() -> PgPool {
        PgPoolOptions::new()
            .max_connections(1)
            .connect(&test_database_url())
            .await
            .expect("connect admin pool to TEST_DATABASE_URL")
    }

    pub(crate) fn pool(&self) -> &PgPool {
        &self.pool
    }

    pub(crate) fn schema_name(&self) -> &str {
        &self.schema_name
    }

    pub(crate) async fn cleanup(self) {
        self.pool.close().await;
        let mut admin_connection = PgConnection::connect(&test_database_url())
            .await
            .expect("connect to TEST_DATABASE_URL for cleanup");
        drop_schema(&mut admin_connection, &self.schema_name).await;
    }
}

async fn isolated_schema_pool(database_url: &str, schema_name: &str) -> PgPool {
    let schema_name = schema_name.to_owned();
    PgPoolOptions::new()
        .max_connections(5)
        .after_connect(move |connection, _metadata| {
            let schema_name = schema_name.clone();
            Box::pin(async move {
                connection
                    .execute(format!("SET search_path TO {schema_name}").as_str())
                    .await?;
                Ok(())
            })
        })
        .connect(database_url)
        .await
        .expect("connect isolated schema pool")
}

async fn create_schema(connection: &mut PgConnection, schema_name: &str) {
    connection
        .execute(format!("CREATE SCHEMA {schema_name}").as_str())
        .await
        .expect("create isolated schema");
}

async fn drop_schema(connection: &mut PgConnection, schema_name: &str) {
    connection
        .execute(format!("DROP SCHEMA IF EXISTS {schema_name} CASCADE").as_str())
        .await
        .expect("drop isolated schema");
}

fn test_database_url() -> String {
    env::var(TEST_DATABASE_URL_ENV)
        .expect("TEST_DATABASE_URL must be set to run explicit Postgres tests")
}

fn unique_schema_name() -> String {
    format!("locks_test_{}", uuid::Uuid::new_v4().simple())
}

#[cfg(test)]
mod tests {
    use sqlx::Row;

    use super::TestDatabase;

    #[tokio::test]
    async fn test_database_creates_migrated_isolated_schema_and_cleans_up() {
        let database = TestDatabase::create().await;
        let schema_name = database.schema_name().to_owned();

        let current_schema = sqlx::query("SELECT current_schema()")
            .fetch_one(database.pool())
            .await
            .expect("query current schema")
            .try_get::<String, _>(0)
            .expect("current_schema is text");
        assert_eq!(current_schema, schema_name);

        assert!(table_exists(database.pool(), "verification_tasks").await);
        assert!(table_exists(database.pool(), "access_credentials").await);
        assert!(table_exists(database.pool(), "creator_authorities").await);

        database.cleanup().await;

        assert_schema_was_dropped(&schema_name).await;
    }

    async fn table_exists(pool: &sqlx::PgPool, table_name: &str) -> bool {
        sqlx::query(
            "SELECT EXISTS (
                SELECT 1
                FROM information_schema.tables
                WHERE table_schema = current_schema()
                  AND table_name = $1
            )",
        )
        .bind(table_name)
        .fetch_one(pool)
        .await
        .expect("query table existence")
        .try_get::<bool, _>(0)
        .expect("exists result is bool")
    }

    async fn assert_schema_was_dropped(schema_name: &str) {
        let admin_pool = TestDatabase::admin_pool().await;
        let exists = sqlx::query(
            "SELECT EXISTS (SELECT 1 FROM information_schema.schemata WHERE schema_name = $1)",
        )
        .bind(schema_name)
        .fetch_one(&admin_pool)
        .await
        .expect("query schema existence")
        .try_get::<bool, _>(0)
        .expect("exists result is bool");
        admin_pool.close().await;

        assert!(!exists, "schema {schema_name} should have been dropped");
    }
}
