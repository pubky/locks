use crate::application::errors::ApplicationError;

/// Errors raised by Postgres-backed infrastructure adapters.
#[derive(Debug, thiserror::Error)]
pub enum PostgresError {
    /// Postgres query, connection, pool, or row-decoding failure.
    #[error("postgres database error: {0}")]
    Database(#[from] sqlx::Error),

    /// Managed migration failure.
    #[error("postgres migration error: {0}")]
    Migration(#[from] sqlx::migrate::MigrateError),
}

impl From<PostgresError> for ApplicationError {
    fn from(error: PostgresError) -> Self {
        ApplicationError::Storage {
            message: error.to_string(),
        }
    }
}
