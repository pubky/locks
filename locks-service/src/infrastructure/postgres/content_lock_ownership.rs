use std::collections::BTreeSet;
use std::str::FromStr;

use async_trait::async_trait;
use locks_core::ids::{CreatorPubky, LockId};
use sqlx::PgPool;

use crate::application::errors::ApplicationError;
use crate::application::models::{ContentLockOwnership, ContentLockOwnershipStatus};
use crate::application::ports::ContentLockOwnershipRepository;

/// PostgreSQL-backed exclusive guarded-path ownership repository.
#[derive(Debug, Clone)]
pub struct PostgresContentLockOwnershipRepository {
    pool: PgPool,
}

impl PostgresContentLockOwnershipRepository {
    /// Creates an ownership repository backed by the supplied pool.
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }
}

#[async_trait]
impl ContentLockOwnershipRepository for PostgresContentLockOwnershipRepository {
    async fn reserve_paths(
        &self,
        creator: &CreatorPubky,
        guarded_paths: &[String],
        lock_id: &LockId,
    ) -> Result<(), ApplicationError> {
        let creator = creator.to_string();
        let lock_id = lock_id.to_string();
        let mut transaction = self.pool.begin().await.map_err(map_sqlx_error)?;

        for guarded_path in sorted_unique_paths(guarded_paths) {
            let insert = sqlx::query(
                r#"
                INSERT INTO content_lock_ownership (creator, guarded_path, lock_id, status)
                VALUES ($1, $2, $3, 'reserved')
                ON CONFLICT (creator, guarded_path) DO NOTHING
                "#,
            )
            .bind(&creator)
            .bind(guarded_path)
            .bind(&lock_id)
            .execute(&mut *transaction)
            .await
            .map_err(map_sqlx_error)?;

            let (existing_lock_id, existing_status) = sqlx::query_as::<_, (String, String)>(
                r#"
                SELECT lock_id, status
                FROM content_lock_ownership
                WHERE creator = $1 AND guarded_path = $2
                "#,
            )
            .bind(&creator)
            .bind(guarded_path)
            .fetch_one(&mut *transaction)
            .await
            .map_err(map_sqlx_error)?;
            if existing_lock_id != lock_id
                || (insert.rows_affected() == 0 && existing_status == "reserved")
            {
                return Err(ApplicationError::ContentLockPathConflict {
                    guarded_path: guarded_path.to_owned(),
                });
            }
        }

        transaction.commit().await.map_err(map_sqlx_error)
    }

    async fn mark_paths_published(
        &self,
        creator: &CreatorPubky,
        guarded_paths: &[String],
        lock_id: &LockId,
    ) -> Result<(), ApplicationError> {
        let creator = creator.to_string();
        let lock_id = lock_id.to_string();
        let mut transaction = self.pool.begin().await.map_err(map_sqlx_error)?;

        for guarded_path in sorted_unique_paths(guarded_paths) {
            let result = sqlx::query(
                r#"
                UPDATE content_lock_ownership
                SET status = 'published'
                WHERE creator = $1 AND guarded_path = $2 AND lock_id = $3
                "#,
            )
            .bind(&creator)
            .bind(guarded_path)
            .bind(&lock_id)
            .execute(&mut *transaction)
            .await
            .map_err(map_sqlx_error)?;
            if result.rows_affected() == 0 {
                let existing_lock_id = sqlx::query_scalar::<_, String>(
                    r#"
                    SELECT lock_id
                    FROM content_lock_ownership
                    WHERE creator = $1 AND guarded_path = $2
                    "#,
                )
                .bind(&creator)
                .bind(guarded_path)
                .fetch_optional(&mut *transaction)
                .await
                .map_err(map_sqlx_error)?;
                return match existing_lock_id {
                    Some(_) => Err(ApplicationError::ContentLockPathConflict {
                        guarded_path: guarded_path.to_owned(),
                    }),
                    None => Err(ApplicationError::MissingRecord {
                        record: "content_lock_ownership",
                    }),
                };
            }
        }

        transaction.commit().await.map_err(map_sqlx_error)
    }

    async fn compensate_reserved_paths(
        &self,
        creator: &CreatorPubky,
        guarded_paths: &[String],
        lock_id: &LockId,
    ) -> Result<(), ApplicationError> {
        let creator = creator.to_string();
        let lock_id = lock_id.to_string();
        let mut transaction = self.pool.begin().await.map_err(map_sqlx_error)?;

        for guarded_path in sorted_unique_paths(guarded_paths) {
            sqlx::query(
                r#"
                DELETE FROM content_lock_ownership
                WHERE creator = $1
                  AND guarded_path = $2
                  AND lock_id = $3
                  AND status = 'reserved'
                "#,
            )
            .bind(&creator)
            .bind(guarded_path)
            .bind(&lock_id)
            .execute(&mut *transaction)
            .await
            .map_err(map_sqlx_error)?;
        }

        transaction.commit().await.map_err(map_sqlx_error)
    }

    async fn get_path_ownership(
        &self,
        creator: &CreatorPubky,
        guarded_path: &str,
    ) -> Result<Option<ContentLockOwnership>, ApplicationError> {
        let row = sqlx::query_as::<_, (String, String)>(
            r#"
            SELECT lock_id, status
            FROM content_lock_ownership
            WHERE creator = $1 AND guarded_path = $2
            "#,
        )
        .bind(creator.to_string())
        .bind(guarded_path)
        .fetch_optional(&self.pool)
        .await
        .map_err(map_sqlx_error)?;

        row.map(|(lock_id, status)| {
            Ok(ContentLockOwnership {
                creator: creator.clone(),
                guarded_path: guarded_path.to_owned(),
                lock_id: LockId::from_str(&lock_id).map_err(|error| ApplicationError::Storage {
                    message: format!("invalid stored content lock ownership Lock ID: {error}"),
                })?,
                status: ContentLockOwnershipStatus::from_storage(&status)?,
            })
        })
        .transpose()
    }
}

fn sorted_unique_paths(guarded_paths: &[String]) -> Vec<&str> {
    guarded_paths
        .iter()
        .map(String::as_str)
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect()
}

fn map_sqlx_error(error: sqlx::Error) -> ApplicationError {
    ApplicationError::Storage {
        message: error.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;
    use std::sync::Arc;

    use locks_core::ids::{CreatorPubky, LockHash, LockId};
    use tokio::sync::Barrier;

    use super::PostgresContentLockOwnershipRepository;
    use crate::application::errors::ApplicationError;
    use crate::application::models::ContentLockOwnershipStatus;
    use crate::application::ports::ContentLockOwnershipRepository;
    use crate::infrastructure::postgres::testing::TestDatabase;

    #[tokio::test]
    async fn reserved_path_blocks_retry_and_conflicting_multi_path_request_is_atomic() {
        let database = TestDatabase::create().await;
        let store = PostgresContentLockOwnershipRepository::new(database.pool().clone());
        let creator = creator();
        let first_lock = lock_id(1);
        let second_lock = lock_id(2);
        let owned_paths = paths(&["a.txt", "b.txt"]);

        store
            .reserve_paths(&creator, &owned_paths, &first_lock)
            .await
            .unwrap();
        assert_eq!(
            store
                .reserve_paths(&creator, &owned_paths, &first_lock)
                .await,
            Err(ApplicationError::ContentLockPathConflict {
                guarded_path: owned_paths[0].clone(),
            })
        );

        let conflicting_paths = paths(&["c.txt", "b.txt"]);
        assert_eq!(
            store
                .reserve_paths(&creator, &conflicting_paths, &second_lock)
                .await,
            Err(ApplicationError::ContentLockPathConflict {
                guarded_path: owned_paths[1].clone(),
            })
        );
        assert_eq!(
            store
                .get_path_ownership(&creator, &conflicting_paths[0])
                .await
                .unwrap(),
            None
        );
        for guarded_path in &owned_paths {
            let ownership = store
                .get_path_ownership(&creator, guarded_path)
                .await
                .unwrap()
                .unwrap();
            assert_eq!(ownership.lock_id, first_lock);
            assert_eq!(ownership.status, ContentLockOwnershipStatus::Reserved);
        }
        store
            .reserve_paths(&second_creator(), &owned_paths, &second_lock)
            .await
            .unwrap();
        assert_eq!(
            store
                .get_path_ownership(&second_creator(), &owned_paths[0])
                .await
                .unwrap()
                .unwrap()
                .lock_id,
            second_lock
        );

        database.cleanup().await;
    }

    #[tokio::test]
    async fn concurrent_competing_reservations_choose_one_owner_without_partial_rows() {
        let database = TestDatabase::create().await;
        let store = PostgresContentLockOwnershipRepository::new(database.pool().clone());
        let creator = creator();
        let first_lock = lock_id(1);
        let second_lock = lock_id(2);
        let shared_path = "/priv/locks.app/content/a-shared.txt".to_owned();
        let first_only_path = "/priv/locks.app/content/b-first.txt".to_owned();
        let second_only_path = "/priv/locks.app/content/c-second.txt".to_owned();
        let barrier = Arc::new(Barrier::new(3));

        let first_store = store.clone();
        let first_creator = creator.clone();
        let first_paths = vec![shared_path.clone(), first_only_path.clone()];
        let first_task_lock = first_lock.clone();
        let first_barrier = barrier.clone();
        let first = tokio::spawn(async move {
            first_barrier.wait().await;
            first_store
                .reserve_paths(&first_creator, &first_paths, &first_task_lock)
                .await
        });

        let second_store = store.clone();
        let second_creator = creator.clone();
        let second_paths = vec![shared_path.clone(), second_only_path.clone()];
        let second_task_lock = second_lock.clone();
        let second_barrier = barrier.clone();
        let second = tokio::spawn(async move {
            second_barrier.wait().await;
            second_store
                .reserve_paths(&second_creator, &second_paths, &second_task_lock)
                .await
        });

        barrier.wait().await;
        let first_result = first.await.unwrap();
        let second_result = second.await.unwrap();
        let shared_owner = store
            .get_path_ownership(&creator, &shared_path)
            .await
            .unwrap()
            .unwrap();

        match (first_result, second_result, shared_owner.lock_id) {
            (Ok(()), Err(ApplicationError::ContentLockPathConflict { guarded_path }), owner)
                if guarded_path == shared_path && owner == first_lock =>
            {
                assert!(
                    store
                        .get_path_ownership(&creator, &first_only_path)
                        .await
                        .unwrap()
                        .is_some()
                );
                assert_eq!(
                    store
                        .get_path_ownership(&creator, &second_only_path)
                        .await
                        .unwrap(),
                    None
                );
            }
            (Err(ApplicationError::ContentLockPathConflict { guarded_path }), Ok(()), owner)
                if guarded_path == shared_path && owner == second_lock =>
            {
                assert_eq!(
                    store
                        .get_path_ownership(&creator, &first_only_path)
                        .await
                        .unwrap(),
                    None
                );
                assert!(
                    store
                        .get_path_ownership(&creator, &second_only_path)
                        .await
                        .unwrap()
                        .is_some()
                );
            }
            results => panic!("expected exactly one complete reservation, got {results:?}"),
        }

        database.cleanup().await;
    }

    #[tokio::test]
    async fn compensation_removes_only_reserved_rows_and_published_ownership_is_durable() {
        let database = TestDatabase::create().await;
        let store = PostgresContentLockOwnershipRepository::new(database.pool().clone());
        let recreated = PostgresContentLockOwnershipRepository::new(database.pool().clone());
        let creator = creator();
        let lock_id = lock_id(1);
        let guarded_paths = paths(&["a.txt"]);

        store
            .reserve_paths(&creator, &guarded_paths, &lock_id)
            .await
            .unwrap();
        store
            .compensate_reserved_paths(&creator, &guarded_paths, &lock_id)
            .await
            .unwrap();
        assert_eq!(
            store
                .get_path_ownership(&creator, &guarded_paths[0])
                .await
                .unwrap(),
            None
        );

        store
            .reserve_paths(&creator, &guarded_paths, &lock_id)
            .await
            .unwrap();
        store
            .mark_paths_published(&creator, &guarded_paths, &lock_id)
            .await
            .unwrap();
        store
            .reserve_paths(&creator, &guarded_paths, &lock_id)
            .await
            .unwrap();
        store
            .compensate_reserved_paths(&creator, &guarded_paths, &lock_id)
            .await
            .unwrap();

        let ownership = recreated
            .get_path_ownership(&creator, &guarded_paths[0])
            .await
            .unwrap()
            .unwrap();
        assert_eq!(ownership.lock_id, lock_id);
        assert_eq!(ownership.status, ContentLockOwnershipStatus::Published);

        database.cleanup().await;
    }

    fn creator() -> CreatorPubky {
        CreatorPubky::from_str("pubkytkrq8zmwb8a3m9k15csu3q17qmfgqnp9dskbrg9uq1rydpyxp7qy").unwrap()
    }

    fn second_creator() -> CreatorPubky {
        CreatorPubky::from_str("pubky7ir1ttte48bcp4zjychjyscicrwi1j34mtt91ptsafdbjmr8g9eo").unwrap()
    }

    fn lock_id(byte: u8) -> LockId {
        LockId::from_hash(LockHash::from_bytes([byte; 32]))
    }

    fn paths(names: &[&str]) -> Vec<String> {
        names
            .iter()
            .map(|name| format!("/priv/locks.app/content/{name}"))
            .collect()
    }
}
