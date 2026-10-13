-- Accepted one-time reset of disposable Locks prototype state.
--
-- This migration is deployed together with the Paykit Server reset. SQLx records
-- it transactionally, so later Locks Server starts do not repeat the reset.
-- Keep the table list explicit: the migration must affect only Locks-owned
-- private runtime tables and must not drop the schema or _sqlx_migrations.
TRUNCATE TABLE
    verification_tasks,
    access_credentials,
    creator_authorities,
    pending_creator_connect_flows,
    frontend_session_codes,
    frontend_sessions;
