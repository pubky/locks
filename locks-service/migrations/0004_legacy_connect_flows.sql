CREATE TABLE pending_creator_connect_flows (
    flow_id TEXT PRIMARY KEY,
    return_to TEXT NOT NULL,
    state TEXT NOT NULL,
    authorization_url TEXT NOT NULL,
    requested_scopes JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL,
    expires_at TIMESTAMPTZ NOT NULL
);

CREATE TABLE frontend_session_codes (
    code_hash BYTEA PRIMARY KEY,
    creator TEXT NOT NULL,
    state TEXT NOT NULL,
    return_to TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL,
    expires_at TIMESTAMPTZ NOT NULL,
    consumed_at TIMESTAMPTZ
);

CREATE TABLE frontend_sessions (
    token_hash BYTEA PRIMARY KEY,
    creator TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL,
    expires_at TIMESTAMPTZ NOT NULL
);
