CREATE TABLE creator_authorities (
    creator TEXT PRIMARY KEY,
    auth_kind TEXT NOT NULL,
    granted_scopes JSONB NOT NULL,
    secret TEXT NOT NULL,
    session_expires_at TIMESTAMPTZ,
    last_revalidated_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
