CREATE TABLE creator_authority_acquisitions (
    flow_id TEXT PRIMARY KEY,
    status TEXT NOT NULL,
    server_path TEXT NOT NULL,
    authorization_url TEXT NOT NULL,
    requested_capabilities JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL,
    expires_at TIMESTAMPTZ NOT NULL,
    completed_at TIMESTAMPTZ,
    failed_at TIMESTAMPTZ,
    failure_code TEXT,
    creator_pubky TEXT
);
