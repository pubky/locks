-- Browser Reader challenges are short-lived and single-use. Plain challenge
-- bytes are returned once and never persisted.
CREATE TABLE reader_auth_challenges (
    challenge_id UUID PRIMARY KEY,
    challenge_hash BYTEA NOT NULL CHECK (octet_length(challenge_hash) = 32),
    browser_origin TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL,
    expires_at TIMESTAMPTZ NOT NULL,
    consumed_at TIMESTAMPTZ,
    CHECK (created_at < expires_at),
    CHECK (consumed_at IS NULL OR consumed_at >= created_at)
);
CREATE INDEX reader_auth_challenges_expiry_idx ON reader_auth_challenges (expires_at);

-- Proof nonces remain fenced until grant expiry, independently of challenge or
-- local session cleanup.
CREATE TABLE reader_auth_proof_replays (
    grant_id TEXT NOT NULL,
    proof_nonce TEXT NOT NULL,
    expires_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (grant_id, proof_nonce)
);
CREATE INDEX reader_auth_proof_replays_expiry_idx ON reader_auth_proof_replays (expires_at);

-- Only token hashes are stored. Signed audience/origin/grant context is kept so
-- renewal can prove it does not switch identity or Lock Server boundary.
CREATE TABLE reader_sessions (
    session_id UUID PRIMARY KEY,
    reader TEXT NOT NULL,
    token_hash BYTEA NOT NULL UNIQUE CHECK (octet_length(token_hash) = 32),
    challenge_id UUID NOT NULL UNIQUE REFERENCES reader_auth_challenges(challenge_id),
    grant_id TEXT NOT NULL,
    grant_client_id TEXT NOT NULL,
    proof_nonce TEXT NOT NULL,
    lock_server_pubky TEXT NOT NULL,
    lock_server_origin TEXT NOT NULL,
    browser_origin TEXT NOT NULL,
    grant_expires_at TIMESTAMPTZ NOT NULL,
    idle_expires_at TIMESTAMPTZ NOT NULL,
    absolute_expires_at TIMESTAMPTZ NOT NULL,
    created_at TIMESTAMPTZ NOT NULL,
    renewed_from_session_id UUID REFERENCES reader_sessions(session_id) ON DELETE SET NULL,
    invalidated_at TIMESTAMPTZ,
    CHECK (created_at < grant_expires_at),
    CHECK (created_at < idle_expires_at),
    CHECK (created_at < absolute_expires_at),
    CHECK (idle_expires_at <= absolute_expires_at),
    CHECK (absolute_expires_at <= grant_expires_at)
);
CREATE INDEX reader_sessions_reader_idx ON reader_sessions (reader);
CREATE INDEX reader_sessions_expiry_idx ON reader_sessions (absolute_expires_at);
