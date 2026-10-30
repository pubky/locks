CREATE TABLE folded_auth_attempts (
    creator TEXT NOT NULL,
    bundle_id TEXT NOT NULL,
    generation BIGINT NOT NULL CHECK (generation > 0),
    state_hash BYTEA PRIMARY KEY CHECK (octet_length(state_hash) = 32),
    status TEXT NOT NULL CHECK (status IN ('pending', 'accepted', 'expired', 'restart_required')),
    submitted_proof_bundle JSONB NOT NULL,
    pubky_lock_resource TEXT NOT NULL,
    lock_hash BYTEA NOT NULL CHECK (octet_length(lock_hash) = 32),
    verification_window_seconds BIGINT NOT NULL
        CHECK (verification_window_seconds BETWEEN 60 AND 604800),
    graph_policy JSONB NOT NULL,
    encrypted_flow_secret TEXT,
    created_at TIMESTAMPTZ NOT NULL,
    expires_at TIMESTAMPTZ NOT NULL,
    terminal_at TIMESTAMPTZ,
    purge_after TIMESTAMPTZ,
    task_id UUID REFERENCES verification_tasks(task_id),
    UNIQUE (creator, bundle_id, generation),
    CHECK (expires_at = created_at + INTERVAL '10 minutes'),
    CHECK (
        (status = 'pending'
            AND encrypted_flow_secret IS NOT NULL
            AND terminal_at IS NULL
            AND purge_after IS NULL
            AND task_id IS NULL)
        OR
        (status = 'accepted'
            AND encrypted_flow_secret IS NULL
            AND terminal_at IS NOT NULL
            AND purge_after IS NOT NULL
            AND task_id IS NOT NULL)
        OR
        (status IN ('expired', 'restart_required')
            AND encrypted_flow_secret IS NULL
            AND terminal_at IS NOT NULL
            AND purge_after IS NOT NULL
            AND task_id IS NULL)
    )
);

CREATE UNIQUE INDEX folded_auth_attempts_one_pending_per_handle
    ON folded_auth_attempts (creator, bundle_id)
    WHERE status = 'pending';

CREATE INDEX folded_auth_attempts_terminal_purge_idx
    ON folded_auth_attempts (purge_after)
    WHERE status <> 'pending';
