CREATE TABLE verification_tasks (
    task_id UUID PRIMARY KEY,
    status TEXT NOT NULL,
    submitted_proof_bundle JSONB NOT NULL,
    submitted_at TIMESTAMPTZ NOT NULL,
    started_at TIMESTAMPTZ,
    completed_at TIMESTAMPTZ,
    failure_message TEXT,
    claimed_by TEXT,
    claim_expires_at TIMESTAMPTZ,
    attempt_count INTEGER NOT NULL DEFAULT 0,
    last_attempt_error TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CHECK (status IN ('pending', 'in_progress', 'completed', 'failed', 'expired')),
    CHECK (attempt_count >= 0)
);

CREATE INDEX verification_tasks_pending_idx
ON verification_tasks (submitted_at)
WHERE status = 'pending';

CREATE INDEX verification_tasks_expired_claim_idx
ON verification_tasks (claim_expires_at)
WHERE status = 'in_progress' AND claim_expires_at IS NOT NULL;

CREATE TABLE access_credentials (
    lookup_key BYTEA PRIMARY KEY,
    creator TEXT NOT NULL,
    bundle_id TEXT NOT NULL,
    expires_at TIMESTAMPTZ NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX access_credentials_expires_at_idx
ON access_credentials (expires_at);
