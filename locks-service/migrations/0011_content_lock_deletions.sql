CREATE TABLE content_lock_deletion_jobs (
    job_id UUID PRIMARY KEY,
    creator TEXT NOT NULL,
    lock_id TEXT NOT NULL,
    frozen_content_lock JSONB NOT NULL,
    deletion_started_at TIMESTAMPTZ NOT NULL,
    state TEXT NOT NULL DEFAULT 'queued',
    phase TEXT NOT NULL DEFAULT 'withdraw',
    attempt_count BIGINT NOT NULL DEFAULT 0,
    next_attempt_at TIMESTAMPTZ,
    force_requested_at TIMESTAMPTZ,
    failure_code TEXT,
    claimed_by TEXT,
    claim_token UUID,
    claim_expires_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT content_lock_deletion_jobs_creator_lock_unique UNIQUE (creator, lock_id),
    CONSTRAINT content_lock_deletion_jobs_state_valid CHECK (
        state IN ('queued', 'running', 'completed', 'failed')
    ),
    CONSTRAINT content_lock_deletion_jobs_phase_valid CHECK (
        phase IN (
            'withdraw',
            'start_payment_drain',
            'drain_payments',
            'drain_existing_credentials',
            'issue_final_credentials',
            'drain_final_reads',
            'delete_content',
            'delete_tombstone',
            'purge_operational_state'
        )
    ),
    CONSTRAINT content_lock_deletion_jobs_attempt_count_valid CHECK (attempt_count >= 0),
    CONSTRAINT content_lock_deletion_jobs_claim_valid CHECK (
        (state = 'running'
            AND claimed_by IS NOT NULL
            AND claim_token IS NOT NULL
            AND claim_expires_at IS NOT NULL
            AND next_attempt_at IS NULL)
        OR
        (state <> 'running'
            AND claimed_by IS NULL
            AND claim_token IS NULL
            AND claim_expires_at IS NULL)
    ),
    CONSTRAINT content_lock_deletion_jobs_failure_valid CHECK (
        (state = 'failed' AND failure_code IN (
            'tombstone_missing',
            'tombstone_replaced',
            'retry_exhausted',
            'state_corrupt'
        ))
        OR
        (state <> 'failed' AND failure_code IS NULL)
    )
);

CREATE INDEX content_lock_deletion_jobs_due_idx
    ON content_lock_deletion_jobs (deletion_started_at)
    WHERE state IN ('queued', 'running');

CREATE TABLE content_lock_force_deletion_receipts (
    creator TEXT NOT NULL,
    lock_id TEXT NOT NULL,
    forced_at TIMESTAMPTZ NOT NULL,
    CONSTRAINT content_lock_force_deletion_receipts_pkey PRIMARY KEY (creator, lock_id)
);
