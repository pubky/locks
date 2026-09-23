DO $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM content_lock_deletion_jobs
        WHERE state IN ('queued', 'running', 'failed')
    ) THEN
        RAISE EXCEPTION USING
            MESSAGE = 'migration 0016 cannot classify pre-existing resumable deletion jobs; drain or explicitly reset pre-0016 deletion jobs before retrying',
            HINT = 'see docs/RUNTIME.md for the required drain/reset procedure';
    END IF;
END
$$;

ALTER TABLE content_lock_deletion_jobs
    ADD COLUMN final_issuance_started_at TIMESTAMPTZ,
    ADD COLUMN final_credential_issuance_deadline TIMESTAMPTZ,
    ADD COLUMN final_read_deadline TIMESTAMPTZ,
    ADD CONSTRAINT content_lock_deletion_jobs_final_window_shape CHECK (
        (final_issuance_started_at IS NULL
         AND final_credential_issuance_deadline IS NULL
         AND final_read_deadline IS NULL)
        OR
        (final_issuance_started_at IS NOT NULL
         AND final_credential_issuance_deadline IS NOT NULL
         AND final_read_deadline IS NOT NULL
         AND final_issuance_started_at < final_credential_issuance_deadline
         AND final_credential_issuance_deadline < final_read_deadline)
    );

ALTER TABLE content_lock_deletion_task_snapshot
    ADD COLUMN had_active_credential_at_cutoff BOOLEAN NOT NULL DEFAULT FALSE,
    ADD COLUMN final_credential_eligible_at TIMESTAMPTZ,
    ADD COLUMN final_credential_issued_at TIMESTAMPTZ,
    ADD CONSTRAINT content_lock_deletion_task_snapshot_final_eligibility_valid CHECK (
        final_credential_eligible_at IS NULL
        OR (
            had_active_credential_at_cutoff = FALSE
            AND paykit_admission_required = TRUE
            AND resolved_status = 'completed'
        )
    ),
    ADD CONSTRAINT content_lock_deletion_task_snapshot_final_issuance_valid CHECK (
        final_credential_issued_at IS NULL
        OR final_credential_eligible_at IS NOT NULL
    );

ALTER TABLE access_credentials
    ADD COLUMN deletion_job_id UUID
        REFERENCES content_lock_deletion_jobs(job_id) ON DELETE CASCADE;

CREATE INDEX access_credentials_deletion_active_idx
    ON access_credentials (deletion_job_id, expires_at)
    WHERE deletion_job_id IS NOT NULL;

CREATE TABLE content_lock_access_drain_credentials (
    credential_id UUID PRIMARY KEY,
    deletion_job_id UUID NOT NULL
        REFERENCES content_lock_deletion_jobs(job_id) ON DELETE CASCADE,
    lookup_key BYTEA NOT NULL UNIQUE
        REFERENCES access_credentials(lookup_key) ON DELETE CASCADE,
    creator TEXT NOT NULL,
    bundle_id TEXT NOT NULL,
    credential_kind TEXT NOT NULL,
    encrypted_bearer TEXT,
    issued_at TIMESTAMPTZ NOT NULL,
    expires_at TIMESTAMPTZ NOT NULL,
    CONSTRAINT content_lock_access_drain_credentials_kind_valid CHECK (
        credential_kind IN ('ordinary', 'final')
    ),
    CONSTRAINT content_lock_access_drain_credentials_envelope_valid CHECK (
        (credential_kind = 'ordinary' AND encrypted_bearer IS NULL)
        OR
        (credential_kind = 'final'
         AND encrypted_bearer IS NOT NULL
         AND encrypted_bearer LIKE 'v1.xchacha20poly1305:%')
    ),
    CONSTRAINT content_lock_access_drain_credentials_expiry_valid CHECK (
        issued_at < expires_at
    )
);

CREATE UNIQUE INDEX content_lock_access_drain_one_final_per_bundle_idx
    ON content_lock_access_drain_credentials (deletion_job_id, creator, bundle_id)
    WHERE credential_kind = 'final';

CREATE INDEX content_lock_access_drain_credentials_job_expiry_idx
    ON content_lock_access_drain_credentials (deletion_job_id, expires_at);

CREATE TABLE content_lock_access_drain_reads (
    credential_id UUID NOT NULL
        REFERENCES content_lock_access_drain_credentials(credential_id) ON DELETE CASCADE,
    guarded_path TEXT NOT NULL,
    claim_token UUID,
    claim_expires_at TIMESTAMPTZ,
    consumed_at TIMESTAMPTZ,
    CONSTRAINT content_lock_access_drain_reads_pkey
        PRIMARY KEY (credential_id, guarded_path),
    CONSTRAINT content_lock_access_drain_reads_claim_shape CHECK (
        (claim_token IS NULL AND claim_expires_at IS NULL)
        OR
        (claim_token IS NOT NULL AND claim_expires_at IS NOT NULL)
    ),
    CONSTRAINT content_lock_access_drain_reads_consumed_shape CHECK (
        consumed_at IS NULL
        OR (claim_token IS NULL AND claim_expires_at IS NULL)
    )
);

CREATE INDEX content_lock_access_drain_reads_claim_idx
    ON content_lock_access_drain_reads (claim_expires_at)
    WHERE consumed_at IS NULL AND claim_token IS NOT NULL;
