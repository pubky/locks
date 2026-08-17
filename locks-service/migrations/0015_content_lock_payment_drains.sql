ALTER TABLE content_lock_deletion_task_snapshot
    ADD COLUMN creator TEXT,
    ADD COLUMN bundle_id TEXT,
    ADD COLUMN pubky_lock_resource TEXT,
    ADD COLUMN criterion_id TEXT,
    ADD COLUMN status_at_cutoff TEXT,
    ADD COLUMN paykit_admission_required BOOLEAN,
    ADD COLUMN payment_in_hours BIGINT,
    ADD COLUMN invoice_created_at TIMESTAMPTZ,
    ADD COLUMN payment_deadline TIMESTAMPTZ,
    ADD COLUMN resolved_status TEXT,
    ADD COLUMN resolved_at TIMESTAMPTZ;

ALTER TABLE verification_tasks
    ADD COLUMN entitlement_publication_claim_token UUID,
    ADD COLUMN deletion_job_id UUID REFERENCES content_lock_deletion_jobs(job_id);

COMMENT ON COLUMN verification_tasks.entitlement_publication_claim_token IS
    'Claim token that crossed the external entitlement-publication boundary.';
COMMENT ON COLUMN verification_tasks.deletion_job_id IS
    'Task-row ownership fence set by graceful deletion admission.';

ALTER TABLE content_lock_deletion_task_snapshot
    ADD CONSTRAINT content_lock_deletion_task_snapshot_cutoff_status_valid
        CHECK (status_at_cutoff IN ('pending', 'in_progress', 'completed', 'failed', 'expired')),
    ADD CONSTRAINT content_lock_deletion_task_snapshot_identity_all_or_none
        CHECK (
            (creator IS NULL AND bundle_id IS NULL AND pubky_lock_resource IS NULL
             AND criterion_id IS NULL AND status_at_cutoff IS NULL)
            OR
            (creator IS NOT NULL AND bundle_id IS NOT NULL
             AND pubky_lock_resource IS NOT NULL AND status_at_cutoff IS NOT NULL)
        ),
    ADD CONSTRAINT content_lock_deletion_task_snapshot_admission_shape_valid
        CHECK (
            (paykit_admission_required IS NULL
             AND payment_in_hours IS NULL
             AND invoice_created_at IS NULL
             AND payment_deadline IS NULL)
            OR
            (paykit_admission_required = FALSE
             AND payment_in_hours IS NULL
             AND invoice_created_at IS NULL
             AND payment_deadline IS NULL)
            OR
            (paykit_admission_required = TRUE
             AND payment_in_hours IS NULL
             AND invoice_created_at IS NULL
             AND payment_deadline IS NULL)
            OR
            (paykit_admission_required = TRUE
             AND payment_in_hours > 0
             AND invoice_created_at IS NOT NULL
             AND payment_deadline IS NOT NULL
             AND invoice_created_at <= payment_deadline)
        ),
    ADD CONSTRAINT content_lock_deletion_task_snapshot_resolution_valid
        CHECK (
            (resolved_status IS NULL AND resolved_at IS NULL)
            OR
            (resolved_status IN ('completed', 'failed', 'expired') AND resolved_at IS NOT NULL)
        ),
    ADD CONSTRAINT content_lock_deletion_task_snapshot_bundle_unique
        UNIQUE (deletion_job_id, creator, bundle_id);

CREATE INDEX content_lock_deletion_task_snapshot_resolution_idx
    ON content_lock_deletion_task_snapshot (deletion_job_id, resolved_status);

CREATE TABLE content_lock_payment_drains (
    deletion_job_id UUID PRIMARY KEY
        REFERENCES content_lock_deletion_jobs(job_id) ON DELETE CASCADE,
    status TEXT NOT NULL,
    accepted_count BIGINT NOT NULL,
    terminal_count BIGINT NOT NULL,
    cancellation_enqueued_count BIGINT NOT NULL,
    cleanup_token TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL,
    CONSTRAINT content_lock_payment_drains_status_valid
        CHECK (status IN ('active', 'completed')),
    CONSTRAINT content_lock_payment_drains_counts_valid
        CHECK (
            accepted_count >= 0
            AND terminal_count >= 0
            AND cancellation_enqueued_count >= 0
        ),
    CONSTRAINT content_lock_payment_drains_cleanup_token_shape
        CHECK (
            length(cleanup_token) = 43
            AND cleanup_token ~ '^[A-Za-z0-9_-]{43}$'
        )
);
