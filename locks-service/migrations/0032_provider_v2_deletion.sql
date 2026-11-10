UPDATE content_lock_deletion_task_snapshot AS snapshot
SET resolved_status = NULL,
    resolved_at = NULL,
    final_credential_eligible_at = NULL
FROM content_lock_deletion_jobs AS deletion,
     verification_tasks AS task
WHERE snapshot.deletion_job_id = deletion.job_id
  AND task.task_id = snapshot.verification_task_id
  AND task.evaluator_mode = 'payment'
  AND deletion.state IN ('queued', 'running', 'failed')
  AND deletion.force_requested_at IS NULL
  AND deletion.phase IN ('withdraw', 'start_payment_drain');

DO $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM content_lock_deletion_jobs AS deletion
        JOIN content_lock_deletion_task_snapshot AS snapshot
          ON snapshot.deletion_job_id = deletion.job_id
        JOIN verification_tasks AS task
          ON task.task_id = snapshot.verification_task_id
        WHERE task.evaluator_mode = 'payment'
          AND deletion.state = 'completed'
          AND deletion.force_requested_at IS NULL
          AND snapshot.resolved_status IS NULL
    ) THEN
        RAISE EXCEPTION USING
            MESSAGE = 'migration 0032 found a completed provider-v2 deletion without a persisted rc3 fence',
            HINT = 'follow the migration 0032 incident query in docs/RUNTIME.md; do not delete or hand-edit the completed job';
    END IF;

    IF EXISTS (
        SELECT 1
        FROM content_lock_deletion_jobs AS deletion
        JOIN content_lock_deletion_task_snapshot AS snapshot
          ON snapshot.deletion_job_id = deletion.job_id
        JOIN verification_tasks AS task
          ON task.task_id = snapshot.verification_task_id
        WHERE task.evaluator_mode = 'payment'
          AND deletion.state IN ('queued', 'running', 'failed')
          AND deletion.force_requested_at IS NULL
          AND deletion.phase NOT IN ('withdraw', 'start_payment_drain')
    ) THEN
        RAISE EXCEPTION USING
            MESSAGE = 'migration 0032 found an active provider-v2 deletion that may have crossed payment drain without a persisted rc3 fence',
            HINT = 'reconcile or explicitly force/reset the deletion before retrying the migration';
    END IF;
END $$;

ALTER TABLE content_lock_deletion_task_snapshot
    ADD COLUMN obligation_kind TEXT,
    ADD COLUMN evaluator_mode_at_cutoff TEXT,
    ADD COLUMN evaluator_revision_at_cutoff BIGINT,
    ADD COLUMN evaluator_snapshot_at_cutoff JSONB,
    ADD COLUMN entitlement_payload_at_cutoff JSONB,
    ADD COLUMN publication_token_at_cutoff UUID,
    ADD COLUMN provider_reconciliation_state TEXT,
    ADD COLUMN provider_create_replay_result TEXT,
    ADD COLUMN provider_create_invoice_created_at TIMESTAMPTZ,
    ADD COLUMN provider_create_proposal_expires_at TIMESTAMPTZ,
    ADD COLUMN provider_create_deadline TIMESTAMPTZ,
    ADD COLUMN provider_status_snapshot JSONB,
    ADD COLUMN provider_reconciled_at TIMESTAMPTZ;

UPDATE content_lock_deletion_task_snapshot AS snapshot
SET obligation_kind = CASE
        WHEN task.evaluator_mode = 'payment' THEN 'provider_v2'
        WHEN snapshot.paykit_admission_required = TRUE THEN 'paykit_v1'
        ELSE 'local'
    END,
    paykit_admission_required = CASE
        WHEN task.evaluator_mode = 'payment' THEN TRUE
        ELSE COALESCE(snapshot.paykit_admission_required, FALSE)
    END,
    evaluator_mode_at_cutoff = CASE WHEN task.evaluator_mode = 'payment' THEN task.evaluator_mode END,
    evaluator_revision_at_cutoff = CASE WHEN task.evaluator_mode = 'payment' THEN task.evaluator_revision END,
    evaluator_snapshot_at_cutoff = CASE WHEN task.evaluator_mode = 'payment' THEN task.evaluator_snapshot END,
    entitlement_payload_at_cutoff = CASE WHEN task.evaluator_mode = 'payment' THEN task.entitlement_to_publish END,
    publication_token_at_cutoff = CASE WHEN task.evaluator_mode = 'payment' THEN task.entitlement_publication_claim_token END,
    provider_reconciliation_state = CASE
        WHEN task.evaluator_mode = 'payment'
             AND deletion.state = 'completed'
             AND deletion.force_requested_at IS NULL
             AND snapshot.resolved_status IS NOT NULL
            THEN 'terminal'
        WHEN task.evaluator_mode = 'payment' THEN 'pending'
    END,
    provider_reconciled_at = CASE
        WHEN task.evaluator_mode = 'payment'
             AND deletion.state = 'completed'
             AND deletion.force_requested_at IS NULL
             AND snapshot.resolved_status IS NOT NULL
            THEN snapshot.resolved_at
    END
FROM verification_tasks AS task,
     content_lock_deletion_jobs AS deletion
WHERE task.task_id = snapshot.verification_task_id
  AND deletion.job_id = snapshot.deletion_job_id;

UPDATE content_lock_deletion_task_snapshot
SET obligation_kind = COALESCE(obligation_kind, 'local'),
    paykit_admission_required = COALESCE(paykit_admission_required, FALSE);

ALTER TABLE content_lock_deletion_task_snapshot
    ALTER COLUMN obligation_kind SET NOT NULL,
    ADD CONSTRAINT content_lock_deletion_snapshot_obligation_kind_closed
        CHECK (obligation_kind IN ('local', 'paykit_v1', 'provider_v2')),
    ADD CONSTRAINT content_lock_deletion_snapshot_kind_shape
        CHECK (
            (obligation_kind = 'local'
             AND paykit_admission_required = FALSE
             AND criterion_id IS NULL
             AND evaluator_mode_at_cutoff IS NULL
             AND evaluator_revision_at_cutoff IS NULL
             AND evaluator_snapshot_at_cutoff IS NULL
             AND entitlement_payload_at_cutoff IS NULL
             AND publication_token_at_cutoff IS NULL
             AND provider_reconciliation_state IS NULL
             AND provider_create_replay_result IS NULL
             AND provider_create_invoice_created_at IS NULL
             AND provider_create_proposal_expires_at IS NULL
             AND provider_create_deadline IS NULL
             AND provider_status_snapshot IS NULL
             AND provider_reconciled_at IS NULL)
            OR
            (obligation_kind = 'paykit_v1'
             AND paykit_admission_required = TRUE
             AND criterion_id IS NOT NULL
             AND evaluator_mode_at_cutoff IS NULL
             AND evaluator_revision_at_cutoff IS NULL
             AND evaluator_snapshot_at_cutoff IS NULL
             AND entitlement_payload_at_cutoff IS NULL
             AND publication_token_at_cutoff IS NULL
             AND provider_reconciliation_state IS NULL
             AND provider_create_replay_result IS NULL
             AND provider_create_invoice_created_at IS NULL
             AND provider_create_proposal_expires_at IS NULL
             AND provider_create_deadline IS NULL
             AND provider_status_snapshot IS NULL
             AND provider_reconciled_at IS NULL)
            OR
            (obligation_kind = 'provider_v2'
             AND paykit_admission_required = TRUE
             AND criterion_id IS NULL
             AND invoice_created_at IS NULL
             AND payment_deadline IS NULL
             AND evaluator_mode_at_cutoff = 'payment'
             AND evaluator_revision_at_cutoff IS NOT NULL
             AND evaluator_revision_at_cutoff >= 0
             AND evaluator_snapshot_at_cutoff IS NOT NULL
             AND provider_reconciliation_state IS NOT NULL)
        ),
    ADD CONSTRAINT content_lock_deletion_snapshot_provider_state_closed
        CHECK (provider_reconciliation_state IS NULL OR provider_reconciliation_state IN (
            'pending', 'binding_replayed', 'not_found_after_fence', 'found', 'terminal', 'abandoned'
        )),
    ADD CONSTRAINT content_lock_deletion_snapshot_provider_replay_closed
        CHECK (provider_create_replay_result IS NULL OR provider_create_replay_result IN ('exact_replay', 'conflict')),
    ADD CONSTRAINT content_lock_deletion_snapshot_provider_timestamps_shape
        CHECK (
            (provider_create_invoice_created_at IS NULL
             AND provider_create_proposal_expires_at IS NULL
             AND provider_create_deadline IS NULL)
            OR
            (provider_create_invoice_created_at IS NOT NULL
             AND provider_create_proposal_expires_at IS NOT NULL
             AND provider_create_deadline IS NOT NULL
             AND provider_create_invoice_created_at < provider_create_proposal_expires_at
             AND provider_create_proposal_expires_at < provider_create_deadline)
        ),
    ADD CONSTRAINT content_lock_deletion_snapshot_provider_evidence_shape
        CHECK (
            obligation_kind <> 'provider_v2'
            OR
            (provider_reconciliation_state = 'pending'
             AND provider_status_snapshot IS NULL
             AND provider_reconciled_at IS NULL
             AND (
                 (provider_create_replay_result IS NULL
                  AND provider_create_invoice_created_at IS NULL
                  AND provider_create_proposal_expires_at IS NULL
                  AND provider_create_deadline IS NULL)
                 OR
                 (provider_create_replay_result = 'conflict'
                  AND provider_create_invoice_created_at IS NULL
                  AND provider_create_proposal_expires_at IS NULL
                  AND provider_create_deadline IS NULL)
             ))
            OR
            (provider_reconciliation_state = 'binding_replayed'
             AND provider_create_replay_result = 'exact_replay'
             AND provider_create_invoice_created_at IS NOT NULL
             AND provider_create_proposal_expires_at IS NOT NULL
             AND provider_create_deadline IS NOT NULL
             AND provider_status_snapshot IS NULL
             AND provider_reconciled_at IS NULL)
            OR
            (provider_reconciliation_state = 'not_found_after_fence'
             AND provider_create_replay_result = 'conflict'
             AND provider_create_invoice_created_at IS NULL
             AND provider_create_proposal_expires_at IS NULL
             AND provider_create_deadline IS NULL
             AND provider_status_snapshot IS NULL
             AND provider_reconciled_at IS NOT NULL)
            OR
            (provider_reconciliation_state = 'found'
             AND provider_create_replay_result = 'exact_replay'
             AND provider_create_invoice_created_at IS NOT NULL
             AND provider_create_proposal_expires_at IS NOT NULL
             AND provider_create_deadline IS NOT NULL
             AND provider_status_snapshot IS NOT NULL
             AND provider_reconciled_at IS NOT NULL)
            OR
            (provider_reconciliation_state = 'terminal'
             AND resolved_status IS NOT NULL
             AND provider_reconciled_at IS NOT NULL
             AND (
                 -- v0.1.1-rc3 completed graceful deletions retain terminal
                 -- evidence in the frozen evaluator snapshot and resolution fields.
                 (provider_create_replay_result IS NULL
                  AND provider_create_invoice_created_at IS NULL
                  AND provider_create_proposal_expires_at IS NULL
                  AND provider_create_deadline IS NULL
                  AND provider_status_snapshot IS NULL)
                 OR
                 (provider_create_replay_result = 'conflict'
                  AND provider_create_invoice_created_at IS NULL
                  AND provider_create_proposal_expires_at IS NULL
                  AND provider_create_deadline IS NULL
                  AND provider_status_snapshot IS NULL)
                 OR
                 (provider_create_replay_result = 'exact_replay'
                  AND provider_create_invoice_created_at IS NOT NULL
                  AND provider_create_proposal_expires_at IS NOT NULL
                  AND provider_create_deadline IS NOT NULL
                  AND provider_status_snapshot IS NOT NULL)
             ))
            OR
            (provider_reconciliation_state = 'abandoned'
             AND resolved_status = 'expired'
             AND provider_create_replay_result IS NULL
             AND provider_create_invoice_created_at IS NULL
             AND provider_create_proposal_expires_at IS NULL
             AND provider_create_deadline IS NULL
             AND provider_status_snapshot IS NULL)
        );

ALTER TABLE content_lock_payment_drains
    ADD COLUMN pubky_lock_resource TEXT,
    ADD COLUMN paykit_authority_scheme TEXT,
    ADD COLUMN paykit_authority_host TEXT,
    ADD COLUMN paykit_authority_port INTEGER,
    ADD COLUMN paykit_authority_base_path TEXT,
    ADD COLUMN provider_v2_fence_acknowledged_at TIMESTAMPTZ,
    ADD COLUMN cleanup_state TEXT NOT NULL DEFAULT 'pending',
    ADD COLUMN cleanup_completed_at TIMESTAMPTZ,
    ADD CONSTRAINT content_lock_payment_drains_authority_shape CHECK (
        (paykit_authority_scheme IS NULL AND paykit_authority_host IS NULL
         AND paykit_authority_port IS NULL AND paykit_authority_base_path IS NULL)
        OR
        (paykit_authority_scheme IN ('http', 'https')
         AND NULLIF(BTRIM(paykit_authority_host), '') IS NOT NULL
         AND paykit_authority_port BETWEEN 1 AND 65535
         AND paykit_authority_base_path IS NOT NULL)
    ),
    ADD CONSTRAINT content_lock_payment_drains_cleanup_state_closed
        CHECK (cleanup_state IN ('pending', 'completed')),
    ADD CONSTRAINT content_lock_payment_drains_cleanup_shape
        CHECK ((cleanup_state = 'pending' AND cleanup_completed_at IS NULL)
            OR (cleanup_state = 'completed' AND cleanup_completed_at IS NOT NULL));

COMMENT ON COLUMN content_lock_payment_drains.provider_v2_fence_acknowledged_at IS
    'Persisted acknowledgement that Paykit Server rc3 established the permanent provider-v2 create fence.';
COMMENT ON COLUMN content_lock_payment_drains.cleanup_state IS
    'Graceful deletion may purge this row only after exact payment-drain cleanup replay completes.';

CREATE TABLE content_lock_provider_v2_drain_fences (
    deletion_job_id UUID PRIMARY KEY
        REFERENCES content_lock_deletion_jobs(job_id) ON DELETE CASCADE,
    pubky_lock_resource TEXT NOT NULL,
    paykit_authority_scheme TEXT NOT NULL CHECK (paykit_authority_scheme IN ('http', 'https')),
    paykit_authority_host TEXT NOT NULL CHECK (NULLIF(BTRIM(paykit_authority_host), '') IS NOT NULL),
    paykit_authority_port INTEGER NOT NULL CHECK (paykit_authority_port BETWEEN 1 AND 65535),
    paykit_authority_base_path TEXT NOT NULL,
    prepared_at TIMESTAMPTZ NOT NULL
);

COMMENT ON TABLE content_lock_provider_v2_drain_fences IS
    'Claim-fenced Paykit authority continuity persisted before any provider-v2 drain HTTP call.';

ALTER TABLE content_lock_force_deletion_receipts
    ADD COLUMN provider_v2_abandoned_at TIMESTAMPTZ;
