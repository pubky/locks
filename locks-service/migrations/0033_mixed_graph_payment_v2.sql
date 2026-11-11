ALTER TABLE verification_tasks
    DROP CONSTRAINT verification_tasks_evaluator_mode_closed,
    ADD CONSTRAINT verification_tasks_evaluator_mode_closed
        CHECK (evaluator_mode IS NULL OR evaluator_mode IN ('graph', 'payment', 'mixed')),
    DROP CONSTRAINT verification_tasks_evaluator_snapshot_shape,
    ADD CONSTRAINT verification_tasks_evaluator_snapshot_shape
        CHECK (
            (
                evaluator_mode IS NULL
                AND evaluator_snapshot IS NULL
                AND evaluator_revision = 0
            )
            OR (
                evaluator_mode IN ('graph', 'payment', 'mixed')
                AND evaluator_snapshot IS NOT NULL
                AND invoice_admission_phase = 'ready'
                AND invoice_admission_intent IS NULL
                AND admission_deadline_at IS NULL
                AND invoice_created_at IS NULL
                AND payment_deadline IS NULL
            )
        );

CREATE INDEX verification_tasks_due_mixed_idx
    ON verification_tasks (COALESCE(next_attempt_at, claim_expires_at), submitted_at, task_id)
    WHERE evaluator_mode = 'mixed'
      AND evaluator_snapshot IS NOT NULL
      AND status IN ('pending', 'in_progress');

ALTER TABLE folded_auth_attempts
    ADD COLUMN evaluator_mode TEXT,
    ADD COLUMN payment_policy JSONB;

UPDATE folded_auth_attempts
SET evaluator_mode = 'graph';

ALTER TABLE folded_auth_attempts
    ALTER COLUMN evaluator_mode SET NOT NULL,
    ADD CONSTRAINT folded_auth_attempts_evaluator_mode_closed
        CHECK (evaluator_mode IN ('graph', 'mixed')),
    ADD CONSTRAINT folded_auth_attempts_evaluator_policy_shape
        CHECK (
            (evaluator_mode = 'graph' AND payment_policy IS NULL)
            OR (evaluator_mode = 'mixed' AND payment_policy IS NOT NULL)
        );

ALTER TABLE content_lock_deletion_task_snapshot
    DROP CONSTRAINT content_lock_deletion_snapshot_kind_shape,
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
             AND evaluator_mode_at_cutoff IN ('payment', 'mixed')
             AND evaluator_revision_at_cutoff IS NOT NULL
             AND evaluator_revision_at_cutoff >= 0
             AND evaluator_snapshot_at_cutoff IS NOT NULL
             AND provider_reconciliation_state IS NOT NULL)
        );
