ALTER TABLE verification_tasks
    ADD COLUMN evaluator_mode TEXT;

UPDATE verification_tasks
SET evaluator_mode = 'graph'
WHERE evaluator_snapshot IS NOT NULL;

ALTER TABLE verification_tasks
    ADD CONSTRAINT verification_tasks_evaluator_mode_closed
        CHECK (evaluator_mode IS NULL OR evaluator_mode IN ('graph', 'payment')),
    DROP CONSTRAINT verification_tasks_graph_snapshot_shape,
    ADD CONSTRAINT verification_tasks_evaluator_snapshot_shape
        CHECK (
            (
                evaluator_mode IS NULL
                AND evaluator_snapshot IS NULL
                AND evaluator_revision = 0
            )
            OR (
                evaluator_mode IN ('graph', 'payment')
                AND evaluator_snapshot IS NOT NULL
                AND invoice_admission_phase = 'ready'
                AND invoice_admission_intent IS NULL
                AND admission_deadline_at IS NULL
                AND invoice_created_at IS NULL
                AND payment_deadline IS NULL
            )
        );

DROP INDEX verification_tasks_due_graph_idx;

CREATE INDEX verification_tasks_due_graph_idx
    ON verification_tasks (COALESCE(next_attempt_at, claim_expires_at), submitted_at, task_id)
    WHERE evaluator_mode = 'graph'
      AND evaluator_snapshot IS NOT NULL
      AND status IN ('pending', 'in_progress');

CREATE INDEX verification_tasks_due_payment_idx
    ON verification_tasks (COALESCE(next_attempt_at, claim_expires_at), submitted_at, task_id)
    WHERE evaluator_mode = 'payment'
      AND evaluator_snapshot IS NOT NULL
      AND status IN ('pending', 'in_progress');

ALTER TABLE verification_tasks
    DROP CONSTRAINT verification_tasks_state_check;

ALTER TABLE verification_tasks
    ADD CONSTRAINT verification_tasks_state_check CHECK (
        CASE status
            WHEN 'pending' THEN
                started_at IS NULL
                AND completed_at IS NULL
                AND failure_message IS NULL
                AND terminal_reason IS NULL
                AND entitlement_to_publish IS NULL
                AND (
                    (claimed_by IS NULL AND claim_token IS NULL AND claim_expires_at IS NULL)
                    OR
                    (invoice_admission_phase = 'invoice_pending'
                     AND claimed_by IS NOT NULL
                     AND claim_token IS NOT NULL
                     AND claim_expires_at IS NOT NULL)
                )
            WHEN 'in_progress' THEN
                started_at IS NOT NULL
                AND completed_at IS NULL
                AND failure_message IS NULL
                AND terminal_reason IS NULL
                AND entitlement_to_publish IS NULL
                AND (
                    (claimed_by IS NULL AND claim_token IS NULL AND claim_expires_at IS NULL)
                    OR
                    (claimed_by IS NOT NULL AND claim_token IS NOT NULL AND claim_expires_at IS NOT NULL)
                )
            WHEN 'publishing_entitlement' THEN
                started_at IS NOT NULL
                AND completed_at IS NULL
                AND failure_message IS NULL
                AND terminal_reason IS NULL
                AND entitlement_to_publish IS NOT NULL
                AND (
                    (claimed_by IS NULL AND claim_token IS NULL AND claim_expires_at IS NULL)
                    OR
                    (claimed_by IS NOT NULL AND claim_token IS NOT NULL AND claim_expires_at IS NOT NULL)
                )
            WHEN 'completed' THEN
                started_at IS NOT NULL
                AND completed_at IS NOT NULL
                AND failure_message IS NULL
                AND terminal_reason IS NULL
                AND entitlement_to_publish IS NOT NULL
                AND claimed_by IS NULL
                AND claim_token IS NULL
                AND claim_expires_at IS NULL
            WHEN 'failed' THEN
                started_at IS NOT NULL
                AND completed_at IS NOT NULL
                AND (
                    (
                        NULLIF(BTRIM(failure_message), '') IS NOT NULL
                        AND terminal_reason IS NULL
                    )
                    OR
                    (
                        failure_message IS NULL
                        AND COALESCE(
                            terminal_reason IN (
                                'verification_unavailable_by_deadline',
                                'invalid_verifier_response',
                                'verification_conflict'
                            ),
                            FALSE
                        )
                    )
                )
                AND entitlement_to_publish IS NULL
                AND claimed_by IS NULL
                AND claim_token IS NULL
                AND claim_expires_at IS NULL
            WHEN 'expired' THEN
                started_at IS NOT NULL
                AND completed_at IS NOT NULL
                AND failure_message IS NULL
                AND terminal_reason IS NOT NULL
                AND terminal_reason IN (
                    'payment_request_rejected',
                    'payment_request_canceled',
                    'proposal_expired',
                    'payment_deadline_expired',
                    'content_lock_deleted',
                    'graph_condition_not_observed_by_deadline'
                )
                AND entitlement_to_publish IS NULL
                AND claimed_by IS NULL
                AND claim_token IS NULL
                AND claim_expires_at IS NULL
            ELSE FALSE
        END
    );
