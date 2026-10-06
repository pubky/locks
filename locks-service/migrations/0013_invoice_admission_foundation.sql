ALTER TABLE verification_tasks
    ADD COLUMN invoice_admission_phase TEXT NOT NULL DEFAULT 'ready',
    ADD COLUMN invoice_admission_intent JSONB,
    ADD COLUMN admission_deadline_at TIMESTAMPTZ;

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
                AND NULLIF(BTRIM(failure_message), '') IS NOT NULL
                AND terminal_reason IS NULL
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
                    'payment_deadline_expired'
                )
                AND entitlement_to_publish IS NULL
                AND claimed_by IS NULL
                AND claim_token IS NULL
                AND claim_expires_at IS NULL
            ELSE FALSE
        END
    );

ALTER TABLE verification_tasks
    ADD CONSTRAINT verification_tasks_invoice_admission_check CHECK (
        CASE invoice_admission_phase
            WHEN 'invoice_pending' THEN
                invoice_admission_intent IS NOT NULL
                AND admission_deadline_at IS NOT NULL
                AND status = 'pending'
            WHEN 'ready' THEN
                (invoice_admission_intent IS NULL AND admission_deadline_at IS NULL)
                OR
                (invoice_admission_intent IS NOT NULL AND admission_deadline_at IS NOT NULL)
            WHEN 'failed' THEN
                invoice_admission_intent IS NOT NULL
                AND admission_deadline_at IS NOT NULL
                AND status = 'failed'
            ELSE FALSE
        END
    );

CREATE INDEX verification_tasks_due_invoice_admission_idx
ON verification_tasks (next_attempt_at, submitted_at, task_id)
WHERE invoice_admission_phase = 'invoice_pending'
  AND status = 'pending';
