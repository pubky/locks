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
                    'payment_deadline_expired',
                    'content_lock_deleted'
                )
                AND entitlement_to_publish IS NULL
                AND claimed_by IS NULL
                AND claim_token IS NULL
                AND claim_expires_at IS NULL
            ELSE FALSE
        END
    );
