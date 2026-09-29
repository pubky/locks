-- Staging-only lifecycle pivot: existing verification attempts use the retired
-- expired-without-reason contract and cannot be represented safely.
TRUNCATE TABLE verification_tasks;

ALTER TABLE verification_tasks
    ADD COLUMN terminal_reason TEXT;

ALTER TABLE verification_tasks
    ADD CONSTRAINT verification_tasks_terminal_reason_check CHECK (
        (status = 'expired'
            AND terminal_reason IN (
                'payment_request_rejected',
                'payment_request_canceled',
                'proposal_expired',
                'payment_deadline_expired'
            )
            AND completed_at IS NOT NULL
            AND failure_message IS NULL)
        OR
        (status <> 'expired' AND terminal_reason IS NULL)
    );
