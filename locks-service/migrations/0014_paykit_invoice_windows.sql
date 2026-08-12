ALTER TABLE paykit_task_admissions
ADD COLUMN payment_in_hours BIGINT,
ADD COLUMN invoice_created_at TIMESTAMPTZ,
ADD COLUMN payment_deadline TIMESTAMPTZ;

-- Rows created before this migration have no authoritative payment window to
-- backfill. Preserve that all-NULL legacy state so the application can fail
-- closed instead of fabricating invoice facts. Every post-migration admission
-- writes a positive payment_in_hours and becomes ready only with a complete
-- immutable timestamp window.
ALTER TABLE paykit_task_admissions
ADD CONSTRAINT paykit_task_admissions_invoice_window_valid
    CHECK (
        (payment_in_hours IS NULL
            AND invoice_created_at IS NULL
            AND payment_deadline IS NULL)
        OR
        (payment_in_hours > 0
            AND NOT ready
            AND invoice_created_at IS NULL
            AND payment_deadline IS NULL)
        OR
        (payment_in_hours > 0
            AND ready
            AND invoice_created_at IS NOT NULL
            AND payment_deadline IS NOT NULL
            AND invoice_created_at <= payment_deadline)
    );
