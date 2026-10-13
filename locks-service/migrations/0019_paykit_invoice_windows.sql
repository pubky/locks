ALTER TABLE verification_tasks
    ADD COLUMN invoice_created_at TIMESTAMPTZ,
    ADD COLUMN payment_deadline TIMESTAMPTZ;

-- Existing ready rows have no trustworthy Paykit response window to backfill.
-- Preserve their all-NULL legacy state so every action boundary can fail closed.
ALTER TABLE verification_tasks
    ADD CONSTRAINT verification_tasks_invoice_window_valid CHECK (
        (invoice_created_at IS NULL AND payment_deadline IS NULL)
        OR
        (invoice_admission_phase = 'ready'
         AND invoice_admission_intent IS NOT NULL
         AND invoice_created_at IS NOT NULL
         AND payment_deadline IS NOT NULL
         AND payment_deadline > invoice_created_at)
    );
