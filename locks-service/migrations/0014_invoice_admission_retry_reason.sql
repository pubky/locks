ALTER TABLE verification_tasks
    ADD COLUMN invoice_admission_retry_reason TEXT;

ALTER TABLE verification_tasks
    ADD CONSTRAINT verification_tasks_invoice_admission_retry_reason_check CHECK (
        invoice_admission_retry_reason IS NULL
        OR (
            invoice_admission_phase = 'invoice_pending'
            AND status = 'pending'
            AND invoice_admission_retry_reason = 'reader_wallet_setup_needed'
        )
    );
