ALTER TABLE folded_auth_attempts
    ADD COLUMN poll_dispatched_at TIMESTAMPTZ;

ALTER TABLE folded_auth_attempts
    ADD CONSTRAINT folded_auth_attempts_poll_dispatched_pending_only
    CHECK (status = 'pending' OR poll_dispatched_at IS NULL);
