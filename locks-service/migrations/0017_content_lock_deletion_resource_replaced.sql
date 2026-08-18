ALTER TABLE content_lock_deletion_jobs
    DROP CONSTRAINT content_lock_deletion_jobs_failure_valid;

ALTER TABLE content_lock_deletion_jobs
    ADD CONSTRAINT content_lock_deletion_jobs_failure_valid CHECK (
        (state = 'failed' AND failure_code IN (
            'tombstone_missing',
            'tombstone_replaced',
            'resource_replaced',
            'retry_exhausted',
            'state_corrupt'
        ))
        OR
        (state <> 'failed' AND failure_code IS NULL)
    );
