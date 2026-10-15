CREATE TABLE content_lock_deletion_task_snapshot (
    deletion_job_id UUID NOT NULL REFERENCES content_lock_deletion_jobs(job_id) ON DELETE CASCADE,
    verification_task_id UUID NOT NULL REFERENCES verification_tasks(task_id),
    CONSTRAINT content_lock_deletion_task_snapshot_pkey
        PRIMARY KEY (deletion_job_id, verification_task_id),
    CONSTRAINT content_lock_deletion_task_snapshot_task_unique
        UNIQUE (verification_task_id)
);
