CREATE TABLE content_lock_deletion_task_snapshot (
    deletion_job_id UUID NOT NULL REFERENCES content_lock_deletion_jobs(job_id) ON DELETE CASCADE,
    verification_task_id UUID NOT NULL REFERENCES verification_tasks(task_id),
    CONSTRAINT content_lock_deletion_task_snapshot_pkey
        PRIMARY KEY (deletion_job_id, verification_task_id),
    CONSTRAINT content_lock_deletion_task_snapshot_task_unique
        UNIQUE (verification_task_id)
);

CREATE TABLE paykit_task_admissions (
    verification_task_id UUID PRIMARY KEY REFERENCES verification_tasks(task_id) ON DELETE CASCADE,
    ready BOOLEAN NOT NULL DEFAULT FALSE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    ready_at TIMESTAMPTZ,
    CONSTRAINT paykit_task_admissions_ready_time_valid CHECK (
        (ready AND ready_at IS NOT NULL) OR (NOT ready AND ready_at IS NULL)
    )
);
