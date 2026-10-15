ALTER TABLE verification_tasks
    DROP CONSTRAINT verification_tasks_deletion_job_id_fkey,
    ADD CONSTRAINT verification_tasks_deletion_job_id_fkey
        FOREIGN KEY (deletion_job_id)
        REFERENCES content_lock_deletion_jobs(job_id)
        ON DELETE SET NULL;