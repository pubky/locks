ALTER TABLE verification_tasks
ADD COLUMN next_attempt_at TIMESTAMPTZ;

DROP INDEX verification_tasks_pending_idx;

CREATE INDEX verification_tasks_due_pending_idx
ON verification_tasks (submitted_at, next_attempt_at)
WHERE status = 'pending';
