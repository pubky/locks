ALTER TABLE verification_tasks
DROP CONSTRAINT verification_tasks_status_check;

ALTER TABLE verification_tasks
ADD CONSTRAINT verification_tasks_status_check
CHECK (status IN ('pending', 'in_progress', 'completed', 'failed', 'cancelled', 'expired'));
