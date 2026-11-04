ALTER TABLE verification_tasks
    ADD COLUMN evaluator_snapshot JSONB,
    ADD COLUMN evaluator_revision BIGINT NOT NULL DEFAULT 0,
    ADD CONSTRAINT verification_tasks_evaluator_revision_nonnegative
        CHECK (evaluator_revision >= 0),
    ADD CONSTRAINT verification_tasks_graph_snapshot_shape
        CHECK (
            (evaluator_snapshot IS NULL AND evaluator_revision = 0)
            OR (
                evaluator_snapshot IS NOT NULL
                AND invoice_admission_phase = 'ready'
                AND invoice_admission_intent IS NULL
                AND admission_deadline_at IS NULL
                AND invoice_created_at IS NULL
                AND payment_deadline IS NULL
            )
        );

CREATE INDEX verification_tasks_due_graph_idx
    ON verification_tasks (COALESCE(next_attempt_at, claim_expires_at), submitted_at, task_id)
    WHERE evaluator_snapshot IS NOT NULL
      AND status IN ('pending', 'in_progress');
