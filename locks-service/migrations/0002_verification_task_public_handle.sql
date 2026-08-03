ALTER TABLE verification_tasks
ADD COLUMN creator TEXT,
ADD COLUMN bundle_id TEXT;

UPDATE verification_tasks
SET
    creator = split_part(submitted_proof_bundle->>'pubky_lock_resource', '/', 1),
    bundle_id = submitted_proof_bundle->>'bundle_id'
WHERE creator IS NULL OR bundle_id IS NULL;

DO $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM verification_tasks
        WHERE creator IS NULL
           OR creator = ''
           OR bundle_id IS NULL
           OR bundle_id = ''
    ) THEN
        RAISE EXCEPTION 'verification task public handle fields cannot be backfilled';
    END IF;

    IF EXISTS (
        SELECT 1
        FROM verification_tasks
        GROUP BY creator, bundle_id
        HAVING COUNT(*) > 1
    ) THEN
        RAISE EXCEPTION 'duplicate verification task public handle rows exist';
    END IF;
END $$;

ALTER TABLE verification_tasks
ALTER COLUMN creator SET NOT NULL,
ALTER COLUMN bundle_id SET NOT NULL;

ALTER TABLE verification_tasks
ADD CONSTRAINT verification_tasks_creator_bundle_unique UNIQUE (creator, bundle_id);
