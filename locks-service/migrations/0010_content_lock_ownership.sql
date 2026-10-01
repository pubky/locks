CREATE TABLE content_lock_ownership (
    creator TEXT NOT NULL,
    guarded_path TEXT NOT NULL,
    lock_id TEXT NOT NULL,
    status TEXT NOT NULL,
    CONSTRAINT content_lock_ownership_creator_path_unique UNIQUE (creator, guarded_path),
    CONSTRAINT content_lock_ownership_status_valid CHECK (status IN ('reserved', 'published'))
);
