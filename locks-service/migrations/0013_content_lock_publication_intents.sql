CREATE TABLE content_lock_publication_intents (
    creator TEXT NOT NULL,
    lock_id TEXT NOT NULL,
    publication_token UUID NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT content_lock_publication_intents_pkey PRIMARY KEY (creator, lock_id),
    CONSTRAINT content_lock_publication_intents_token_unique UNIQUE (publication_token)
);