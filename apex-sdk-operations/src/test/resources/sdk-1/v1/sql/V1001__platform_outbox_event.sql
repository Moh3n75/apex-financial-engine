-- Reusable Outbox schema installed into each owning Cell database.
CREATE TABLE IF NOT EXISTS outbox_event
(
    id                     BIGSERIAL PRIMARY KEY,
    event_id               UUID         NOT NULL,
    event_type             VARCHAR(150) NOT NULL,
    event_version          INTEGER      NOT NULL,
    correlation_id         UUID         NOT NULL,
    causation_id           UUID,
    aggregate_id           UUID         NOT NULL,
    aggregate_type         VARCHAR(100) NOT NULL,
    source_service         VARCHAR(150) NOT NULL,
    cell_id                VARCHAR(100) NOT NULL,
    occurred_at            TIMESTAMPTZ  NOT NULL,
    payload                JSONB        NOT NULL,
    published              BOOLEAN      NOT NULL DEFAULT FALSE,
    published_at           TIMESTAMPTZ,
    created_at             TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    retry_count            INTEGER      NOT NULL DEFAULT 0,
    last_error             TEXT,
    next_retry_at          TIMESTAMPTZ,
    dead_letter            BOOLEAN      NOT NULL DEFAULT FALSE,
    dead_letter_at         TIMESTAMPTZ,
    claim_token            UUID,
    claimed_at             TIMESTAMPTZ,
    claimed_until          TIMESTAMPTZ,
    publisher_instance_id  VARCHAR(150),
    CONSTRAINT uq_outbox_event_event_id UNIQUE (event_id),
    CONSTRAINT ck_outbox_event_event_version CHECK (event_version > 0)
);

CREATE INDEX IF NOT EXISTS ix_outbox_event_claimable
    ON outbox_event (published, dead_letter, next_retry_at, claimed_until);

CREATE INDEX IF NOT EXISTS ix_outbox_event_unpublished_occurred_at
    ON outbox_event (occurred_at)
    WHERE published = false;
