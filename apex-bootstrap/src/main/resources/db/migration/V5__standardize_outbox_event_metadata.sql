ALTER TABLE outbox_event
    ADD COLUMN event_id UUID,
    ADD COLUMN event_version INTEGER,
    ADD COLUMN correlation_id UUID,
    ADD COLUMN causation_id UUID,
    ADD COLUMN aggregate_type VARCHAR(100),
    ADD COLUMN source_service VARCHAR(150),
    ADD COLUMN cell_id VARCHAR(100),
    ADD COLUMN occurred_at TIMESTAMPTZ;

ALTER TABLE outbox_event
    ADD CONSTRAINT ck_outbox_event_event_version
        CHECK (event_version IS NULL OR event_version > 0);

CREATE UNIQUE INDEX uq_outbox_event_event_id
    ON outbox_event(event_id)
    WHERE event_id IS NOT NULL;

CREATE INDEX ix_outbox_event_unpublished_occurred_at
    ON outbox_event(occurred_at)
    WHERE published = false;
