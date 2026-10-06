ALTER TABLE consumer_event_failure
    ADD COLUMN message_key VARCHAR(500),
    ADD COLUMN original_payload TEXT,
    ADD COLUMN replay_count INTEGER NOT NULL DEFAULT 0,
    ADD COLUMN replay_requested_at TIMESTAMPTZ,
    ADD COLUMN last_replayed_at TIMESTAMPTZ,
    ADD COLUMN last_replay_error TEXT;

ALTER TABLE consumer_event_failure
    ADD CONSTRAINT ck_consumer_event_failure_replay_count
        CHECK (replay_count >= 0);

CREATE INDEX ix_consumer_event_failure_recovery
    ON consumer_event_failure (
                               consumer_group,
                               event_id,
                               status
        );