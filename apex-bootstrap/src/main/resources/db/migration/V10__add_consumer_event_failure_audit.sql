CREATE TABLE consumer_event_failure
(
    id                  BIGSERIAL PRIMARY KEY,

    consumer_group      VARCHAR(200) NOT NULL,

    event_id            UUID,
    event_type          VARCHAR(150),
    event_version       INTEGER,

    topic               VARCHAR(255) NOT NULL,
    partition_id        INTEGER NOT NULL,
    offset_value        BIGINT NOT NULL,

    attempt_count       INTEGER NOT NULL,

    error_type          VARCHAR(500),
    last_error          TEXT,

    status              VARCHAR(50) NOT NULL,

    first_failed_at     TIMESTAMPTZ NOT NULL,
    last_failed_at      TIMESTAMPTZ NOT NULL,

    dead_letter_topic   VARCHAR(255),
    dead_lettered_at    TIMESTAMPTZ,

    recovered_at        TIMESTAMPTZ,

    CONSTRAINT uk_consumer_event_failure_delivery
        UNIQUE (
                consumer_group,
                topic,
                partition_id,
                offset_value
            ),

    CONSTRAINT ck_consumer_event_failure_attempt
        CHECK (attempt_count > 0),

    CONSTRAINT ck_consumer_event_failure_version
        CHECK (
            event_version IS NULL
                OR event_version > 0
            ),

    CONSTRAINT ck_consumer_event_failure_partition
        CHECK (partition_id >= 0),

    CONSTRAINT ck_consumer_event_failure_offset
        CHECK (offset_value >= 0)
);

CREATE INDEX ix_consumer_event_failure_status
    ON consumer_event_failure (status);

CREATE INDEX ix_consumer_event_failure_event_id
    ON consumer_event_failure (event_id);

CREATE INDEX ix_consumer_event_failure_last_failed
    ON consumer_event_failure (last_failed_at);