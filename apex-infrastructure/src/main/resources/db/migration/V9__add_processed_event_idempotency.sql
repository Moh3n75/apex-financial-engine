CREATE TABLE processed_event
(
    consumer_group VARCHAR(200) NOT NULL,
    event_id       UUID         NOT NULL,

    event_type     VARCHAR(150) NOT NULL,
    event_version  INTEGER      NOT NULL,

    topic          VARCHAR(255) NOT NULL,
    partition_id   INTEGER      NOT NULL,
    offset_value   BIGINT       NOT NULL,

    processed_at   TIMESTAMPTZ  NOT NULL,

    CONSTRAINT pk_processed_event
        PRIMARY KEY (consumer_group, event_id),

    CONSTRAINT ck_processed_event_version
        CHECK (event_version > 0),

    CONSTRAINT ck_processed_event_partition
        CHECK (partition_id >= 0),

    CONSTRAINT ck_processed_event_offset
        CHECK (offset_value >= 0)
);

CREATE INDEX ix_processed_event_processed_at
    ON processed_event (processed_at);

CREATE INDEX ix_processed_event_type
    ON processed_event (event_type, event_version);