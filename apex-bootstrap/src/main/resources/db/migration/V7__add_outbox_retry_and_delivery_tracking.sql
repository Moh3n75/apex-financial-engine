ALTER TABLE outbox_event
    ADD COLUMN published_at timestamp with time zone;

ALTER TABLE outbox_event
    ADD COLUMN last_error text;

ALTER TABLE outbox_event
    ADD COLUMN next_retry_at timestamp with time zone;