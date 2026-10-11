ALTER TABLE outbox_event
    ADD COLUMN claim_token UUID,
    ADD COLUMN claimed_at TIMESTAMP WITH TIME ZONE,
    ADD COLUMN claimed_until TIMESTAMP WITH TIME ZONE,
    ADD COLUMN publisher_instance_id VARCHAR(150);


CREATE INDEX ix_outbox_event_claimable
    ON outbox_event (
                     published,
                     dead_letter,
                     next_retry_at,
                     claimed_until
        );