ALTER TABLE outbox_event
    ALTER COLUMN aggregate_id TYPE UUID
        USING aggregate_id::text::uuid;