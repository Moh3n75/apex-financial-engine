ALTER TABLE outbox_event

    ADD COLUMN dead_letter    boolean DEFAULT false NOT NULL,

    ADD COLUMN dead_letter_at timestamp with time zone;