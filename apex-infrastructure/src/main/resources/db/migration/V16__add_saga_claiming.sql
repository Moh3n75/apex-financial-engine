ALTER TABLE saga_instance
    ADD COLUMN claimed_by VARCHAR(100);

ALTER TABLE saga_instance
    ADD COLUMN claimed_at TIMESTAMP WITH TIME ZONE;