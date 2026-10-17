CREATE TABLE customer_summary (
    customer_id        UUID         PRIMARY KEY,
    external_reference VARCHAR(64)  NOT NULL,
    display_name       VARCHAR(150) NOT NULL,
    status             VARCHAR(16)  NOT NULL,
    created_at         TIMESTAMPTZ  NOT NULL,
    projected_at       TIMESTAMPTZ  NOT NULL
);

CREATE INDEX customer_summary_external_reference_idx
    ON customer_summary (external_reference);
