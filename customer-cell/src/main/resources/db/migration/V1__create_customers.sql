CREATE TABLE customers (
    id                 UUID         PRIMARY KEY,
    external_reference VARCHAR(64)  NOT NULL,
    display_name       VARCHAR(150) NOT NULL,
    status             VARCHAR(16)  NOT NULL,
    created_at         TIMESTAMPTZ  NOT NULL,
    CONSTRAINT customers_external_reference_uq UNIQUE (external_reference),
    CONSTRAINT customers_status_chk CHECK (status IN ('ACTIVE'))
);

CREATE INDEX customers_created_at_idx ON customers (created_at);
