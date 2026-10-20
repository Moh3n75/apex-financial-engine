CREATE TABLE orders (
    id UUID PRIMARY KEY,
    external_reference VARCHAR(64) NOT NULL UNIQUE,
    description VARCHAR(200) NOT NULL,
    created_at TIMESTAMPTZ NOT NULL
);
