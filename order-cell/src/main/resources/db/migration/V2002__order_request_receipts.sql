-- Order-owned only. The receipt and existing Order/items/Outbox commit atomically.
CREATE TABLE order_request_receipt (
    idempotency_key VARCHAR(128) PRIMARY KEY,
    client_id VARCHAR(64) NOT NULL,
    tenant_id VARCHAR(128) NOT NULL,
    principal VARCHAR(128) NOT NULL,
    contract_type VARCHAR(64) NOT NULL,
    contract_version INTEGER NOT NULL CHECK (contract_version > 0),
    request_id UUID NOT NULL,
    fingerprint CHAR(64) NOT NULL,
    response BYTEA,
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    completed_at TIMESTAMPTZ,
    CONSTRAINT uq_order_rpc_request UNIQUE (client_id,request_id),
    CONSTRAINT ck_order_rpc_response CHECK (
        (response IS NULL AND completed_at IS NULL) OR
        (response IS NOT NULL AND completed_at IS NOT NULL AND octet_length(response) <= 65536))
);
