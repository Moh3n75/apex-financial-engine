-- Simulation only. No amounts, currencies, charges, capture or ledger operations.
CREATE TABLE payment_simulation_policy (
    order_id UUID PRIMARY KEY,
    decision VARCHAR(16) NOT NULL CHECK (decision IN ('AUTHORIZED','DECLINED'))
);
CREATE TABLE payment_authorizations (
    order_id UUID PRIMARY KEY,
    status VARCHAR(16) NOT NULL CHECK (status IN ('PENDING','AUTHORIZED','DECLINED')),
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);
