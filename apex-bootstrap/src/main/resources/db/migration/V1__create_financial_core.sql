CREATE TABLE credit_account_state
(
    id BIGSERIAL PRIMARY KEY,

    public_id UUID NOT NULL UNIQUE,

    owner_id BIGINT NOT NULL,

    credit_type VARCHAR(50) NOT NULL,


    available_units BIGINT NOT NULL DEFAULT 0,

    blocked_units BIGINT NOT NULL DEFAULT 0,


    version BIGINT NOT NULL DEFAULT 1,


    created_at TIMESTAMP NOT NULL DEFAULT now(),

    updated_at TIMESTAMP NOT NULL DEFAULT now()
);



CREATE TABLE financial_transaction
(
    id BIGSERIAL PRIMARY KEY,

    public_id UUID NOT NULL UNIQUE,

    type VARCHAR(50) NOT NULL,

    status VARCHAR(50) NOT NULL,

    amount_units BIGINT NOT NULL,

    version BIGINT NOT NULL DEFAULT 1,

    created_at TIMESTAMP NOT NULL DEFAULT now(),

    updated_at TIMESTAMP NOT NULL DEFAULT now()
);



CREATE TABLE ledger_transaction
(
    id BIGSERIAL PRIMARY KEY,

    transaction_id BIGINT NOT NULL,

    created_at TIMESTAMP NOT NULL DEFAULT now()
);



CREATE TABLE ledger_entry
(
    id BIGSERIAL PRIMARY KEY,

    ledger_transaction_id BIGINT NOT NULL,

    account_id BIGINT NOT NULL,

    entry_type VARCHAR(20) NOT NULL,

    amount_units BIGINT NOT NULL,

    created_at TIMESTAMP NOT NULL DEFAULT now()
);



CREATE TABLE outbox_event
(
    id BIGSERIAL PRIMARY KEY,

    event_type VARCHAR(100) NOT NULL,

    aggregate_id BIGINT NOT NULL,

    payload JSONB NOT NULL,

    published BOOLEAN NOT NULL DEFAULT FALSE,

    created_at TIMESTAMP NOT NULL DEFAULT now()
);