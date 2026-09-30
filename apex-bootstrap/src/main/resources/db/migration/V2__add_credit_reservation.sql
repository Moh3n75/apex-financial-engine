CREATE TABLE credit_reservation
(
    id BIGSERIAL PRIMARY KEY,

    public_id UUID NOT NULL UNIQUE,

    credit_account_id BIGINT NOT NULL,

    transaction_id BIGINT NOT NULL,

    amount_units BIGINT NOT NULL,

    status VARCHAR(30) NOT NULL,

    created_at TIMESTAMP NOT NULL DEFAULT now(),

    updated_at TIMESTAMP NOT NULL DEFAULT now()
);


CREATE INDEX idx_credit_reservation_account
    ON credit_reservation(credit_account_id);


CREATE INDEX idx_credit_reservation_transaction
    ON credit_reservation(transaction_id);