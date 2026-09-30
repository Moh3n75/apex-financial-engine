CREATE TABLE credit_hold
(
    id BIGSERIAL PRIMARY KEY,

    public_id UUID NOT NULL UNIQUE,


    credit_account_id BIGINT NOT NULL,

    transaction_id BIGINT,


    hold_type VARCHAR(50) NOT NULL,


    original_amount_units BIGINT NOT NULL,

    remaining_amount_units BIGINT NOT NULL,

    consumed_amount_units BIGINT NOT NULL,


    status VARCHAR(30) NOT NULL,


    reference_type VARCHAR(50),

    reference_id VARCHAR(100),


    expire_at TIMESTAMP,


    version BIGINT NOT NULL DEFAULT 1,


    created_at TIMESTAMP NOT NULL DEFAULT now(),

    updated_at TIMESTAMP NOT NULL DEFAULT now()
);



CREATE INDEX idx_credit_hold_account_status
    ON credit_hold(
                   credit_account_id,
                   status
        );



CREATE INDEX idx_credit_hold_transaction
    ON credit_hold(
                   transaction_id
        );