ALTER TABLE credit_account_state
    ADD COLUMN member_public_id UUID,
    ADD COLUMN credit_type_public_id UUID;


ALTER TABLE financial_transaction
    ADD COLUMN reference_id VARCHAR(120),
    ADD COLUMN failure_reason VARCHAR(100);


CREATE UNIQUE INDEX uq_financial_transaction_reference
    ON financial_transaction(reference_id)
    WHERE reference_id IS NOT NULL;