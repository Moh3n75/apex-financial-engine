-- New business facts for the opt-in three-cell orchestrator; no legacy data inferred.
ALTER TABLE orders ADD COLUMN inventory_decision VARCHAR(16)
    CHECK (inventory_decision IN ('RESERVED','REJECTED'));
ALTER TABLE orders ADD COLUMN payment_decision VARCHAR(16)
    CHECK (payment_decision IN ('AUTHORIZED','DECLINED'));
ALTER TABLE orders ADD COLUMN release_acknowledged BOOLEAN NOT NULL DEFAULT false;
