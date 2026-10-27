-- Historical transitions cannot be reconstructed safely from created_at or status alone.
-- Existing rows deliberately retain NULL; the operational view reports unknown timing.
ALTER TABLE orders ADD COLUMN status_entered_at TIMESTAMPTZ;

CREATE INDEX ix_orders_nonterminal_status_entry
    ON orders (status_entered_at, id)
    WHERE status IN ('PENDING_INVENTORY', 'PENDING_PAYMENT', 'CANCELLING');

-- Bounded owner-local Outbox diagnostics without scanning the full Outbox for each page item.
CREATE INDEX ix_order_outbox_aggregate_id ON outbox_event (aggregate_id);
