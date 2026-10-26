-- Demo-only: run manually against a separately provisioned DISPOSABLE Inventory database.
-- No production endpoint/startup code invokes this file; existing stock is never overwritten.
INSERT INTO inventory_stock(product_id, available_quantity, reserved_quantity)
VALUES ('11111111-1111-4111-8111-111111111111', 10, 0),
       ('22222222-2222-4222-8222-222222222222', 5, 0)
ON CONFLICT (product_id) DO NOTHING;
