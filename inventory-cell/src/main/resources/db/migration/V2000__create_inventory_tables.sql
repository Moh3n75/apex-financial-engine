CREATE TABLE inventory_stock (
    product_id UUID PRIMARY KEY,
    available_quantity INTEGER NOT NULL,
    reserved_quantity INTEGER NOT NULL DEFAULT 0,
    CONSTRAINT chk_inventory_available CHECK (available_quantity >= 0),
    CONSTRAINT chk_inventory_reserved CHECK (reserved_quantity >= 0)
);

CREATE TABLE inventory_reservations (
    order_id UUID PRIMARY KEY,
    status VARCHAR(32) NOT NULL,
    created_at TIMESTAMPTZ NOT NULL,
    CONSTRAINT chk_inventory_reservation_status
        CHECK (status IN ('RESERVED', 'REJECTED', 'RELEASED'))
);

CREATE TABLE inventory_reservation_items (
    order_id UUID NOT NULL,
    product_id UUID NOT NULL,
    quantity INTEGER NOT NULL,
    CONSTRAINT pk_inventory_reservation_items PRIMARY KEY (order_id, product_id),
    CONSTRAINT fk_inventory_reservation_items_order
        FOREIGN KEY (order_id) REFERENCES inventory_reservations(order_id),
    CONSTRAINT chk_inventory_reservation_item_quantity CHECK (quantity > 0)
);
