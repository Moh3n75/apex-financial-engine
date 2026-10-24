ALTER TABLE orders
    ADD COLUMN status VARCHAR(32)
        NOT NULL DEFAULT 'CREATED';

CREATE TABLE order_items
(
    order_id   UUID    NOT NULL REFERENCES orders (id),
    product_id UUID    NOT NULL,
    quantity   INTEGER NOT NULL CHECK (quantity > 0),

    PRIMARY KEY (order_id, product_id)
);