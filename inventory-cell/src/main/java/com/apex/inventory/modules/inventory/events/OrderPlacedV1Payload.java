package com.apex.inventory.modules.inventory.events;

import java.time.Instant;
import java.util.List;
import java.util.Objects;
import java.util.UUID;

/** Inventory-owned wire contract; no dependency on the Order application. */
public record OrderPlacedV1Payload(UUID orderId, String externalReference, List<Item> items, Instant placedAt) {
    public static final String EVENT_TYPE = "ORDER_PLACED";
    public static final int EVENT_VERSION = 1;

    public OrderPlacedV1Payload {
        Objects.requireNonNull(orderId, "orderId");
        Objects.requireNonNull(externalReference, "externalReference");
        Objects.requireNonNull(placedAt, "placedAt");
        items = List.copyOf(Objects.requireNonNull(items, "items"));
        if (items.isEmpty()) {
            throw new IllegalArgumentException("Order must contain at least one item");
        }
    }

    public record Item(UUID productId, int quantity) {
        public Item {
            Objects.requireNonNull(productId, "productId");
            if (quantity <= 0) {
                throw new IllegalArgumentException("Quantity must be positive");
            }
        }
    }
}
