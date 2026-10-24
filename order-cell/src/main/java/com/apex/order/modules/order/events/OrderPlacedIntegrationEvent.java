
package com.apex.order.modules.order.events;

import com.apex.platform.events.IntegrationEvent;

import java.time.Instant;
import java.util.List;
import java.util.Objects;
import java.util.UUID;

public record OrderPlacedIntegrationEvent(
        UUID orderId,
        String externalReference,
        List<Item> items,
        Instant placedAt
) implements IntegrationEvent {

    public static final String EVENT_TYPE = "ORDER_PLACED";
    public static final int EVENT_VERSION = 1;
    public static final String AGGREGATE_TYPE = "ORDER";

    public OrderPlacedIntegrationEvent {
        Objects.requireNonNull(orderId, "orderId");
        Objects.requireNonNull(externalReference, "externalReference");
        Objects.requireNonNull(items, "items");
        Objects.requireNonNull(placedAt, "placedAt");

        items = List.copyOf(items);

        if (items.isEmpty()) {
            throw new IllegalArgumentException(
                    "Order must contain at least one item"
            );
        }
    }

    public record Item(UUID productId, int quantity) {
        public Item {
            Objects.requireNonNull(productId, "productId");
            if (quantity <= 0) {
                throw new IllegalArgumentException(
                        "Quantity must be positive"
                );
            }
        }
    }
}
