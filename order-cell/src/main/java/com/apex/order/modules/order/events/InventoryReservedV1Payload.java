package com.apex.order.modules.order.events;

import java.util.Objects;
import java.util.UUID;

/** Order-owned Inventory outcome wire contract. */
public record InventoryReservedV1Payload(UUID orderId) {
    public InventoryReservedV1Payload {
        Objects.requireNonNull(orderId, "orderId");
    }
}
