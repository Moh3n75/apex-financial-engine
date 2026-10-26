package com.apex.order.modules.order.events;

import java.util.Objects;
import java.util.UUID;

/** Order-owned Inventory outcome wire contract. */
public record InventoryReservationRejectedV1Payload(UUID orderId) {
    public InventoryReservationRejectedV1Payload {
        Objects.requireNonNull(orderId, "orderId");
    }
}
