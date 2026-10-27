package com.apex.inventory.modules.inventory.events;
import java.util.UUID;
public record OrderCancellationV1Payload(UUID orderId) {
    public OrderCancellationV1Payload { java.util.Objects.requireNonNull(orderId); }
}
