package com.apex.order.modules.order.events;
import java.util.UUID;
public record SagaOutcomePayload(UUID orderId) {
    public SagaOutcomePayload { java.util.Objects.requireNonNull(orderId); }
}
