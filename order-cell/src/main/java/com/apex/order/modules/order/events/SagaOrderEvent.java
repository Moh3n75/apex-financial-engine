package com.apex.order.modules.order.events;
import java.util.UUID;
import com.apex.platform.events.IntegrationEvent;
public record SagaOrderEvent(UUID orderId) implements IntegrationEvent {
    public SagaOrderEvent { java.util.Objects.requireNonNull(orderId); }
}
