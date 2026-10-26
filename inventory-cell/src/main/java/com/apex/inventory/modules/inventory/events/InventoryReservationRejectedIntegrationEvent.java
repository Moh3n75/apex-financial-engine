package com.apex.inventory.modules.inventory.events;

import java.util.Objects;
import java.util.UUID;
import com.apex.platform.events.IntegrationEvent;

public record InventoryReservationRejectedIntegrationEvent(UUID orderId) implements IntegrationEvent {
    public InventoryReservationRejectedIntegrationEvent {
        Objects.requireNonNull(orderId, "orderId");
    }
}
