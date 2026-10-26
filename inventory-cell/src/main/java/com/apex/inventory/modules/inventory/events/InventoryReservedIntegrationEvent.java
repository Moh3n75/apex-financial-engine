package com.apex.inventory.modules.inventory.events;

import java.util.Objects;
import java.util.UUID;
import com.apex.platform.events.IntegrationEvent;

public record InventoryReservedIntegrationEvent(UUID orderId) implements IntegrationEvent {
    public InventoryReservedIntegrationEvent {
        Objects.requireNonNull(orderId, "orderId");
    }
}
