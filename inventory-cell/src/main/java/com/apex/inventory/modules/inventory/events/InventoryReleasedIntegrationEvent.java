package com.apex.inventory.modules.inventory.events;
import java.util.UUID;
import com.apex.platform.events.IntegrationEvent;
public record InventoryReleasedIntegrationEvent(UUID orderId) implements IntegrationEvent { }
