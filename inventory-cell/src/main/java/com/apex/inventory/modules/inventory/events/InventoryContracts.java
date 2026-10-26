package com.apex.inventory.modules.inventory.events;

import com.apex.platform.events.api.EventContract;
import com.apex.platform.events.api.EventContracts;

public final class InventoryContracts {
    private InventoryContracts() {}

    public static final EventContract<InventoryReservedIntegrationEvent> RESERVED_V1 = EventContracts.define(
            "INVENTORY_RESERVED", 1, "INVENTORY_RESERVATION",
            InventoryReservedIntegrationEvent.class, "inventory.reserved.v1");

    public static final EventContract<InventoryReservationRejectedIntegrationEvent> REJECTED_V1 = EventContracts.define(
            "INVENTORY_RESERVATION_REJECTED", 1, "INVENTORY_RESERVATION",
            InventoryReservationRejectedIntegrationEvent.class, "inventory.reservation-rejected.v1");
}
