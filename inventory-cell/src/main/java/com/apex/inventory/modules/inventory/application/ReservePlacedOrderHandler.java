package com.apex.inventory.modules.inventory.application;

import com.apex.inventory.modules.inventory.domain.ReservationItem;
import com.apex.inventory.modules.inventory.domain.ReservationRequest;
import com.apex.inventory.modules.inventory.events.InventoryContracts;
import com.apex.inventory.modules.inventory.events.InventoryReservedIntegrationEvent;
import com.apex.inventory.modules.inventory.events.InventoryReservationRejectedIntegrationEvent;
import com.apex.inventory.modules.inventory.events.OrderPlacedV1Payload;
import com.apex.platform.events.EventMetadata;
import com.apex.platform.events.api.CellEvents;
import com.apex.platform.messaging.consumer.OnCellEvent;
import org.springframework.stereotype.Component;

@Component
public class ReservePlacedOrderHandler {
    private final ReserveInventoryUseCase reserveInventory;
    private final CellEvents events;

    public ReservePlacedOrderHandler(ReserveInventoryUseCase reserveInventory, CellEvents events) {
        this.reserveInventory = reserveInventory;
        this.events = events;
    }

    @OnCellEvent(type = OrderPlacedV1Payload.EVENT_TYPE,
            version = OrderPlacedV1Payload.EVENT_VERSION)
    public void handle(OrderPlacedV1Payload payload, EventMetadata metadata) {
        if (!payload.orderId().toString().equals(metadata.aggregateId()) || !"ORDER".equals(metadata.aggregateType())) {
            throw new IllegalArgumentException("ORDER_PLACED aggregate does not match orderId");
        }
        var request = new ReservationRequest(payload.orderId(), payload.items().stream()
                .map(item -> new ReservationItem(item.productId(), item.quantity())).toList());
        var result = reserveInventory.execute(request);
        switch (result.status()) {
            case RESERVED -> events.append(InventoryContracts.RESERVED_V1, payload.orderId(),
                    new InventoryReservedIntegrationEvent(payload.orderId()));
            case REJECTED -> events.append(InventoryContracts.REJECTED_V1, payload.orderId(),
                    new InventoryReservationRejectedIntegrationEvent(payload.orderId()));
            case RELEASED -> throw new IllegalStateException(
                    "Cannot reserve an already RELEASED reservation: " + payload.orderId());
        }
    }
}
