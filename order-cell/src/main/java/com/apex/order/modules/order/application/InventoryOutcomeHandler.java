package com.apex.order.modules.order.application;

import com.apex.order.modules.order.domain.OrderRepository;
import com.apex.order.modules.order.domain.OrderStatus;
import com.apex.order.modules.order.events.InventoryReservedV1Payload;
import com.apex.order.modules.order.events.InventoryReservationRejectedV1Payload;
import com.apex.platform.events.EventMetadata;
import com.apex.platform.messaging.consumer.OnCellEvent;
import org.springframework.stereotype.Component;

import java.util.UUID;

@Component
public class InventoryOutcomeHandler {
    private final OrderRepository orders;

    public InventoryOutcomeHandler(OrderRepository orders) {
        this.orders = orders;
    }

    @OnCellEvent(type = "INVENTORY_RESERVED", version = 1)
    public void reserved(InventoryReservedV1Payload payload, EventMetadata metadata) {
        apply(payload.orderId(), OrderStatus.PENDING_PAYMENT, metadata);
    }

    @OnCellEvent(type = "INVENTORY_RESERVATION_REJECTED", version = 1)
    public void rejected(InventoryReservationRejectedV1Payload payload, EventMetadata metadata) {
        apply(payload.orderId(), OrderStatus.CANCELLED, metadata);
    }

    private void apply(UUID orderId, OrderStatus target, EventMetadata metadata) {
        if (!orderId.toString().equals(metadata.aggregateId())
                || !"INVENTORY_RESERVATION".equals(metadata.aggregateType())) {
            throw new IllegalArgumentException("Inventory outcome aggregate does not match orderId");
        }
        orders.applyInventoryOutcome(orderId, target);
    }
}
