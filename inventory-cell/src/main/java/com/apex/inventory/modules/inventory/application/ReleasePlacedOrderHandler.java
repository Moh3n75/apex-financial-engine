package com.apex.inventory.modules.inventory.application;

import com.apex.inventory.modules.inventory.events.*;
import com.apex.platform.events.EventMetadata;
import com.apex.platform.events.api.*;
import com.apex.platform.messaging.consumer.OnCellEvent;
import org.springframework.stereotype.Component;
import org.springframework.context.annotation.Profile;

@Component
@Profile("three-cell-saga")
public class ReleasePlacedOrderHandler {
    private static final EventContract<InventoryReleasedIntegrationEvent> RELEASED=EventContracts.define(
            "INVENTORY_RELEASED",1,"INVENTORY_RESERVATION",InventoryReleasedIntegrationEvent.class,"inventory.released.v1");
    private final ReleaseInventoryUseCase release;
    private final CellEvents events;
    public ReleasePlacedOrderHandler(ReleaseInventoryUseCase release,CellEvents events) { this.release=release; this.events=events; }
    @OnCellEvent(type="ORDER_CANCELLATION_REQUESTED",version=1)
    public void handle(OrderCancellationV1Payload payload,EventMetadata metadata) {
        if(!"ORDER".equals(metadata.aggregateType()) || !payload.orderId().toString().equals(metadata.aggregateId())) {
            throw new IllegalArgumentException("Release aggregate mismatch");
        }
        release.execute(payload.orderId());
        // Re-acknowledge an already released business operation for outcome liveness, never mutate stock twice.
        events.append(RELEASED,payload.orderId(),new InventoryReleasedIntegrationEvent(payload.orderId()));
    }
}
