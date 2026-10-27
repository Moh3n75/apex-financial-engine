package com.apex.order.modules.order.application;

import com.apex.order.modules.order.domain.OrderRepository;
import com.apex.order.modules.order.events.*;
import com.apex.platform.events.EventMetadata;
import com.apex.platform.events.api.*;
import com.apex.platform.messaging.consumer.OnCellEvent;
import org.springframework.stereotype.Component;
import org.springframework.context.annotation.Profile;

@Component
@Profile("three-cell-saga")
public class ThreeCellOrderHandler {
    private static final EventContract<SagaOrderEvent> READY = EventContracts.define(
            "ORDER_READY_FOR_PAYMENT",1,"ORDER",SagaOrderEvent.class,"order.ready-for-payment.v1");
    private static final EventContract<SagaOrderEvent> CANCEL = EventContracts.define(
            "ORDER_CANCELLATION_REQUESTED",1,"ORDER",SagaOrderEvent.class,"order.cancellation-requested.v1");
    private final OrderRepository orders;
    private final CellEvents events;
    public ThreeCellOrderHandler(OrderRepository orders, CellEvents events) { this.orders=orders; this.events=events; }
    @OnCellEvent(type="INVENTORY_RESERVED",version=1)
    public void reserved(SagaOutcomePayload payload, EventMetadata metadata) { apply(payload,metadata,"INVENTORY_RESERVATION","INVENTORY_RESERVED",READY); }
    @OnCellEvent(type="INVENTORY_RESERVATION_REJECTED",version=1)
    public void rejected(SagaOutcomePayload payload, EventMetadata metadata) { apply(payload,metadata,"INVENTORY_RESERVATION","INVENTORY_RESERVATION_REJECTED",null); }
    @OnCellEvent(type="PAYMENT_AUTHORIZED",version=1)
    public void authorized(SagaOutcomePayload payload, EventMetadata metadata) { apply(payload,metadata,"PAYMENT_AUTHORIZATION","PAYMENT_AUTHORIZED",null); }
    @OnCellEvent(type="PAYMENT_DECLINED",version=1)
    public void declined(SagaOutcomePayload payload, EventMetadata metadata) { apply(payload,metadata,"PAYMENT_AUTHORIZATION","PAYMENT_DECLINED",CANCEL); }
    @OnCellEvent(type="INVENTORY_RELEASED",version=1)
    public void released(SagaOutcomePayload payload, EventMetadata metadata) { apply(payload,metadata,"INVENTORY_RESERVATION","INVENTORY_RELEASED",null); }
    private void apply(SagaOutcomePayload payload, EventMetadata metadata, String aggregate, String outcome,
                       EventContract<SagaOrderEvent> next) {
        if (!aggregate.equals(metadata.aggregateType()) || !payload.orderId().toString().equals(metadata.aggregateId())) {
            throw new IllegalArgumentException("Saga outcome aggregate mismatch");
        }
        if (orders.applySagaOutcome(payload.orderId(),outcome) && next!=null) {
            events.append(next,payload.orderId(),new SagaOrderEvent(payload.orderId()));
        }
    }
}
