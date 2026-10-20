package com.apex.order.modules.order.events;

import com.apex.platform.events.api.EventContract;
import com.apex.platform.events.api.EventContracts;

public final class OrderContracts {
    private OrderContracts() {}

    public static final EventContract<OrderCreatedIntegrationEvent> CREATED_V1 = EventContracts.define(
            "ORDER_CREATED", 1, "ORDER", OrderCreatedIntegrationEvent.class, "order.created.v1");
}
