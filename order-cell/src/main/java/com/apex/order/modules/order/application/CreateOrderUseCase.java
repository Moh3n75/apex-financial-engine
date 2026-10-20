package com.apex.order.modules.order.application;

import java.time.Clock;

import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import com.apex.order.modules.order.domain.Order;
import com.apex.order.modules.order.domain.OrderRepository;
import com.apex.order.modules.order.events.OrderContracts;
import com.apex.order.modules.order.events.OrderCreatedIntegrationEvent;
import com.apex.platform.events.api.CellEvents;

@Service
public class CreateOrderUseCase {
    private final OrderRepository orders;
    private final CellEvents events;
    private final Clock clock;

    public CreateOrderUseCase(OrderRepository orders, CellEvents events, Clock clock) {
        this.orders = orders;
        this.events = events;
        this.clock = clock;
    }

    @Transactional
    public Order execute(String externalReference, String description) {
        Order order = Order.create(externalReference, description, clock.instant());
        orders.save(order);
        events.append(OrderContracts.CREATED_V1, order.id(), new OrderCreatedIntegrationEvent(
                order.id(), order.externalReference(), order.description(), order.createdAt()));
        return order;
    }
}
