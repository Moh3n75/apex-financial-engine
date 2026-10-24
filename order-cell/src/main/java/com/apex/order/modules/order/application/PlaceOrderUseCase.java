
package com.apex.order.modules.order.application;

import com.apex.order.modules.order.domain.Order;
import com.apex.order.modules.order.domain.OrderItem;
import com.apex.order.modules.order.domain.OrderRepository;
import com.apex.order.modules.order.events.OrderContracts;
import com.apex.order.modules.order.events.OrderPlacedIntegrationEvent;
import com.apex.platform.events.api.CellEvents;

import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Clock;
import java.util.List;

@Service
public class PlaceOrderUseCase {

    private final OrderRepository orders;
    private final CellEvents events;
    private final Clock clock;

    public PlaceOrderUseCase(
            OrderRepository orders,
            CellEvents events,
            Clock clock
    ) {
        this.orders = orders;
        this.events = events;
        this.clock = clock;
    }

    @Transactional
    public Order execute(
            String externalReference,
            String description,
            List<OrderItem> items
    ) {
        Order order = Order.place(
                externalReference,
                description,
                items,
                clock.instant()
        );

        orders.save(order);

        var eventItems = order.items().stream()
                .map(item -> new OrderPlacedIntegrationEvent.Item(
                        item.productId(),
                        item.quantity()
                ))
                .toList();

        events.append(
                OrderContracts.PLACED_V1,
                order.id(),
                new OrderPlacedIntegrationEvent(
                        order.id(),
                        order.externalReference(),
                        eventItems,
                        order.createdAt()
                )
        );

        return order;
    }
}
