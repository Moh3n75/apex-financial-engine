package com.apex.order.modules.order.domain;

import java.util.Optional;
import java.util.UUID;

public interface OrderRepository {
    void save(Order order);
    Optional<OrderStatus> findStatus(UUID orderId);
    void applyInventoryOutcome(UUID orderId, OrderStatus target);
}
