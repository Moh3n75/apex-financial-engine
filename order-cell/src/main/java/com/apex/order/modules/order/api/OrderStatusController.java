package com.apex.order.modules.order.api;

import java.util.UUID;
import com.apex.order.modules.order.domain.OrderRepository;
import com.apex.order.modules.order.domain.OrderStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class OrderStatusController {
    private final OrderRepository orders;

    public OrderStatusController(OrderRepository orders) {
        this.orders = orders;
    }

    @GetMapping("/api/v1/order-placements/{orderId}")
    public ResponseEntity<StatusResponse> status(@PathVariable UUID orderId) {
        return orders.findStatus(orderId)
                .map(status -> ResponseEntity.ok(new StatusResponse(orderId, status)))
                .orElseGet(() -> ResponseEntity.notFound().build());
    }

    public record StatusResponse(UUID orderId, OrderStatus status) {}
}
