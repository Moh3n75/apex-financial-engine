
package com.apex.order.modules.order.api;

import com.apex.order.modules.order.application.PlaceOrderUseCase;
import com.apex.order.modules.order.domain.OrderItem;
import com.apex.order.modules.order.domain.OrderStatus;

import jakarta.validation.Valid;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Size;

import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.UUID;

@RestController
@RequestMapping("/api/v1/order-placements")
public class OrderPlacementController {

    private final PlaceOrderUseCase placeOrder;

    public OrderPlacementController(PlaceOrderUseCase placeOrder) {
        this.placeOrder = placeOrder;
    }

    @PostMapping
    @ResponseStatus(HttpStatus.CREATED)
    public PlaceOrderResponse place(
            @Valid @RequestBody PlaceOrderRequest request
    ) {
        var items = request.items().stream()
                .map(item -> new OrderItem(
                        item.productId(),
                        item.quantity()
                ))
                .toList();

        var order = placeOrder.execute(
                request.externalReference(),
                request.description(),
                items
        );

        return new PlaceOrderResponse(
                order.id(),
                order.status()
        );
    }

    public record PlaceOrderRequest(
            @NotBlank @Size(max = 64) String externalReference,
            @NotBlank @Size(max = 200) String description,
            @NotEmpty List<@NotNull @Valid ItemRequest> items
    ) {}

    public record ItemRequest(
            @NotNull UUID productId,
            @Min(1) int quantity
    ) {}

    public record PlaceOrderResponse(
            UUID orderId,
            OrderStatus status
    ) {}
}
