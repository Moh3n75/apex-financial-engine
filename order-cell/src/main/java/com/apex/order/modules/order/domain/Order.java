
package com.apex.order.modules.order.domain;

import java.time.Instant;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.UUID;

public record Order(
        UUID id,
        String externalReference,
        String description,
        Instant createdAt,
        OrderStatus status,
        List<OrderItem> items
) {

    public Order {
        Objects.requireNonNull(id, "id");
        Objects.requireNonNull(createdAt, "createdAt");
        Objects.requireNonNull(status, "status");
        Objects.requireNonNull(items, "items");

        if (externalReference == null
                || externalReference.isBlank()
                || externalReference.length() > 64) {
            throw new IllegalArgumentException(
                    "externalReference must contain 1 to 64 characters"
            );
        }

        if (description == null
                || description.isBlank()
                || description.length() > 200) {
            throw new IllegalArgumentException(
                    "description must contain 1 to 200 characters"
            );
        }

        items = List.copyOf(items);

        var productIds = new HashSet<UUID>();

        for (OrderItem item : items) {
            if (!productIds.add(item.productId())) {
                throw new IllegalArgumentException(
                        "Duplicate product in order: " + item.productId()
                );
            }
        }

        if (status == OrderStatus.PENDING_INVENTORY
                && items.isEmpty()) {
            throw new IllegalArgumentException(
                    "Saga order must contain at least one item"
            );
        }
    }

    // Backward-compatible constructor
    public Order(
            UUID id,
            String externalReference,
            String description,
            Instant createdAt
    ) {
        this(
                id,
                externalReference,
                description,
                createdAt,
                OrderStatus.CREATED,
                List.of()
        );
    }

    // Existing M24 factory
    public static Order create(
            String externalReference,
            String description,
            Instant createdAt
    ) {
        return new Order(
                UUID.randomUUID(),
                externalReference,
                description,
                createdAt
        );
    }

    // New Saga factory
    public static Order place(
            String externalReference,
            String description,
            List<OrderItem> items,
            Instant createdAt
    ) {
        return new Order(
                UUID.randomUUID(),
                externalReference,
                description,
                createdAt,
                OrderStatus.PENDING_INVENTORY,
                items
        );
    }
}
