
package com.apex.inventory.modules.inventory.domain;

import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.UUID;

public record ReservationRequest(
        UUID orderId,
        List<ReservationItem> items
) {
    public ReservationRequest {
        Objects.requireNonNull(orderId, "orderId");
        Objects.requireNonNull(items, "items");

        if (items.isEmpty()) {
            throw new IllegalArgumentException(
                    "Reservation must contain at least one item"
            );
        }

        items = items.stream()
                .map(item -> Objects.requireNonNull(item, "item"))
                .sorted(Comparator.comparing(
                        ReservationItem::productId
                ))
                .toList();

        var productIds = new HashSet<UUID>();

        for (ReservationItem item : items) {
            if (!productIds.add(item.productId())) {
                throw new IllegalArgumentException(
                        "Duplicate product: " + item.productId()
                );
            }
        }
    }
}
