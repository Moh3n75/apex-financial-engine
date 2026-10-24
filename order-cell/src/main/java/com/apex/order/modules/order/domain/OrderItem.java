
package com.apex.order.modules.order.domain;

import java.util.Objects;
import java.util.UUID;

public record OrderItem(
        UUID productId,
        int quantity
) {
    public OrderItem {
        Objects.requireNonNull(productId, "productId");

        if (quantity <= 0) {
            throw new IllegalArgumentException(
                    "Order item quantity must be positive"
            );
        }
    }
}
