
package com.apex.inventory.modules.inventory.domain;

import java.util.Objects;
import java.util.UUID;

public record ReservationItem(
        UUID productId,
        int quantity
) {
    public ReservationItem {
        Objects.requireNonNull(productId, "productId");

        if (quantity <= 0) {
            throw new IllegalArgumentException(
                    "Quantity must be positive"
            );
        }
    }
}
