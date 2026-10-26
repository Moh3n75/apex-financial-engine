
package com.apex.inventory.modules.inventory.domain;

import java.util.Objects;
import java.util.UUID;

public record ReservationResult(
        UUID orderId,
        ReservationStatus status
) {
    public ReservationResult {
        Objects.requireNonNull(orderId, "orderId");
        Objects.requireNonNull(status, "status");
    }
}
