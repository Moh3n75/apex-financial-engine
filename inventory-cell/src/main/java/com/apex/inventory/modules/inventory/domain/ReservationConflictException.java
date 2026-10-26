
package com.apex.inventory.modules.inventory.domain;

import java.util.UUID;

public class ReservationConflictException
        extends RuntimeException {

    public ReservationConflictException(UUID orderId) {
        super(
                "Reservation already exists with different items"
                        + " for order: " + orderId
        );
    }
}
