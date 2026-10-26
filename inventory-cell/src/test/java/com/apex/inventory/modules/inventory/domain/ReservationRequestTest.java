
package com.apex.inventory.modules.inventory.domain;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

class ReservationRequestTest {

    @Test
    void sameItemsInDifferentOrderAreEquivalent() {
        UUID orderId = UUID.randomUUID();
        UUID productA = UUID.randomUUID();
        UUID productB = UUID.randomUUID();

        var first = new ReservationRequest(
                orderId,
                List.of(
                        new ReservationItem(productA, 2),
                        new ReservationItem(productB, 1)
                )
        );

        var second = new ReservationRequest(
                orderId,
                List.of(
                        new ReservationItem(productB, 1),
                        new ReservationItem(productA, 2)
                )
        );

        assertEquals(first, second);
    }

    @Test
    void duplicateProductIsRejected() {
        UUID productId = UUID.randomUUID();

        assertThrows(IllegalArgumentException.class,
                () -> new ReservationRequest(
                        UUID.randomUUID(),
                        List.of(
                                new ReservationItem(productId, 2),
                                new ReservationItem(productId, 3)
                        )
                ));
    }

    @Test
    void nonPositiveQuantityIsRejected() {
        assertThrows(IllegalArgumentException.class,
                () -> new ReservationItem(UUID.randomUUID(), 0));
    }
}
