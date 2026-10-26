
package com.apex.inventory.modules.inventory.domain;

import org.junit.jupiter.api.Test;

import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

class ReservationResultTest {

    @Test
    void createsSuccessfulReservationResult() {
        UUID orderId = UUID.randomUUID();

        var result = new ReservationResult(
                orderId,
                ReservationStatus.RESERVED
        );

        assertEquals(orderId, result.orderId());
        assertEquals(
                ReservationStatus.RESERVED,
                result.status()
        );
    }

    @Test
    void createsRejectedReservationResult() {
        var result = new ReservationResult(
                UUID.randomUUID(),
                ReservationStatus.REJECTED
        );

        assertEquals(
                ReservationStatus.REJECTED,
                result.status()
        );
    }

    @Test
    void rejectsNullStatus() {
        assertThrows(
                NullPointerException.class,
                () -> new ReservationResult(
                        UUID.randomUUID(),
                        null
                )
        );
    }

    @Test
    void conflictExceptionContainsOrderId() {
        UUID orderId = UUID.randomUUID();

        var exception =
                new ReservationConflictException(orderId);

        assertTrue(
                exception.getMessage()
                        .contains(orderId.toString())
        );
    }
}
