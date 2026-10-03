package com.apex.credit.domain;


import com.apex.credit.domain.credit.model.CreditReservation;
import com.apex.credit.domain.credit.model.ReservationStatus;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;


class CreditReservationTest {



    @Test
    void should_create_active_reservation(){


        var reservation =
                CreditReservation.create(
                        10,
                        100,
                        500
                );


        assertEquals(
                ReservationStatus.ACTIVE,
                reservation.status()
        );


        assertEquals(
                500,
                reservation.amount()
        );

    }



    @Test
    void should_consume_reservation(){


        var reservation =
                CreditReservation.create(
                        10,
                        100,
                        500
                );


        reservation.consume();


        assertEquals(
                ReservationStatus.CONSUMED,
                reservation.status()
        );

    }


}