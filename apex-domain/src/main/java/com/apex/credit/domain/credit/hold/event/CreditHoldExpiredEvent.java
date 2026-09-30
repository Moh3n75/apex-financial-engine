package com.apex.credit.domain.credit.hold.event;

import java.util.UUID;

public record CreditHoldExpiredEvent(

        UUID eventId,

        UUID holdId,

        long version

) implements CreditHoldEvent {

}