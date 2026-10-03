package com.apex.credit.domain.credit.hold.event;

import java.util.UUID;

public record CreditHoldConsumedEvent(

        UUID eventId,

        UUID holdId,

        long amount,

        long version

) implements CreditHoldEvent {

}