package com.apex.credit.domain.credit.hold.event;

import java.util.UUID;

public record CreditHoldReleasedEvent(

        UUID eventId,

        UUID holdId,

        long amount,

        long version

) implements CreditHoldEvent {

}