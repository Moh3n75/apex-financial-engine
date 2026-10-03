package com.apex.credit.domain.credit.hold.event;


import com.apex.credit.domain.credit.hold.valueobject.HoldType;

import java.util.UUID;


public record CreditHoldCreatedEvent(

        UUID eventId,

        UUID holdId,

        long creditAccountId,

        long transactionId,

        long amount,

        HoldType holdType,

        long version

) implements CreditHoldEvent {

}