package com.apex.credit.domain.event;

import com.apex.credit.domain.valueobject.CreditAccountId;
import com.apex.credit.domain.valueobject.Money;

import java.util.UUID;

public record CreditBlockedEvent(

        UUID eventId,

        CreditAccountId aggregateId,

        Money amount,

        String referenceId,

        long version


) implements DomainEvent {


}
