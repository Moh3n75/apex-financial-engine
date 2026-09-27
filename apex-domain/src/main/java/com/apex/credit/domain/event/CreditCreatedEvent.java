package com.apex.credit.domain.event;


import com.apex.credit.domain.valueobject.CreditAccountId;
import com.apex.credit.domain.valueobject.MemberId;

import java.util.UUID;


public record CreditCreatedEvent(

        UUID eventId,

        CreditAccountId aggregateId,

        MemberId memberId,

        long version

) implements DomainEvent {


}
