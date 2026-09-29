package com.apex.credit.domain.event;

import com.apex.credit.domain.valueobject.CreditAccountId;
import com.apex.credit.domain.valueobject.CreditTypeId;
import com.apex.credit.domain.valueobject.MemberId;

import java.util.UUID;

public record CreditAccountCreatedEvent(

        UUID eventId,
        CreditAccountId aggregateId,
        MemberId memberId,
        CreditTypeId creditTypeId,
        long version

) implements CreditAccountEvent {
}