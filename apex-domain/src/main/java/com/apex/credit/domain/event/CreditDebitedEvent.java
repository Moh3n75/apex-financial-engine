package com.apex.credit.domain.event;

import com.apex.credit.domain.valueobject.CreditAccountId;
import com.apex.credit.domain.valueobject.CreditAmount;

import java.util.UUID;

public record CreditDebitedEvent(

        UUID eventId,

        CreditAccountId aggregateId,

        CreditAmount amount,

        String referenceId,

        long version

) implements CreditAccountEvent {

}