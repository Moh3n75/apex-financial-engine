package com.apex.credit.domain.event;

import com.apex.credit.domain.valueobject.CreditAccountId;

public sealed interface CreditAccountEvent
        extends DomainEvent
        permits CreditAccountCreatedEvent,
        CreditAddedEvent,
        CreditBlockedEvent,
        CreditDebitedEvent{

    CreditAccountId aggregateId();
}