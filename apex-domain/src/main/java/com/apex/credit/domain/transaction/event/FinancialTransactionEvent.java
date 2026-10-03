package com.apex.credit.domain.transaction.event;

import com.apex.credit.domain.event.DomainEvent;
import com.apex.credit.domain.transaction.valueobject.TransactionId;

public sealed interface FinancialTransactionEvent
        extends DomainEvent
        permits TransactionCreatedEvent,
        TransactionStartedEvent,
        TransactionCompletedEvent,
        TransactionFailedEvent {

    TransactionId aggregateId();
}