package com.apex.credit.domain.transaction.event;

import com.apex.credit.domain.transaction.valueobject.TransactionId;

import java.util.UUID;

public record TransactionCompletedEvent(

        UUID eventId,
        TransactionId aggregateId,
        long version

) implements FinancialTransactionEvent {
}