package com.apex.credit.domain.transaction.event;

import com.apex.credit.domain.transaction.valueobject.TransactionId;

import java.util.UUID;

public record TransactionFailedEvent(

        UUID eventId,
        TransactionId aggregateId,
        String reasonCode,
        long version

) implements FinancialTransactionEvent {
}