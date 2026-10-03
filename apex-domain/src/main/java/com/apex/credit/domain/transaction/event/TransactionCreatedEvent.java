package com.apex.credit.domain.transaction.event;

import com.apex.credit.domain.transaction.valueobject.TransactionId;
import com.apex.credit.domain.transaction.valueobject.TransactionType;
import com.apex.credit.domain.valueobject.CreditAmount;

import java.util.UUID;

public record TransactionCreatedEvent(

        UUID eventId,
        TransactionId aggregateId,
        TransactionType type,
        CreditAmount requestedAmount,
        String referenceId,
        long version

) implements FinancialTransactionEvent {
}