package com.apex.credit.application.outbox;

import com.apex.credit.domain.transaction.valueobject.TransactionId;

public record OutboxMessage(

        String eventType,

        TransactionId aggregateId,

        String payload

) {

    public OutboxMessage {

        if (eventType == null || eventType.isBlank()) {
            throw new IllegalArgumentException(
                    "Event type is required"
            );
        }

        if (aggregateId == null) {
            throw new IllegalArgumentException(
                    "Aggregate id is required"
            );
        }

        if (payload == null || payload.isBlank()) {
            throw new IllegalArgumentException(
                    "Payload is required"
            );
        }
    }
}