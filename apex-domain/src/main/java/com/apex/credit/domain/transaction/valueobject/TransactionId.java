package com.apex.credit.domain.transaction.valueobject;

import java.util.UUID;

public record TransactionId(UUID value) {

    public TransactionId {
        if (value == null) {
            throw new IllegalArgumentException(
                    "TransactionId cannot be null"
            );
        }
    }

    public static TransactionId generate() {
        return new TransactionId(UUID.randomUUID());
    }
}