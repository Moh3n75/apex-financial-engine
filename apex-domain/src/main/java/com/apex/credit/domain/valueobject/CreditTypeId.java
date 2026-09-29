package com.apex.credit.domain.valueobject;

import java.util.UUID;

public record CreditTypeId(UUID value) {

    public CreditTypeId {
        if (value == null) {
            throw new IllegalArgumentException(
                    "CreditTypeId cannot be null"
            );
        }
    }
}