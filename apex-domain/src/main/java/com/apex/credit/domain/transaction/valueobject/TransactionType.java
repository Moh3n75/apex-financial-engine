package com.apex.credit.domain.transaction.valueobject;


public record TransactionType(
        String value
) {

    public TransactionType {

        if(value == null || value.isBlank()){

            throw new IllegalArgumentException(
                    "Transaction type cannot be empty"
            );
        }
    }
}