package com.apex.credit.application.purchase;

import com.apex.credit.domain.valueobject.CreditAccountId;
import com.apex.credit.domain.valueobject.CreditAmount;

public record PurchaseCommand(

        CreditAccountId sourceAccountId,

        CreditAccountId destinationAccountId,

        CreditAmount amount,

        String referenceId

) {

    public PurchaseCommand {

        if (sourceAccountId == null) {
            throw new IllegalArgumentException(
                    "Source account is required"
            );
        }

        if (destinationAccountId == null) {
            throw new IllegalArgumentException(
                    "Destination account is required"
            );
        }

        if (sourceAccountId.equals(destinationAccountId)) {
            throw new IllegalArgumentException(
                    "Source and destination cannot be the same"
            );
        }

        if (amount == null || !amount.isPositive()) {
            throw new IllegalArgumentException(
                    "Amount must be positive"
            );
        }

        if (referenceId == null || referenceId.isBlank()) {
            throw new IllegalArgumentException(
                    "Reference id is required"
            );
        }
    }
}