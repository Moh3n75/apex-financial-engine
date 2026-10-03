package com.apex.credit.domain.ledger;

import com.apex.credit.domain.valueobject.CreditAccountId;
import com.apex.credit.domain.valueobject.CreditAmount;

public record LedgerEntry(

        CreditAccountId accountId,
        LedgerEntryType type,
        CreditAmount amount

) {

    public LedgerEntry {

        if (accountId == null) {
            throw new IllegalArgumentException(
                    "Ledger account id is required"
            );
        }

        if (type == null) {
            throw new IllegalArgumentException(
                    "Ledger entry type is required"
            );
        }

        if (amount == null || !amount.isPositive()) {
            throw new IllegalArgumentException(
                    "Ledger amount must be positive"
            );
        }
    }
}