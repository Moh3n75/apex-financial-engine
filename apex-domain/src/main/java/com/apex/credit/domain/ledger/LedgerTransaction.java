package com.apex.credit.domain.ledger;

import com.apex.credit.domain.transaction.valueobject.TransactionId;

import java.util.List;

public final class LedgerTransaction {

    private final TransactionId financialTransactionId;

    private final List<LedgerEntry> entries;


    private LedgerTransaction(
            TransactionId financialTransactionId,
            List<LedgerEntry> entries
    ) {

        this.financialTransactionId =
                financialTransactionId;

        this.entries =
                List.copyOf(entries);
    }


    public static LedgerTransaction create(
            TransactionId financialTransactionId,
            List<LedgerEntry> entries
    ) {

        if (financialTransactionId == null) {
            throw new IllegalArgumentException(
                    "Financial transaction id is required"
            );
        }

        if (entries == null || entries.size() < 2) {
            throw new IllegalArgumentException(
                    "Ledger transaction requires at least two entries"
            );
        }


        long debitTotal = 0;
        long creditTotal = 0;


        for (LedgerEntry entry : entries) {

            switch (entry.type()) {

                case DEBIT ->
                        debitTotal =
                                Math.addExact(
                                        debitTotal,
                                        entry.amount().units()
                                );

                case CREDIT ->
                        creditTotal =
                                Math.addExact(
                                        creditTotal,
                                        entry.amount().units()
                                );
            }
        }


        if (debitTotal != creditTotal) {

            throw new IllegalStateException(
                    "Ledger transaction is not balanced. debit="
                            + debitTotal
                            + ", credit="
                            + creditTotal
            );
        }


        return new LedgerTransaction(
                financialTransactionId,
                entries
        );
    }


    public TransactionId financialTransactionId() {
        return financialTransactionId;
    }


    public List<LedgerEntry> entries() {
        return entries;
    }
}