package com.apex.credit.domain;

import com.apex.credit.domain.transaction.model.FinancialTransaction;
import com.apex.credit.domain.transaction.model.TransactionStatus;
import com.apex.credit.domain.transaction.valueobject.TransactionType;
import com.apex.credit.domain.valueobject.CreditAmount;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class FinancialTransactionTest {

    @Test
    void should_create_transaction() {

        var transaction =
                FinancialTransaction.create(
                        new TransactionType("PURCHASE"),
                        CreditAmount.of(100_000),
                        "ORDER-1001"
                );

        assertEquals(
                TransactionStatus.CREATED,
                transaction.getStatus()
        );

        assertEquals(
                1,
                transaction.getVersion()
        );

        assertEquals(
                1,
                transaction.uncommittedEvents().size()
        );
    }


    @Test
    void should_complete_transaction() {

        var transaction =
                FinancialTransaction.create(
                        new TransactionType("PURCHASE"),
                        CreditAmount.of(100_000),
                        "ORDER-1002"
                );

        transaction.start();

        assertEquals(
                TransactionStatus.PROCESSING,
                transaction.getStatus()
        );

        transaction.complete();

        assertEquals(
                TransactionStatus.COMPLETED,
                transaction.getStatus()
        );

        assertEquals(
                3,
                transaction.getVersion()
        );
    }


    @Test
    void should_fail_processing_transaction() {

        var transaction =
                FinancialTransaction.create(
                        new TransactionType("PURCHASE"),
                        CreditAmount.of(100_000),
                        "ORDER-1003"
                );

        transaction.start();

        transaction.fail(
                "INSUFFICIENT_CREDIT"
        );

        assertEquals(
                TransactionStatus.FAILED,
                transaction.getStatus()
        );

        assertEquals(
                "INSUFFICIENT_CREDIT",
                transaction.getFailureReason()
        );
    }


    @Test
    void should_not_complete_created_transaction() {

        var transaction =
                FinancialTransaction.create(
                        new TransactionType("PURCHASE"),
                        CreditAmount.of(100_000),
                        "ORDER-1004"
                );

        assertThrows(
                IllegalStateException.class,
                transaction::complete
        );
    }
}
