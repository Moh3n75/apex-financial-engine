package com.apex.apexbootstrap.purchase;

import com.apex.credit.application.purchase.PurchaseApplicationService;
import com.apex.credit.application.purchase.PurchaseCommand;

import com.apex.credit.domain.transaction.model.TransactionStatus;
import com.apex.credit.domain.valueobject.CreditAccountId;
import com.apex.credit.domain.valueobject.CreditAmount;

import com.apex.infrastructure.jooq.generated.tables.records.LedgerEntryRecord;
import org.jooq.DSLContext;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.transaction.annotation.Transactional;

import java.util.UUID;

import static com.apex.infrastructure.jooq.generated.Tables.CREDIT_ACCOUNT_STATE;
import static com.apex.infrastructure.jooq.generated.Tables.FINANCIAL_TRANSACTION;
import static com.apex.infrastructure.jooq.generated.Tables.LEDGER_ENTRY;
import static com.apex.infrastructure.jooq.generated.Tables.LEDGER_TRANSACTION;
import static com.apex.infrastructure.jooq.generated.Tables.OUTBOX_EVENT;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;


@SpringBootTest
@Transactional
class PurchaseIntegrationTest {


    private static final UUID BUYER_ACCOUNT_ID =
            UUID.fromString(
                    "11111111-1111-1111-1111-111111111111"
            );


    private static final UUID SELLER_ACCOUNT_ID =
            UUID.fromString(
                    "22222222-2222-2222-2222-222222222222"
            );


    private static final UUID BUYER_MEMBER_ID =
            UUID.fromString(
                    "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"
            );


    private static final UUID SELLER_MEMBER_ID =
            UUID.fromString(
                    "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb"
            );


    private static final UUID GENERAL_CREDIT_TYPE_ID =
            UUID.fromString(
                    "cccccccc-cccc-cccc-cccc-cccccccccccc"
            );


    @Autowired
    private DSLContext dsl;


    @Autowired
    private PurchaseApplicationService purchaseService;


    @BeforeEach
    void prepareDatabase() {

        /*
         * چون تست @Transactional است،
         * همه تغییرات در پایان تست Rollback می‌شوند.
         */

        dsl.deleteFrom(FINANCIAL_TRANSACTION)
                .where(
                        FINANCIAL_TRANSACTION.REFERENCE_ID
                                .like("IT-PURCHASE-%")
                )
                .execute();


        dsl.deleteFrom(CREDIT_ACCOUNT_STATE)
                .where(
                        CREDIT_ACCOUNT_STATE.PUBLIC_ID
                                .in(
                                        BUYER_ACCOUNT_ID,
                                        SELLER_ACCOUNT_ID
                                )
                )
                .execute();


        /*
         * Buyer
         */
        dsl.insertInto(CREDIT_ACCOUNT_STATE)

                .set(
                        CREDIT_ACCOUNT_STATE.PUBLIC_ID,
                        BUYER_ACCOUNT_ID
                )

                .set(
                        CREDIT_ACCOUNT_STATE.OWNER_ID,
                        1L
                )

                .set(
                        CREDIT_ACCOUNT_STATE.CREDIT_TYPE,
                        "GENERAL"
                )

                .set(
                        CREDIT_ACCOUNT_STATE.MEMBER_PUBLIC_ID,
                        BUYER_MEMBER_ID
                )

                .set(
                        CREDIT_ACCOUNT_STATE.CREDIT_TYPE_PUBLIC_ID,
                        GENERAL_CREDIT_TYPE_ID
                )

                .set(
                        CREDIT_ACCOUNT_STATE.AVAILABLE_UNITS,
                        100_000L
                )

                .set(
                        CREDIT_ACCOUNT_STATE.BLOCKED_UNITS,
                        0L
                )

                .set(
                        CREDIT_ACCOUNT_STATE.VERSION,
                        1L
                )

                .execute();


        /*
         * Seller
         */
        dsl.insertInto(CREDIT_ACCOUNT_STATE)

                .set(
                        CREDIT_ACCOUNT_STATE.PUBLIC_ID,
                        SELLER_ACCOUNT_ID
                )

                .set(
                        CREDIT_ACCOUNT_STATE.OWNER_ID,
                        2L
                )

                .set(
                        CREDIT_ACCOUNT_STATE.CREDIT_TYPE,
                        "GENERAL"
                )

                .set(
                        CREDIT_ACCOUNT_STATE.MEMBER_PUBLIC_ID,
                        SELLER_MEMBER_ID
                )

                .set(
                        CREDIT_ACCOUNT_STATE.CREDIT_TYPE_PUBLIC_ID,
                        GENERAL_CREDIT_TYPE_ID
                )

                .set(
                        CREDIT_ACCOUNT_STATE.AVAILABLE_UNITS,
                        10_000L
                )

                .set(
                        CREDIT_ACCOUNT_STATE.BLOCKED_UNITS,
                        0L
                )

                .set(
                        CREDIT_ACCOUNT_STATE.VERSION,
                        1L
                )

                .execute();
    }


    @Test
    void shouldExecutePurchaseAndPersistChanges() {

        String referenceId =
                "IT-PURCHASE-" + UUID.randomUUID();


        /*
         * Execute
         */
        var result =
                purchaseService.execute(

                        new PurchaseCommand(

                                new CreditAccountId(
                                        BUYER_ACCOUNT_ID
                                ),

                                new CreditAccountId(
                                        SELLER_ACCOUNT_ID
                                ),

                                new CreditAmount(
                                        30_000L
                                ),

                                referenceId
                        )

                );


        /*
         * Transaction result
         */
        assertNotNull(
                result.transactionId()
        );

        assertEquals(
                TransactionStatus.COMPLETED,
                result.status()
        );


        /*
         * Read balances directly from DB
         */
        Long buyerBalance =
                dsl.select(
                                CREDIT_ACCOUNT_STATE.AVAILABLE_UNITS
                        )
                        .from(
                                CREDIT_ACCOUNT_STATE
                        )
                        .where(
                                CREDIT_ACCOUNT_STATE.PUBLIC_ID.eq(
                                        BUYER_ACCOUNT_ID
                                )
                        )
                        .fetchOne(
                                CREDIT_ACCOUNT_STATE.AVAILABLE_UNITS
                        );


        Long sellerBalance =
                dsl.select(
                                CREDIT_ACCOUNT_STATE.AVAILABLE_UNITS
                        )
                        .from(
                                CREDIT_ACCOUNT_STATE
                        )
                        .where(
                                CREDIT_ACCOUNT_STATE.PUBLIC_ID.eq(
                                        SELLER_ACCOUNT_ID
                                )
                        )
                        .fetchOne(
                                CREDIT_ACCOUNT_STATE.AVAILABLE_UNITS
                        );


        assertEquals(
                70_000L,
                buyerBalance
        );


        assertEquals(
                40_000L,
                sellerBalance
        );


        /*
         * Verify financial transaction
         */
        var financialTransaction =
                dsl.selectFrom(
                                FINANCIAL_TRANSACTION
                        )
                        .where(
                                FINANCIAL_TRANSACTION.REFERENCE_ID.eq(
                                        referenceId
                                )
                        )
                        .fetchOne();


        assertNotNull(
                financialTransaction
        );


        assertEquals(
                "PURCHASE",
                financialTransaction.getType()
        );


        assertEquals(
                "COMPLETED",
                financialTransaction.getStatus()
        );


        assertEquals(
                30_000L,
                financialTransaction.getAmountUnits()
        );


        assertEquals(
                3L,
                financialTransaction.getVersion()
        );


        Long transactionDbId =
                financialTransaction.getId();

        var ledgerTransaction =
                dsl.selectFrom(
                                LEDGER_TRANSACTION
                        )

                        .where(
                                LEDGER_TRANSACTION.TRANSACTION_ID.eq(
                                        transactionDbId
                                )
                        )

                        .fetchOne();


        assertNotNull(
                ledgerTransaction
        );

        var ledgerEntries =
                dsl.selectFrom(
                                LEDGER_ENTRY
                        )

                        .where(
                                LEDGER_ENTRY.LEDGER_TRANSACTION_ID.eq(
                                        ledgerTransaction.getId()
                                )
                        )

                        .fetch();


        assertEquals(
                2,
                ledgerEntries.size()
        );


        long debit =
                ledgerEntries.stream()

                        .filter(
                                entry ->
                                        "DEBIT".equals(
                                                entry.getEntryType()
                                        )
                        )

                        .mapToLong(
                                LedgerEntryRecord::getAmountUnits
                        )

                        .sum();

        long credit =
                ledgerEntries.stream()

                        .filter(
                                entry ->
                                        "CREDIT".equals(
                                                entry.getEntryType()
                                        )
                        )

                        .mapToLong(
                                LedgerEntryRecord::getAmountUnits
                        )

                        .sum();

        assertEquals(
                30_000L,
                debit
        );

        assertEquals(
                30_000L,
                credit
        );

        assertEquals(
                debit,
                credit
        );


        var outboxEvent =
                dsl.selectFrom(
                                OUTBOX_EVENT
                        )

                        .where(
                                OUTBOX_EVENT.AGGREGATE_ID.eq(
                                        transactionDbId
                                )
                        )

                        .fetchOne();


        assertNotNull(
                outboxEvent
        );


        assertEquals(
                "PURCHASE_COMPLETED",
                outboxEvent.getEventType()
        );


        assertEquals(
                false,
                outboxEvent.getPublished()
        );
    }
}