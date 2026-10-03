package com.apex.infrastructure.persistence.jooq;

import com.apex.credit.application.port.out.LedgerRepository;

import com.apex.credit.domain.ledger.LedgerTransaction;

import org.jooq.DSLContext;

import org.springframework.stereotype.Repository;

import java.util.ArrayList;
import java.util.UUID;

import static com.apex.infrastructure.jooq.generated.Tables.CREDIT_ACCOUNT_STATE;
import static com.apex.infrastructure.jooq.generated.Tables.FINANCIAL_TRANSACTION;
import static com.apex.infrastructure.jooq.generated.Tables.LEDGER_ENTRY;
import static com.apex.infrastructure.jooq.generated.Tables.LEDGER_TRANSACTION;


@Repository
public class JooqLedgerRepository
        implements LedgerRepository {


    private final DSLContext dsl;


    public JooqLedgerRepository(
            DSLContext dsl
    ) {
        this.dsl = dsl;
    }


    @Override
    public void append(
            LedgerTransaction ledgerTransaction
    ) {

        /*
         * Domain uses UUID.
         * BIGINT is only persistence implementation detail.
         */

        Long financialTransactionDbId =
                dsl.select(
                                FINANCIAL_TRANSACTION.ID
                        )
                        .from(
                                FINANCIAL_TRANSACTION
                        )
                        .where(
                                FINANCIAL_TRANSACTION.PUBLIC_ID.eq(
                                        ledgerTransaction
                                                .financialTransactionId()
                                                .value()
                                )
                        )
                        .fetchOne(
                                FINANCIAL_TRANSACTION.ID
                        );


        if (financialTransactionDbId == null) {
            throw new IllegalStateException(
                    "Financial transaction not found for ledger"
            );
        }


        Long ledgerTransactionDbId =
                dsl.insertInto(
                                LEDGER_TRANSACTION
                        )

                        .set(
                                LEDGER_TRANSACTION.TRANSACTION_ID,
                                financialTransactionDbId
                        )

                        .returning(
                                LEDGER_TRANSACTION.ID
                        )

                        .fetchOne(
                                LEDGER_TRANSACTION.ID
                        );


        if (ledgerTransactionDbId == null) {
            throw new IllegalStateException(
                    "Could not create ledger transaction"
            );
        }


        var accountIds =
                ledgerTransaction.entries()
                        .stream()
                        .map(
                                entry ->
                                        entry.accountId().value()
                        )
                        .distinct()
                        .toList();


        var internalAccountIds =
                dsl.select(
                                CREDIT_ACCOUNT_STATE.PUBLIC_ID,
                                CREDIT_ACCOUNT_STATE.ID
                        )

                        .from(
                                CREDIT_ACCOUNT_STATE
                        )

                        .where(
                                CREDIT_ACCOUNT_STATE.PUBLIC_ID.in(
                                        accountIds
                                )
                        )

                        .fetchMap(
                                CREDIT_ACCOUNT_STATE.PUBLIC_ID,
                                CREDIT_ACCOUNT_STATE.ID
                        );


        var insertQueries =
                new ArrayList<org.jooq.Query>();


        for (var entry :
                ledgerTransaction.entries()) {

            UUID publicAccountId =
                    entry.accountId().value();


            Long accountDbId =
                    internalAccountIds.get(
                            publicAccountId
                    );


            if (accountDbId == null) {

                throw new IllegalStateException(
                        "Ledger account not found: "
                                + publicAccountId
                );
            }


            insertQueries.add(

                    dsl.insertInto(
                                    LEDGER_ENTRY
                            )

                            .set(
                                    LEDGER_ENTRY.LEDGER_TRANSACTION_ID,
                                    ledgerTransactionDbId
                            )

                            .set(
                                    LEDGER_ENTRY.ACCOUNT_ID,
                                    accountDbId
                            )

                            .set(
                                    LEDGER_ENTRY.ENTRY_TYPE,
                                    entry.type().name()
                            )

                            .set(
                                    LEDGER_ENTRY.AMOUNT_UNITS,
                                    entry.amount().units()
                            )

            );
        }


        dsl.batch(
                insertQueries
        ).execute();
    }
}