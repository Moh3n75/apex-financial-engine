package com.apex.infrastructure.persistence.jooq;

import com.apex.credit.application.exception.DuplicateRequestException;
import com.apex.credit.application.port.out.FinancialTransactionRepository;

import com.apex.credit.domain.transaction.model.FinancialTransaction;
import com.apex.credit.domain.transaction.model.TransactionStatus;

import com.apex.credit.domain.transaction.valueobject.TransactionId;
import com.apex.credit.domain.transaction.valueobject.TransactionType;

import com.apex.credit.domain.valueobject.CreditAmount;

import org.jooq.DSLContext;

import org.springframework.dao.DuplicateKeyException;
import org.springframework.stereotype.Repository;

import java.time.LocalDateTime;
import java.util.Optional;

import static com.apex.infrastructure.jooq.generated.Tables.FINANCIAL_TRANSACTION;


@Repository
public class JooqFinancialTransactionRepository implements FinancialTransactionRepository {


    private final DSLContext dsl;


    public JooqFinancialTransactionRepository(
            DSLContext dsl
    ) {

        this.dsl = dsl;
    }


    @Override
    public Optional<FinancialTransaction> findById(
            TransactionId id
    ) {

        return dsl
                .selectFrom(
                        FINANCIAL_TRANSACTION
                )
                .where(
                        FINANCIAL_TRANSACTION.PUBLIC_ID.eq(
                                id.value()
                        )
                )
                .fetchOptional(
                        this::restore
                );
    }


    @Override
    public Optional<FinancialTransaction>
    findByReferenceId(
            String referenceId
    ) {

        return dsl
                .selectFrom(
                        FINANCIAL_TRANSACTION
                )
                .where(
                        FINANCIAL_TRANSACTION
                                .REFERENCE_ID
                                .eq(referenceId)
                )
                .fetchOptional(
                        this::restore
                );
    }


    @Override
    public void save(
            FinancialTransaction transaction
    ) {

        if (
                transaction.persistedVersion()
                        == 0
        ) {

            insert(transaction);

        } else {

            update(transaction);
        }


        transaction.markChangesAsCommitted();
    }


    private void insert(
            FinancialTransaction transaction
    ) {

        try {

            dsl.insertInto(
                            FINANCIAL_TRANSACTION
                    )

                    .set(
                            FINANCIAL_TRANSACTION.PUBLIC_ID,
                            transaction.getId().value()
                    )

                    .set(
                            FINANCIAL_TRANSACTION.TYPE,
                            transaction.getType().value()
                    )

                    .set(
                            FINANCIAL_TRANSACTION.STATUS,
                            transaction.getStatus().name()
                    )

                    .set(
                            FINANCIAL_TRANSACTION.AMOUNT_UNITS,
                            transaction
                                    .getRequestedAmount()
                                    .units()
                    )

                    .set(
                            FINANCIAL_TRANSACTION.REFERENCE_ID,
                            transaction.getReferenceId()
                    )

                    .set(
                            FINANCIAL_TRANSACTION.FAILURE_REASON,
                            transaction.getFailureReason()
                    )

                    .set(
                            FINANCIAL_TRANSACTION.VERSION,
                            transaction.getVersion()
                    )

                    .execute();

        } catch (DuplicateKeyException exception) {

            throw new DuplicateRequestException(
                    transaction.getReferenceId()
            );
        }
    }
    private void update(
            FinancialTransaction transaction
    ) {

        long expectedVersion =
                transaction.persistedVersion();


        int updated =
                dsl
                        .update(
                                FINANCIAL_TRANSACTION
                        )

                        .set(
                                FINANCIAL_TRANSACTION.STATUS,
                                transaction
                                        .getStatus()
                                        .name()
                        )

                        .set(
                                FINANCIAL_TRANSACTION.FAILURE_REASON,
                                transaction.getFailureReason()
                        )

                        .set(
                                FINANCIAL_TRANSACTION.VERSION,
                                transaction.getVersion()
                        )

                        .set(
                                FINANCIAL_TRANSACTION.UPDATED_AT,
                                LocalDateTime.now()
                        )

                        .where(
                                FINANCIAL_TRANSACTION.PUBLIC_ID.eq(
                                        transaction
                                                .getId()
                                                .value()
                                )
                        )

                        .and(
                                FINANCIAL_TRANSACTION.VERSION.eq(
                                        expectedVersion
                                )
                        )

                        .execute();


        if (updated != 1) {

            throw new IllegalStateException(
                    "Transaction concurrent modification detected"
            );
        }
    }


    private FinancialTransaction restore(
            com.apex.infrastructure.jooq.generated.tables.records.FinancialTransactionRecord record
    ) {

        return FinancialTransaction.restore(

                new TransactionId(
                        record.getPublicId()
                ),

                new TransactionType(
                        record.getType()
                ),

                new CreditAmount(
                        record.getAmountUnits()
                ),

                record.getReferenceId(),

                TransactionStatus.valueOf(
                        record.getStatus()
                ),

                record.getFailureReason(),

                record.getVersion()

        );
    }
}