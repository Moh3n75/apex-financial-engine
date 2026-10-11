package com.apex.credit.application.purchase;

import com.apex.credit.application.exception.DuplicateRequestException;
import com.apex.credit.application.port.out.*;


import com.apex.credit.application.purchase.event.PurchaseCompletedIntegrationEvent;
import com.apex.credit.application.purchase.event.PurchaseEvents;
import com.apex.credit.domain.ledger.LedgerEntry;
import com.apex.credit.domain.ledger.LedgerEntryType;
import com.apex.credit.domain.ledger.LedgerTransaction;
import com.apex.credit.domain.transaction.model.FinancialTransaction;
import com.apex.credit.domain.transaction.valueobject.TransactionType;
import com.apex.platform.core.context.ExecutionContext;
import com.apex.platform.events.EventEnvelope;
import com.apex.platform.events.EventEnvelopeFactory;

public final class PurchaseApplicationService {

    private static final TransactionType PURCHASE =
            new TransactionType("PURCHASE");

    private final CreditAccountRepository
            creditAccountRepository;

    private final FinancialTransactionRepository
            transactionRepository;

    private final UnitOfWork
            unitOfWork;

    private final LedgerRepository ledgerRepository;

    private final OutboxRepository outboxRepository;

    private final EventEnvelopeFactory eventEnvelopeFactory;


    public PurchaseApplicationService(
            CreditAccountRepository creditAccountRepository,
            FinancialTransactionRepository transactionRepository,
            LedgerRepository ledgerRepository,
            OutboxRepository outboxRepository,
            UnitOfWork unitOfWork,
            EventEnvelopeFactory eventEnvelopeFactory
    ) {

        this.creditAccountRepository =
                creditAccountRepository;

        this.transactionRepository =
                transactionRepository;

        this.ledgerRepository =
                ledgerRepository;

        this.outboxRepository =
                outboxRepository;

        this.unitOfWork =
                unitOfWork;

        this.eventEnvelopeFactory =
                eventEnvelopeFactory;
    }


    public PurchaseResult execute(
            PurchaseCommand command
    ) {

        /*
         * Fast path for sequential duplicate requests
         */
        var existing =
                transactionRepository
                        .findByReferenceId(
                                command.referenceId()
                        );


        if (existing.isPresent()) {

            var transaction =
                    existing.get();


            return new PurchaseResult(

                    transaction.getId(),

                    transaction.getStatus()
            );
        }


        ExecutionContext executionContext =
                ExecutionContext.root(
                        "apex-financial-engine",
                        "local-cell-1"
                );

        try {

            /*
             * Real financial transaction
             */
            return unitOfWork.execute(

                    () ->
                            executeInsideTransaction(
                                    command,
                                    executionContext
                            )
            );

        }
        catch (DuplicateRequestException exception) {

            /*
             * Concurrent duplicate request.
             *
             * Transaction قبلی Rollback شده و اکنون
             * رکورد Request برنده را می‌خوانیم.
             */
            var transaction =
                    transactionRepository
                            .findByReferenceId(
                                    command.referenceId()
                            )
                            .orElseThrow(
                                    () -> exception
                            );


            return new PurchaseResult(

                    transaction.getId(),

                    transaction.getStatus()
            );
        }
    }


    private PurchaseResult executeInsideTransaction(
            PurchaseCommand command,
            ExecutionContext executionContext
    ) {

        /*
         * 1. Create financial transaction
         *
         * مهم:
         * دیگر duplicate check را اینجا انجام نمی‌دهیم.
         * کنترل اولیه در execute() انجام می‌شود و
         * کنترل نهایی concurrency توسط UNIQUE constraint دیتابیس.
         */
        var transaction =
                FinancialTransaction.create(

                        new TransactionType(
                                "PURCHASE"
                        ),

                        command.amount(),

                        command.referenceId()
                );


        /*
         * CREATED -> PROCESSING
         */
        transaction.start();


        /*
         * 2. Idempotency Gate
         *
         * تراکنش را قبل از تغییر Balance ذخیره می‌کنیم.
         *
         * اگر دو Request همزمان با referenceId یکسان برسند،
         * UNIQUE constraint دیتابیس اجازه Insert دوم را نمی‌دهد.
         */
        transactionRepository.save(
                transaction
        );


        /*
         * 3. Load source account
         */
        var source =
                creditAccountRepository
                        .findById(
                                command.sourceAccountId()
                        )
                        .orElseThrow(
                                () -> new IllegalStateException(
                                        "Source credit account not found"
                                )
                        );


        /*
         * 4. Load destination account
         */
        var destination =
                creditAccountRepository
                        .findById(
                                command.destinationAccountId()
                        )
                        .orElseThrow(
                                () -> new IllegalStateException(
                                        "Destination credit account not found"
                                )
                        );


        /*
         * Internal correlation reference
         */
        String transactionReference =
                transaction
                        .getId()
                        .value()
                        .toString();


        /*
         * 5. Debit source
         */
        source.debit(
                command.amount(),
                transactionReference
        );


        /*
         * 6. Credit destination
         */
        destination.addCredit(
                command.amount(),
                transactionReference
        );


        /*
         * 7. Persist account states
         */
        creditAccountRepository.save(
                source
        );


        creditAccountRepository.save(
                destination
        );


        /*
         * 8. Create balanced ledger
         */
        var ledgerTransaction =
                LedgerTransaction.create(

                        transaction.getId(),

                        java.util.List.of(

                                new LedgerEntry(

                                        command.sourceAccountId(),

                                        LedgerEntryType.DEBIT,

                                        command.amount()
                                ),

                                new LedgerEntry(

                                        command.destinationAccountId(),

                                        LedgerEntryType.CREDIT,

                                        command.amount()
                                )
                        )
                );


        /*
         * Ledger is immutable:
         * append only
         */
        ledgerRepository.append(
                ledgerTransaction
        );


        /*
         * 9. Financial operation successfully completed
         *
         * PROCESSING -> COMPLETED
         */
        transaction.complete();


        /*
         * Update financial_transaction
         *
         * قبلاً PROCESSING با version=2 ذخیره شده.
         * حالا COMPLETED با version=3 Update می‌شود.
         */
        transactionRepository.save(
                transaction
        );


        /*
         * 10. Build integration event payload
         */
        PurchaseCompletedIntegrationEvent event =
                new PurchaseCompletedIntegrationEvent(
                        transaction.getId().value(),
                        transaction.getReferenceId(),
                        transaction.getStatus().name(),
                        transaction.getRequestedAmount().units()
                );

        EventEnvelope<PurchaseCompletedIntegrationEvent> envelope =
                eventEnvelopeFactory.create(
                        PurchaseEvents.PURCHASE_COMPLETED,
                        PurchaseEvents.PURCHASE_COMPLETED_VERSION,
                        transaction.getId().value().toString(),
                        "FINANCIAL_TRANSACTION",
                        executionContext,
                        event
                );

        outboxRepository.append(envelope);
        /*
         * 12. Return application result
         */
        return new PurchaseResult(

                transaction.getId(),

                transaction.getStatus()
        );
    }
}