package com.apex.credit.application.purchase;

import com.apex.credit.application.exception.DuplicateRequestException;
import com.apex.credit.application.outbox.OutboxMessage;
import com.apex.credit.application.port.out.*;


import com.apex.credit.domain.ledger.LedgerEntry;
import com.apex.credit.domain.ledger.LedgerEntryType;
import com.apex.credit.domain.ledger.LedgerTransaction;
import com.apex.credit.domain.transaction.model.FinancialTransaction;
import com.apex.credit.domain.transaction.valueobject.TransactionType;

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


    public PurchaseApplicationService(
            CreditAccountRepository creditAccountRepository,
            FinancialTransactionRepository transactionRepository,
            LedgerRepository ledgerRepository,
            OutboxRepository outboxRepository,
            UnitOfWork unitOfWork
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


        try {

            /*
             * Real financial transaction
             */
            return unitOfWork.execute(

                    () ->
                            executeInsideTransaction(
                                    command
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
            PurchaseCommand command
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
        String payload =
                """
                {
                  "transactionId":"%s",
                  "referenceId":"%s",
                  "status":"COMPLETED",
                  "amountUnits":%d
                }
                """.formatted(

                        transaction
                                .getId()
                                .value(),

                        command.referenceId(),

                        command
                                .amount()
                                .units()
                );


        /*
         * 11. Transactional Outbox
         *
         * این Event هنوز Kafka Publish نشده.
         * فقط در همان DB Transaction ذخیره می‌شود.
         */
        outboxRepository.append(

                new OutboxMessage(

                        "PURCHASE_COMPLETED",

                        transaction.getId(),

                        payload
                )
        );


        /*
         * 12. Return application result
         */
        return new PurchaseResult(

                transaction.getId(),

                transaction.getStatus()
        );
    }
}