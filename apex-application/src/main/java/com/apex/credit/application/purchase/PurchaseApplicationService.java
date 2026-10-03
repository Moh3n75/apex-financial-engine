package com.apex.credit.application.purchase;

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

        return unitOfWork.execute(
                () -> executeInsideTransaction(command)
        );
    }


    private PurchaseResult executeInsideTransaction(
            PurchaseCommand command
    ) {

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


        var transaction =
                FinancialTransaction.create(

                        new TransactionType(
                                "PURCHASE"
                        ),

                        command.amount(),

                        command.referenceId()

                );


        transaction.start();


        String transactionReference =
                transaction
                        .getId()
                        .value()
                        .toString();


        source.debit(
                command.amount(),
                transactionReference
        );


        destination.addCredit(
                command.amount(),
                transactionReference
        );


        transaction.complete();


        creditAccountRepository.save(
                source
        );


        creditAccountRepository.save(
                destination
        );


        transactionRepository.save(
                transaction
        );

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


        ledgerRepository.append(
                ledgerTransaction
        );


        String payload =
                """
                {
                  "transactionId":"%s",
                  "status":"COMPLETED",
                  "amountUnits":%d
                }
                """.formatted(

                        transaction
                                .getId()
                                .value(),

                        command
                                .amount()
                                .units()

                );


        outboxRepository.append(

                new OutboxMessage(

                        "PURCHASE_COMPLETED",

                        transaction.getId(),

                        payload

                )

        );

        return new PurchaseResult(
                transaction.getId(),
                transaction.getStatus()
        );
    }
}