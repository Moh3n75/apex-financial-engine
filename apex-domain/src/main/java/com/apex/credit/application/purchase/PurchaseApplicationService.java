package com.apex.credit.application.purchase;

import com.apex.credit.application.port.out.CreditAccountRepository;
import com.apex.credit.application.port.out.FinancialTransactionRepository;


import com.apex.credit.domain.model.CreditAccount;
import com.apex.credit.domain.transaction.model.FinancialTransaction;
import com.apex.credit.domain.transaction.valueobject.TransactionType;

public final class PurchaseApplicationService {

    private static final TransactionType PURCHASE =
            new TransactionType("PURCHASE");

    private final CreditAccountRepository
            creditAccountRepository;

    private final FinancialTransactionRepository
            transactionRepository;


    public PurchaseApplicationService(
            CreditAccountRepository creditAccountRepository,
            FinancialTransactionRepository transactionRepository
    ) {

        this.creditAccountRepository =
                creditAccountRepository;

        this.transactionRepository =
                transactionRepository;
    }


    public PurchaseResult execute(
            PurchaseCommand command
    ) {

        /*
         * 1 — Idempotency guard
         */
        var existing =
                transactionRepository.findByReferenceId(
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


        /*
         * 2 — Load accounts
         */
        CreditAccount source =
                creditAccountRepository
                        .findById(
                                command.sourceAccountId()
                        )
                        .orElseThrow(
                                () ->
                                        new IllegalStateException(
                                                "Source credit account not found"
                                        )
                        );


        CreditAccount destination =
                creditAccountRepository
                        .findById(
                                command.destinationAccountId()
                        )
                        .orElseThrow(
                                () ->
                                        new IllegalStateException(
                                                "Destination credit account not found"
                                        )
                        );


        /*
         * 3 — Create financial transaction
         */
        FinancialTransaction transaction =
                FinancialTransaction.create(
                        PURCHASE,
                        command.amount(),
                        command.referenceId()
                );


        transaction.start();


        /*
         * Internal correlation reference.
         */
        String transactionReference =
                transaction
                        .getId()
                        .value()
                        .toString();


        /*
         * 4 — Debit source
         */
        source.debit(
                command.amount(),
                transactionReference
        );


        /*
         * 5 — Credit destination
         */
        destination.addCredit(
                command.amount(),
                transactionReference
        );


        /*
         * 6 — Complete workflow
         */
        transaction.complete();


        /*
         * 7 — Persistence ports
         *
         * مرحله بعد همه اینها داخل یک DB transaction
         * انجام خواهند شد.
         */
        creditAccountRepository.save(
                source
        );

        creditAccountRepository.save(
                destination
        );

        transactionRepository.save(
                transaction
        );


        return new PurchaseResult(
                transaction.getId(),
                transaction.getStatus()
        );
    }
}