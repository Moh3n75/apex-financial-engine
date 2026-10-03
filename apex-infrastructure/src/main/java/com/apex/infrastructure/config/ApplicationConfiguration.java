package com.apex.infrastructure.config;

import com.apex.credit.application.port.out.*;
import com.apex.credit.application.purchase.PurchaseApplicationService;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;


@Configuration
public class ApplicationConfiguration {


    @Bean
    public PurchaseApplicationService
    purchaseApplicationService(

            CreditAccountRepository creditAccountRepository,

            FinancialTransactionRepository transactionRepository,

            LedgerRepository ledgerRepository,

            OutboxRepository outboxRepository,

            UnitOfWork unitOfWork

    ) {

        return new PurchaseApplicationService(

                creditAccountRepository,

                transactionRepository,

                ledgerRepository,

                outboxRepository,

                unitOfWork

        );
    }
}