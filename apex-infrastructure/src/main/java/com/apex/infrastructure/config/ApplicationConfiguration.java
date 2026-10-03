package com.apex.infrastructure.config;

import com.apex.credit.application.port.out.*;
import com.apex.credit.application.purchase.PurchaseApplicationService;

import com.apex.platform.events.EventEnvelopeFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.time.Clock;


@Configuration
public class ApplicationConfiguration {


    @Bean
    public EventEnvelopeFactory eventEnvelopeFactory() {
        return new EventEnvelopeFactory(
                Clock.systemUTC()
        );
    }

    @Bean
    public PurchaseApplicationService
    purchaseApplicationService(

            CreditAccountRepository creditAccountRepository,

            FinancialTransactionRepository transactionRepository,

            LedgerRepository ledgerRepository,

            OutboxRepository outboxRepository,

            UnitOfWork unitOfWork,

            EventEnvelopeFactory eventEnvelopeFactory

    ) {

        return new PurchaseApplicationService(

                creditAccountRepository,

                transactionRepository,

                ledgerRepository,

                outboxRepository,

                unitOfWork,

                eventEnvelopeFactory

        );
    }
}