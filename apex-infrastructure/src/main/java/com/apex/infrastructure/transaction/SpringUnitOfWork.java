package com.apex.infrastructure.transaction;

import com.apex.credit.application.port.out.UnitOfWork;

import org.springframework.stereotype.Component;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

import java.util.function.Supplier;

@Component
public final class SpringUnitOfWork
        implements UnitOfWork {

    private final TransactionTemplate
            transactionTemplate;


    public SpringUnitOfWork(
            PlatformTransactionManager transactionManager
    ) {

        this.transactionTemplate =
                new TransactionTemplate(
                        transactionManager
                );
    }


    @Override
    public <T> T execute(
            Supplier<T> operation
    ) {

        return transactionTemplate.execute(
                status -> operation.get()
        );
    }
}