package com.apex.credit.application.port.out;

import com.apex.credit.domain.transaction.model.FinancialTransaction;
import com.apex.credit.domain.transaction.valueobject.TransactionId;

import java.util.Optional;

public interface FinancialTransactionRepository {

    void save(
            FinancialTransaction transaction
    );

    Optional<FinancialTransaction> findById(
            TransactionId id
    );

    Optional<FinancialTransaction> findByReferenceId(
            String referenceId
    );
}