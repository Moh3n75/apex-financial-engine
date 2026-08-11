package com.sadeghi.accounting.transaction;

import com.sadeghi.accounting.transaction.controller.TransactionModel;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
@RequiredArgsConstructor
public class TransactionService {

    private final AccountRepository accountRepository;

    @Transactional(rollbackFor = Exception.class)
    public TransactionModel create(TransactionModel transactionModel) {
        Account source = accountRepository.findById(transactionModel.sourceAccount()).orElseThrow();
        Account destination = accountRepository.findById(transactionModel.sourceAccount()).orElseThrow();



        return transactionModel;
    }
}
