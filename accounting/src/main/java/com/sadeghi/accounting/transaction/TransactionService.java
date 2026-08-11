package com.sadeghi.accounting.transaction;

import com.sadeghi.accounting.transaction.controller.TransactionModel;
import com.sadeghi.accounting.transaction.orm.Account;
import com.sadeghi.accounting.transaction.orm.AccountRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.math.BigDecimal;

@Service
@RequiredArgsConstructor
public class TransactionService {

    private final AccountRepository accountRepository;

    @Transactional(rollbackFor = Exception.class)
    public TransactionModel create(TransactionModel transactionModel) {
        Account source = accountRepository.findById(transactionModel.sourceAccount()).orElseThrow();
        Account destination = accountRepository.findById(transactionModel.destinationAccount()).orElseThrow();

        BigDecimal multiply = transactionModel.amount().multiply(new BigDecimal("-1"));
        source.setAvailableAmount(source.getAvailableAmount().add(multiply));
        source.setTotalAmount(source.getTotalAmount().add(multiply));

        accountRepository.save(source);

        destination.setAvailableAmount(destination.getAvailableAmount().add(transactionModel.amount()));
        destination.setTotalAmount(destination.getTotalAmount().add(transactionModel.amount()));

        accountRepository.save(destination);

        return transactionModel;
    }
}
