package com.sadeghi.accounting.transaction;

import com.sadeghi.accounting.transaction.controller.TransactionModel;
import com.sadeghi.accounting.transaction.orm.AccountRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
@RequiredArgsConstructor
public class TransactionService {

    private final AccountRepository accountRepository;

    @Transactional(rollbackFor = Exception.class)
    public TransactionModel create(TransactionModel transactionModel) {

        /*
        First code without Lock / JPA Read-Modify-Write
        Account source = accountRepository.findById(transactionModel.sourceAccount()).orElseThrow();
        Account destination = accountRepository.findById(transactionModel.destinationAccount()).orElseThrow();*/

        //**We don't use the Optimistic lock because we need to implement the retry model


        /*
        Second use the Pessimistic lock
        Account source = accountRepository.findByIdForUpdate(transactionModel.sourceAccount()).orElseThrow();
        Account destination = accountRepository.findByIdForUpdate(transactionModel.destinationAccount()).orElseThrow();

        source.setAvailableAmount(source.getAvailableAmount().add(transactionModel.amount().negate()));
        source.setTotalAmount(source.getTotalAmount().add(transactionModel.amount().negate()));

        accountRepository.save(source);

        destination.setAvailableAmount(destination.getAvailableAmount().add(transactionModel.amount()));
        destination.setTotalAmount(destination.getTotalAmount().add(transactionModel.amount()));

        accountRepository.save(destination);*/


        //Third use the Query native
        accountRepository.decreaseBalance(transactionModel.sourceAccount(),transactionModel.amount());
        accountRepository.increaseBalance(transactionModel.destinationAccount(),transactionModel.amount());

        return transactionModel;
    }
}
