package com.sadeghi.accounting.transaction;

import com.sadeghi.accounting.transaction.controller.TransactionModel;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.bind.annotation.RequestBody;

@Service
public class TransactionService {



    @Transactional(rollbackFor = Exception.class)
    public TransactionModel create(TransactionModel transactionModel) {

    }
}
