package com.sadeghi.accounting.transaction.controller;

import com.sadeghi.accounting.transaction.TransactionService;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/v1/transactions")
public record TransactionController(TransactionService transactionService) {


    @PostMapping
    public TransactionModel create(@RequestBody TransactionModel transactionModel) {
        return transactionService.create(transactionModel);
    }
}
