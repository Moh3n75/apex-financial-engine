package com.sadeghi.accounting.transaction.controller;

import java.math.BigDecimal;

public record TransactionModel(String transactionId, Integer sourceAccount, Integer destinationAccount,
                               BigDecimal amount) {
}
