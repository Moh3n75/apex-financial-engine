package com.apex.credit.application.purchase;

import com.apex.credit.domain.transaction.model.TransactionStatus;
import com.apex.credit.domain.transaction.valueobject.TransactionId;

public record PurchaseResult(

        TransactionId transactionId,

        TransactionStatus status

) {
}