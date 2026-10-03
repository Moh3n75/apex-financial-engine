package com.apex.apexbootstrap.api.purchase;

import java.util.UUID;

public record PurchaseResponse(

        UUID transactionId,

        String status

) {
}