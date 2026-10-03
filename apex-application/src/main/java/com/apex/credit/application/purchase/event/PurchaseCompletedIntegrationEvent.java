package com.apex.credit.application.purchase.event;

import com.apex.platform.events.IntegrationEvent;

import java.util.UUID;

public record PurchaseCompletedIntegrationEvent(
        UUID transactionId,
        String referenceId,
        String status,
        long amountUnits
) implements IntegrationEvent {
}