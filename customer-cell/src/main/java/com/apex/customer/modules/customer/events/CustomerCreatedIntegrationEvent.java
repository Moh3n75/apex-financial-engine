package com.apex.customer.modules.customer.events;

import java.time.Instant;
import java.util.UUID;

import com.apex.platform.events.IntegrationEvent;

/**
 * Public v1 cross-Cell contract. Not a Customer domain entity.
 */
public record CustomerCreatedIntegrationEvent(
        UUID customerId,
        String externalReference,
        String displayName,
        String status,
        Instant createdAt
) implements IntegrationEvent {

    public static final String EVENT_TYPE = "CUSTOMER_CREATED";
    public static final int EVENT_VERSION = 1;
    public static final String AGGREGATE_TYPE = "CUSTOMER";
}
