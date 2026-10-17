package com.apex.reporting.modules.reporting.events;

import java.time.Instant;
import java.util.UUID;

/**
 * Reporting-owned view of CUSTOMER_CREATED v1. Does not import Customer Cell types.
 */
public record CustomerCreatedV1Payload(
        UUID customerId,
        String externalReference,
        String displayName,
        String status,
        Instant createdAt
) {

    public static final String EVENT_TYPE = "CUSTOMER_CREATED";
    public static final int EVENT_VERSION = 1;
}
