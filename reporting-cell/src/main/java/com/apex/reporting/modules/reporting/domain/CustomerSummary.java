package com.apex.reporting.modules.reporting.domain;

import java.time.Instant;
import java.util.UUID;

public record CustomerSummary(
        UUID customerId,
        String externalReference,
        String displayName,
        String status,
        Instant createdAt,
        Instant projectedAt
) {
}
