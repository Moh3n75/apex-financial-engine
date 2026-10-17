package com.apex.reporting.modules.reporting.api;

import java.time.Instant;
import java.util.UUID;

import com.apex.reporting.modules.reporting.domain.CustomerSummary;

public record CustomerSummaryResponse(
        UUID customerId,
        String externalReference,
        String displayName,
        String status,
        Instant createdAt,
        Instant projectedAt
) {

    public static CustomerSummaryResponse from(CustomerSummary summary) {
        return new CustomerSummaryResponse(
                summary.customerId(),
                summary.externalReference(),
                summary.displayName(),
                summary.status(),
                summary.createdAt(),
                summary.projectedAt()
        );
    }
}
