package com.apex.customer.modules.customer.api;

import java.time.Instant;
import java.util.UUID;

import com.apex.customer.modules.customer.domain.Customer;

public record CustomerResponse(
        UUID id,
        String externalReference,
        String displayName,
        String status,
        Instant createdAt
) {

    public static CustomerResponse from(Customer customer) {
        return new CustomerResponse(
                customer.id(),
                customer.externalReference(),
                customer.displayName(),
                customer.status().name(),
                customer.createdAt()
        );
    }
}
