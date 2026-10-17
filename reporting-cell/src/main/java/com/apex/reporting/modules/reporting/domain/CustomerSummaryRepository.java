package com.apex.reporting.modules.reporting.domain;

import java.util.Optional;
import java.util.UUID;

public interface CustomerSummaryRepository {

    void upsert(CustomerSummary summary);

    Optional<CustomerSummary> findByCustomerId(UUID customerId);

    long count();
}
