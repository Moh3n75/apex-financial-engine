
package com.apex.reporting.modules.reporting.application;

import java.time.Clock;

import org.springframework.stereotype.Component;

import com.apex.platform.messaging.consumer.OnCellEvent;
import com.apex.reporting.modules.reporting.domain.CustomerSummary;
import com.apex.reporting.modules.reporting.domain.CustomerSummaryRepository;
import com.apex.reporting.modules.reporting.events.CustomerCreatedV1Payload;

@Component
public class ProjectCustomerCreatedHandler {

    private final CustomerSummaryRepository customerSummaryRepository;
    private final Clock clock;

    public ProjectCustomerCreatedHandler(
            CustomerSummaryRepository customerSummaryRepository,
            Clock clock
    ) {
        this.customerSummaryRepository = customerSummaryRepository;
        this.clock = clock;
    }

    @OnCellEvent(
            type = CustomerCreatedV1Payload.EVENT_TYPE,
            version = CustomerCreatedV1Payload.EVENT_VERSION
    )
    public void handle(CustomerCreatedV1Payload payload) {

        customerSummaryRepository.upsert(new CustomerSummary(
                payload.customerId(),
                payload.externalReference(),
                payload.displayName(),
                payload.status(),
                payload.createdAt(),
                clock.instant()
        ));
    }
}
