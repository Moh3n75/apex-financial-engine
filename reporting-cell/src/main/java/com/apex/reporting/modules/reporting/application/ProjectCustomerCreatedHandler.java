package com.apex.reporting.modules.reporting.application;

import java.time.Clock;

import org.springframework.stereotype.Component;

import com.apex.platform.events.EventEnvelope;
import com.apex.platform.messaging.consumer.ConsumerContext;
import com.apex.platform.messaging.consumer.EventHandler;
import com.apex.reporting.modules.reporting.domain.CustomerSummary;
import com.apex.reporting.modules.reporting.domain.CustomerSummaryRepository;
import com.apex.reporting.modules.reporting.events.CustomerCreatedV1Payload;

@Component
public class ProjectCustomerCreatedHandler implements EventHandler<CustomerCreatedV1Payload> {

    private final CustomerSummaryRepository customerSummaryRepository;
    private final Clock clock;

    public ProjectCustomerCreatedHandler(
            CustomerSummaryRepository customerSummaryRepository,
            Clock clock
    ) {
        this.customerSummaryRepository = customerSummaryRepository;
        this.clock = clock;
    }

    @Override
    public String eventType() {
        return CustomerCreatedV1Payload.EVENT_TYPE;
    }

    @Override
    public int eventVersion() {
        return CustomerCreatedV1Payload.EVENT_VERSION;
    }

    @Override
    public Class<CustomerCreatedV1Payload> payloadType() {
        return CustomerCreatedV1Payload.class;
    }

    @Override
    public void handle(EventEnvelope<CustomerCreatedV1Payload> event, ConsumerContext context) {
        CustomerCreatedV1Payload payload = event.payload();
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
