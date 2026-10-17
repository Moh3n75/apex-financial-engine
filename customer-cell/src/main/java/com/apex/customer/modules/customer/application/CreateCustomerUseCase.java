package com.apex.customer.modules.customer.application;

import java.time.Clock;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import com.apex.customer.modules.customer.domain.Customer;
import com.apex.customer.modules.customer.domain.CustomerRepository;
import com.apex.customer.modules.customer.events.CustomerCreatedIntegrationEvent;
import com.apex.platform.core.context.ExecutionContext;
import com.apex.platform.events.EventEnvelope;
import com.apex.platform.events.EventEnvelopeFactory;
import com.apex.platform.messaging.outbox.OutboxAppender;

@Service
public class CreateCustomerUseCase {

    private final CustomerRepository customerRepository;
    private final OutboxAppender outboxAppender;
    private final EventEnvelopeFactory eventEnvelopeFactory;
    private final Clock clock;
    private final String sourceService;
    private final String cellId;

    public CreateCustomerUseCase(
            CustomerRepository customerRepository,
            OutboxAppender outboxAppender,
            EventEnvelopeFactory eventEnvelopeFactory,
            Clock clock,
            @Value("${spring.application.name}") String sourceService,
            @Value("${apex.cell.id}") String cellId
    ) {
        this.customerRepository = customerRepository;
        this.outboxAppender = outboxAppender;
        this.eventEnvelopeFactory = eventEnvelopeFactory;
        this.clock = clock;
        this.sourceService = sourceService;
        this.cellId = cellId;
    }

    @Transactional
    public Customer execute(String externalReference, String displayName) {
        Customer customer = Customer.create(externalReference, displayName, clock.instant());
        customerRepository.save(customer);

        CustomerCreatedIntegrationEvent payload = new CustomerCreatedIntegrationEvent(
                customer.id(),
                customer.externalReference(),
                customer.displayName(),
                customer.status().name(),
                customer.createdAt()
        );

        ExecutionContext context = ExecutionContext.root(sourceService, cellId);
        EventEnvelope<CustomerCreatedIntegrationEvent> envelope = eventEnvelopeFactory.create(
                CustomerCreatedIntegrationEvent.EVENT_TYPE,
                CustomerCreatedIntegrationEvent.EVENT_VERSION,
                customer.id().toString(),
                CustomerCreatedIntegrationEvent.AGGREGATE_TYPE,
                context,
                payload
        );

        outboxAppender.append(envelope);
        return customer;
    }
}
