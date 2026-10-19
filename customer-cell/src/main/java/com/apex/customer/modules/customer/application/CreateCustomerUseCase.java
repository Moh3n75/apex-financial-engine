
package com.apex.customer.modules.customer.application;

import java.time.Clock;

import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import com.apex.customer.modules.customer.domain.Customer;
import com.apex.customer.modules.customer.domain.CustomerRepository;
import com.apex.customer.modules.customer.events.CustomerContracts;
import com.apex.customer.modules.customer.events.CustomerCreatedIntegrationEvent;
import com.apex.platform.events.api.CellEvents;

@Service
public class CreateCustomerUseCase {

    private final CustomerRepository customerRepository;
    private final CellEvents cellEvents;
    private final Clock clock;

    public CreateCustomerUseCase(
            CustomerRepository customerRepository,
            CellEvents cellEvents,
            Clock clock
    ) {
        this.customerRepository = customerRepository;
        this.cellEvents = cellEvents;
        this.clock = clock;
    }

    @Transactional
    public Customer execute(
            String externalReference,
            String displayName
    ) {
        Customer customer = Customer.create(
                externalReference,
                displayName,
                clock.instant()
        );

        customerRepository.save(customer);

        CustomerCreatedIntegrationEvent payload =
                new CustomerCreatedIntegrationEvent(
                        customer.id(),
                        customer.externalReference(),
                        customer.displayName(),
                        customer.status().name(),
                        customer.createdAt()
                );

        cellEvents.append(
                CustomerContracts.CREATED_V1,
                customer.id(),
                payload
        );

        return customer;
    }
}
