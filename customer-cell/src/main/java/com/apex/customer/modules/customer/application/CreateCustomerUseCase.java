package com.apex.customer.modules.customer.application;

import java.time.Clock;

import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import com.apex.customer.modules.customer.domain.Customer;
import com.apex.customer.modules.customer.domain.CustomerRepository;

@Service
public class CreateCustomerUseCase {

    private final CustomerRepository customerRepository;
    private final Clock clock;

    public CreateCustomerUseCase(CustomerRepository customerRepository, Clock clock) {
        this.customerRepository = customerRepository;
        this.clock = clock;
    }

    @Transactional
    public Customer execute(String externalReference, String displayName) {
        Customer customer = Customer.create(externalReference, displayName, clock.instant());
        return customerRepository.save(customer);
    }
}
