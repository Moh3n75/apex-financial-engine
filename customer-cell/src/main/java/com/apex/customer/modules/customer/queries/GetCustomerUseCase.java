package com.apex.customer.modules.customer.queries;

import java.util.UUID;

import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import com.apex.customer.modules.customer.domain.Customer;
import com.apex.customer.modules.customer.domain.CustomerNotFoundException;
import com.apex.customer.modules.customer.domain.CustomerRepository;

@Service
public class GetCustomerUseCase {

    private final CustomerRepository customerRepository;

    public GetCustomerUseCase(CustomerRepository customerRepository) {
        this.customerRepository = customerRepository;
    }

    @Transactional(readOnly = true)
    public Customer execute(UUID id) {
        return customerRepository.findById(id)
                .orElseThrow(() -> new CustomerNotFoundException(id));
    }
}
