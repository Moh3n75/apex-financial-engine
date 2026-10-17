package com.apex.customer.modules.customer.domain;

import java.util.Optional;
import java.util.UUID;

public interface CustomerRepository {

    Customer save(Customer customer);

    Optional<Customer> findById(UUID id);

    long countByExternalReference(String externalReference);
}
