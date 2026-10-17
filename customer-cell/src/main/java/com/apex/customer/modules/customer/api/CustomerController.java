package com.apex.customer.modules.customer.api;

import java.net.URI;
import java.util.UUID;

import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ResponseStatusException;

import com.apex.customer.modules.customer.application.CreateCustomerUseCase;
import com.apex.customer.modules.customer.domain.Customer;
import com.apex.customer.modules.customer.domain.CustomerNotFoundException;
import com.apex.customer.modules.customer.domain.DuplicateCustomerReferenceException;
import com.apex.customer.modules.customer.queries.GetCustomerUseCase;

import jakarta.validation.Valid;

/**
 * HTTP boundary only. Maps domain error codes to HTTP status via Spring
 * {@link ResponseStatusException} — no Cell-local {@code @ControllerAdvice}
 * and no fabricated platform error JSON contract.
 */
@RestController
@RequestMapping("/api/v1/customers")
public class CustomerController {

    private final CreateCustomerUseCase createCustomerUseCase;
    private final GetCustomerUseCase getCustomerUseCase;

    public CustomerController(
            CreateCustomerUseCase createCustomerUseCase,
            GetCustomerUseCase getCustomerUseCase
    ) {
        this.createCustomerUseCase = createCustomerUseCase;
        this.getCustomerUseCase = getCustomerUseCase;
    }

    @PostMapping
    public ResponseEntity<CustomerResponse> create(@Valid @RequestBody CreateCustomerRequest request) {
        try {
            Customer created = createCustomerUseCase.execute(
                    request.externalReference(),
                    request.displayName()
            );
            CustomerResponse body = CustomerResponse.from(created);
            return ResponseEntity
                    .created(URI.create("/api/v1/customers/" + body.id()))
                    .body(body);
        } catch (DuplicateCustomerReferenceException ex) {
            throw new ResponseStatusException(HttpStatus.CONFLICT, ex.errorCode(), ex);
        } catch (IllegalArgumentException ex) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, ex.getMessage(), ex);
        }
    }

    @GetMapping("/{id}")
    public CustomerResponse get(@PathVariable UUID id) {
        try {
            return CustomerResponse.from(getCustomerUseCase.execute(id));
        } catch (CustomerNotFoundException ex) {
            throw new ResponseStatusException(HttpStatus.NOT_FOUND, ex.errorCode(), ex);
        }
    }
}
