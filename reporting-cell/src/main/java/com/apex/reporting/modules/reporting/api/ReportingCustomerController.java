package com.apex.reporting.modules.reporting.api;

import java.util.Map;
import java.util.UUID;

import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ResponseStatusException;

import com.apex.reporting.modules.reporting.domain.CustomerSummaryRepository;

@RestController
@RequestMapping("/api/v1/reports/customers")
public class ReportingCustomerController {

    private final CustomerSummaryRepository customerSummaryRepository;

    public ReportingCustomerController(CustomerSummaryRepository customerSummaryRepository) {
        this.customerSummaryRepository = customerSummaryRepository;
    }

    @GetMapping("/count")
    public Map<String, Long> count() {
        return Map.of("count", customerSummaryRepository.count());
    }

    @GetMapping("/{customerId}")
    public CustomerSummaryResponse get(@PathVariable UUID customerId) {
        return customerSummaryRepository.findByCustomerId(customerId)
                .map(CustomerSummaryResponse::from)
                .orElseThrow(() -> new ResponseStatusException(
                        HttpStatus.NOT_FOUND,
                        "reporting.customer.notFound"
                ));
    }
}
