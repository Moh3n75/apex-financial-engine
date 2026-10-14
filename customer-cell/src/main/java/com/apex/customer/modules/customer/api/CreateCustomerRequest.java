package com.apex.customer.modules.customer.api;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Size;

public record CreateCustomerRequest(
        @NotBlank(message = "customer.validation.externalReference.required")
        @Size(max = 64, message = "customer.validation.externalReference.required")
        String externalReference,

        @NotBlank(message = "customer.validation.displayName.required")
        @Size(max = 150, message = "customer.validation.displayName.required")
        String displayName
) {
}
