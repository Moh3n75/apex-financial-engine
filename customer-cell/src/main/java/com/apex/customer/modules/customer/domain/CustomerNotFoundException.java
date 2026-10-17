package com.apex.customer.modules.customer.domain;

import java.util.UUID;

/**
 * Stable translation key — not localized text.
 */
public class CustomerNotFoundException extends RuntimeException {

    public static final String ERROR_CODE = "customer.error.notFound";

    public CustomerNotFoundException(UUID id) {
        super(ERROR_CODE + ": " + id);
    }

    public String errorCode() {
        return ERROR_CODE;
    }
}
