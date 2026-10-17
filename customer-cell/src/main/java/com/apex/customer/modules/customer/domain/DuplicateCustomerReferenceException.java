package com.apex.customer.modules.customer.domain;

/**
 * Stable translation key — not localized text.
 */
public class DuplicateCustomerReferenceException extends RuntimeException {

    public static final String ERROR_CODE = "customer.error.duplicateReference";

    public DuplicateCustomerReferenceException(String externalReference) {
        super(ERROR_CODE + ": " + externalReference);
    }

    public String errorCode() {
        return ERROR_CODE;
    }
}
