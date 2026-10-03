package com.apex.credit.application.exception;

public class DuplicateRequestException
        extends RuntimeException {

    private final String referenceId;

    public DuplicateRequestException(
            String referenceId
    ) {

        super(
                "Duplicate request: " + referenceId
        );

        this.referenceId = referenceId;
    }

    public String referenceId() {
        return referenceId;
    }
}