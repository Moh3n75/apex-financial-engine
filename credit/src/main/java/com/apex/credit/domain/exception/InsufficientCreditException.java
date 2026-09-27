package com.apex.credit.domain.exception;

public class InsufficientCreditException
        extends RuntimeException {


    public InsufficientCreditException(
            String message
    ) {
        super(message);
    }

}