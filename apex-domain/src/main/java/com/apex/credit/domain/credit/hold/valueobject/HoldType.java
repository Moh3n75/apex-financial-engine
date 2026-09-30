package com.apex.credit.domain.credit.hold.valueobject;


public record HoldType(
        String value
) {

    public HoldType {

        if(value == null || value.isBlank()) {
            throw new IllegalArgumentException(
                    "Hold type cannot be empty"
            );
        }
    }
}