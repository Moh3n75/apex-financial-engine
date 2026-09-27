package com.apex.credit.domain.valueobject;

import java.util.UUID;


public record CreditAccountId(UUID value) {


    public CreditAccountId {

        if(value == null){
            throw new IllegalArgumentException(
                    "CreditAccountId cannot be null"
            );
        }

    }

}