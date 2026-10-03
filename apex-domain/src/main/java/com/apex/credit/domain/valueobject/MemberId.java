package com.apex.credit.domain.valueobject;

import java.util.UUID;


public record MemberId(UUID value) {


    public MemberId {

        if(value == null){
            throw new IllegalArgumentException(
                    "MemberId cannot be null"
            );
        }

    }

}
