package com.apex.credit.domain.valueobject;

import java.math.BigDecimal;


public record Money(BigDecimal amount) {


    public Money {

        if(amount == null ||
                amount.compareTo(BigDecimal.ZERO)<0){

            throw new IllegalArgumentException(
                    "Invalid money"
            );
        }
    }


    public Money add(Money other){

        return new Money(
                amount.add(other.amount())
        );

    }


    public Money subtract(Money other){

        if(amount.compareTo(other.amount()) < 0){

            throw new IllegalArgumentException(
                    "Insufficient amount"
            );
        }


        return new Money(
                amount.subtract(other.amount())
        );

    }


}