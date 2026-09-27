package com.apex.credit.domain.valueobject;

import java.math.BigDecimal;

public record Money(BigDecimal amount) {

    public Money {
        if (amount == null || amount.compareTo(BigDecimal.ZERO) < 0) {
            throw new IllegalArgumentException("Invalid money amount");
        }
    }

    public Money add(Money other) {
        return new Money(
                this.amount.add(other.amount())
        );
    }

    public Money subtract(Money other) {
        if (this.amount.compareTo(other.amount()) < 0) {
            throw new IllegalArgumentException(
                    "Insufficient amount"
            );
        }

        return new Money(
                this.amount.subtract(other.amount())
        );
    }

    public boolean greaterOrEqual(Money other) {
        return this.amount.compareTo(other.amount()) >= 0;
    }

    public boolean lessThan(Money other) {
        return this.amount.compareTo(other.amount()) < 0;
    }

    public boolean isZero() {
        return this.amount.compareTo(BigDecimal.ZERO) == 0;
    }
}