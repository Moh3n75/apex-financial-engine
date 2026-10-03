package com.apex.credit.domain.valueobject;

public record CreditAmount(long units) {

    public static final CreditAmount ZERO =
            new CreditAmount(0);

    public CreditAmount {
        if (units < 0) {
            throw new IllegalArgumentException(
                    "Credit amount cannot be negative"
            );
        }
    }

    public static CreditAmount of(long units) {
        return new CreditAmount(units);
    }

    public boolean isZero() {
        return units == 0;
    }

    public boolean isPositive() {
        return units > 0;
    }

    public boolean greaterOrEqual(CreditAmount other) {
        return units >= other.units;
    }

    public CreditAmount add(CreditAmount other) {
        return new CreditAmount(
                Math.addExact(units, other.units)
        );
    }

    public CreditAmount subtract(CreditAmount other) {

        if (units < other.units) {
            throw new IllegalArgumentException(
                    "Insufficient credit amount"
            );
        }

        return new CreditAmount(
                Math.subtractExact(units, other.units)
        );
    }
}