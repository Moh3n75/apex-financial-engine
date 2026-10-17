package com.apex.credit.application.port.out;

import java.util.function.Supplier;

public interface UnitOfWork {

    <T> T execute(
            Supplier<T> operation
    );
}