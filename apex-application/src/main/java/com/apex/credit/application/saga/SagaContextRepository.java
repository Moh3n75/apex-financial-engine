package com.apex.credit.application.saga;


import java.util.Optional;
import java.util.UUID;


public interface SagaContextRepository {


    void save(
            SagaExecutionContext context
    );


    Optional<SagaExecutionContext> findBySagaId(
            UUID sagaId
    );


}