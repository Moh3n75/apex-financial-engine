package com.apex.credit.application.saga;


import java.util.Optional;
import java.util.UUID;


public interface SagaRepository {


    void save(
            SagaInstance saga
    );


    Optional<SagaInstance> findById(
            UUID id
    );


    void update(
            SagaInstance saga
    );


    void saveStep(
            SagaStep step
    );


    Optional<SagaStep> findStepById(
            UUID id
    );

}