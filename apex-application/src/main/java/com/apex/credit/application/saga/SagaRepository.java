package com.apex.credit.application.saga;


import java.time.Instant;
import java.util.List;
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

    List<SagaInstance> findRecoverable();


    List<SagaStep> findSteps(UUID sagaId);

    Optional<SagaInstance> claim(
            UUID sagaId,
            String owner
    );

    int releaseExpiredClaims(
            Instant expirationTime
    );

    long countAll();


    long countByStatus(
            SagaStatus status
    );

    List<SagaInstance> findTimedOut(
            Instant threshold
    );

    long countTimedOut(
            Instant threshold
    );

}