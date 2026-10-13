package com.apex.credit.application.saga;


import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.UUID;


public class DefaultSagaRecoveryService
        implements SagaRecoveryService {


    private final SagaRepository sagaRepository;

    private final SagaResumeDecisionEngine decisionEngine;

    private final String owner =
            UUID.randomUUID().toString();
    ;

    public DefaultSagaRecoveryService(
            SagaRepository sagaRepository,
            SagaResumeDecisionEngine decisionEngine
    ) {

        this.sagaRepository = sagaRepository;
        this.decisionEngine = decisionEngine;
    }


    @Override
    public void recover() {

        sagaRepository.releaseExpiredClaims(
                Instant.now()
                        .minus(Duration.ofMinutes(10))
        );

        List<SagaInstance> sagas =
                sagaRepository.findRecoverable();


        for (SagaInstance saga : sagas) {

            Optional<SagaInstance> claimed =
                    sagaRepository.claim(
                            saga.id(),
                            owner
                    );


            claimed.ifPresent(
                    this::resume
            );

        }

    }


    private void resume(
            SagaInstance saga
    ) {

        List<SagaStep> steps =
                sagaRepository.findSteps(
                        saga.id()
                );

        SagaResumeDecisionEngine.SagaRecoveryDecision decision =
                decisionEngine.decide(
                        steps
                );


        switch (decision.action()) {


            case RESUME_STEP -> {

                SagaStep step =
                        decision.step();

                // execute again

            }


            case COMPLETE -> {

                // mark saga completed

            }


            case COMPENSATE -> {

                // compensation flow

            }

        }

    }

}