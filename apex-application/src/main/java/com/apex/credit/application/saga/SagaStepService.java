package com.apex.credit.application.saga;


import java.time.Clock;
import java.time.Instant;
import java.util.UUID;


public class SagaStepService {


    private final SagaRepository sagaRepository;

    private final Clock clock;


    public SagaStepService(
            SagaRepository sagaRepository,
            Clock clock
    ){

        this.sagaRepository = sagaRepository;
        this.clock = clock;

    }



    public SagaStep startStep(
            UUID sagaId,
            String stepName
    ){

        SagaStep step =
                new SagaStep(

                        UUID.randomUUID(),

                        sagaId,

                        stepName,

                        SagaStepStatus.RUNNING,

                        Instant.now(clock),

                        null,

                        null
                );


        sagaRepository.saveStep(step);


        return step;

    }



    public SagaStep completeStep(
            SagaStep step
    ){

        SagaStep completed =
                new SagaStep(

                        step.id(),

                        step.sagaId(),

                        step.stepName(),

                        SagaStepStatus.COMPLETED,

                        step.startedAt(),

                        Instant.now(clock),

                        null
                );


        sagaRepository.saveStep(completed);


        return completed;

    }



    public SagaStep failStep(
            SagaStep step,
            String error
    ){

        SagaStep failed =
                new SagaStep(

                        step.id(),

                        step.sagaId(),

                        step.stepName(),

                        SagaStepStatus.FAILED,

                        step.startedAt(),

                        Instant.now(clock),

                        error
                );


        sagaRepository.saveStep(failed);


        return failed;

    }

}