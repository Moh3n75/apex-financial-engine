package com.apex.credit.application.saga;



import java.time.Clock;
import java.time.Instant;
import java.util.UUID;



public class SagaLifecycleService {


    private final SagaRepository sagaRepository;

    private final Clock clock;


    public SagaLifecycleService(
            SagaRepository sagaRepository,
            Clock clock
    ){

        this.sagaRepository = sagaRepository;
        this.clock = clock;

    }



    public SagaInstance start(
            String sagaType,
            String correlationId
    ){

        SagaInstance saga =
                new SagaInstance(

                        UUID.randomUUID(),

                        sagaType,

                        correlationId,

                        SagaStatus.STARTED,

                        Instant.now(clock),

                        null
                );


        sagaRepository.save(saga);


        return saga;

    }


}