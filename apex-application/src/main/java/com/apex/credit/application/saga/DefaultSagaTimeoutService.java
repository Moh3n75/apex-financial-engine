package com.apex.credit.application.saga;


import com.apex.platform.events.saga.SagaTimeoutDetectedEvent;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.UUID;


public class DefaultSagaTimeoutService
        implements SagaTimeoutService {


    private final SagaRepository sagaRepository;

    private final SagaTimeoutPolicyResolver policyResolver;

    private final SagaEventPublisher eventPublisher;

    public DefaultSagaTimeoutService(
            SagaRepository sagaRepository,
            SagaTimeoutPolicyResolver policyResolver, SagaEventPublisher eventPublisher
    ){

        this.sagaRepository = sagaRepository;

        this.policyResolver = policyResolver;
        this.eventPublisher = eventPublisher;
    }



    @Override
    public void detectTimeouts(){

        Instant threshold =
                Instant.now()
                        .minus(
                                Duration.ofMinutes(30)
                        );


        List<SagaInstance> timedOut =
                sagaRepository.findTimedOut(
                        threshold
                );


        for(SagaInstance saga : timedOut){

            handleTimeout(saga);

        }

    }



    private void handleTimeout(
            SagaInstance saga
    ){

        SagaTimeoutPolicy policy =
                policyResolver.resolve(
                        saga
                );

        eventPublisher.publish(
                new SagaTimeoutDetectedEvent(

                        UUID.randomUUID(),

                        saga.id(),

                        saga.sagaType(),

                        UUID.fromString(
                                saga.correlationId()
                        ),

                        Instant.now(),

                        "Saga timeout detected"

                )
        );

        switch(policy){

            case RETRY -> {

                // retry logic

            }


            case COMPENSATE -> {

                // compensation logic

            }


            case FAIL -> {

                // mark failed

            }


            case ALERT -> {

                // publish alert

            }

        }

    }

}