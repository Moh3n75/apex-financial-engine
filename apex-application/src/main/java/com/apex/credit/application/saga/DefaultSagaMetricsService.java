package com.apex.credit.application.saga;


import java.time.Duration;
import java.time.Instant;

public class DefaultSagaMetricsService
        implements SagaMetricsService {


    private final SagaRepository sagaRepository;


    public DefaultSagaMetricsService(
            SagaRepository sagaRepository
    ){

        this.sagaRepository = sagaRepository;

    }



    @Override
    public SagaMetrics collect(){

        long total =
                sagaRepository.countAll();


        long running =
                sagaRepository.countByStatus(
                        SagaStatus.RUNNING
                );


        long failed =
                sagaRepository.countByStatus(
                        SagaStatus.FAILED
                );


        long compensating =
                sagaRepository.countByStatus(
                        SagaStatus.COMPENSATING
                );

        long timedOut =
                sagaRepository.countTimedOut(
                        Instant.now()
                                .minus(
                                        Duration.ofMinutes(30)
                                )
                );


        return new SagaMetrics(
                total,
                running,
                failed,
                compensating,
                timedOut
        );

    }

}