package com.apex.apexbootstrap.config;

import com.apex.credit.application.saga.*;
import com.apex.infrastructure.saga.SagaEventOutboxMapper;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class SagaConfiguration {


    @Bean
    public SagaRecoveryService sagaRecoveryService(
            SagaRepository sagaRepository,
            SagaResumeDecisionEngine engine
    ){

        return new DefaultSagaRecoveryService(
                sagaRepository,
                engine
        );

    }

    @Bean
    public SagaResumeDecisionEngine sagaResumeDecisionEngine(){

        return new SagaResumeDecisionEngine();

    }

    @Bean
    public SagaEventOutboxMapper sagaEventOutboxMapper(){

        return new SagaEventOutboxMapper();

    }

    @Bean
    public SagaMetricsService sagaMetricsService(
            SagaRepository sagaRepository
    ){

        return new DefaultSagaMetricsService(
                sagaRepository
        );

    }

    @Bean
    public SagaTimeoutPolicyResolver sagaTimeoutPolicyResolver(){

        return new DefaultSagaTimeoutPolicyResolver();

    }

    @Bean
    public SagaTimeoutService sagaTimeoutService(
            SagaRepository sagaRepository,
            SagaTimeoutPolicyResolver resolver,
            SagaEventPublisher eventPublisher
    ){

        return new DefaultSagaTimeoutService(
                sagaRepository,
                resolver,
                eventPublisher
        );

    }

}