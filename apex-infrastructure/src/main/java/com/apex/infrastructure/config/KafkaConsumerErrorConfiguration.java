package com.apex.infrastructure.config;

import com.apex.infrastructure.messaging.consumer.ConsumerFailureRetryListener;
import com.apex.platform.messaging.consumer.NonRetryableEventException;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.CommonErrorHandler;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;

import org.springframework.util.backoff.FixedBackOff;

@Configuration
public class KafkaConsumerErrorConfiguration {

    @Bean
    public CommonErrorHandler kafkaCommonErrorHandler(
            KafkaTemplate<String, String> kafkaTemplate,
            ConsumerFailureRetryListener failureRetryListener
    ) {

        DeadLetterPublishingRecoverer recoverer =
                new DeadLetterPublishingRecoverer(
                        kafkaTemplate
                );

        FixedBackOff backOff =
                new FixedBackOff(
                        1_000L,
                        2L
                );

        DefaultErrorHandler errorHandler =
                new DefaultErrorHandler(
                        recoverer,
                        backOff
                );

        errorHandler.addNotRetryableExceptions(
                NonRetryableEventException.class
        );

        errorHandler.setCommitRecovered(
                true
        );

        errorHandler.setRetryListeners(
                failureRetryListener
        );

        return errorHandler;
    }
}