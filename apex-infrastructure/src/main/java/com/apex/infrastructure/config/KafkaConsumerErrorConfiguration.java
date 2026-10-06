package com.apex.infrastructure.config;

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
            KafkaTemplate<String, String> kafkaTemplate
    ) {

        DeadLetterPublishingRecoverer recoverer =
                new DeadLetterPublishingRecoverer(
                        kafkaTemplate
                );

        /*
         * 2 retries after the original delivery:
         *
         * attempt 1
         * + retry 1
         * + retry 2
         * = 3 total attempts
         */
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

        /*
         * Malformed / poison messages are not helped
         * by repeating the exact same operation.
         */
        errorHandler.addNotRetryableExceptions(
                NonRetryableEventException.class
        );

        /*
         * We use MANUAL_IMMEDIATE.
         * After successful DLT recovery, commit the
         * original record's offset.
         */
        errorHandler.setCommitRecovered(
                true
        );

        return errorHandler;
    }
}