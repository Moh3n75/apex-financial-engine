package com.apex.infrastructure.messaging.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import org.springframework.kafka.listener.RetryListener;
import org.springframework.stereotype.Component;

@Component
public class ConsumerFailureRetryListener
        implements RetryListener {

    private final ConsumerFailureAuditService auditService;

    private final ConsumerMetrics metrics;


    public ConsumerFailureRetryListener(
            ConsumerFailureAuditService auditService,
            ConsumerMetrics metrics
    ) {

        this.auditService = auditService;
        this.metrics = metrics;
    }


    @Override
    public void failedDelivery(
            ConsumerRecord<?, ?> record,
            Exception exception,
            int deliveryAttempt
    ) {

        auditService.recordFailure(
                record,
                exception,
                deliveryAttempt
        );

        metrics.incrementFailedDelivery();
    }


    @Override
    public void recovered(
            ConsumerRecord<?, ?> record,
            Exception exception
    ) {

        /*
         * Our recoverer is DeadLetterPublishingRecoverer.
         *
         * Therefore recovered() means the record
         * was successfully published to DLT.
         */

        auditService.markDeadLettered(
                record,
                exception
        );

        metrics.incrementDeadLettered();
    }


    @Override
    public void recoveryFailed(
            ConsumerRecord<?, ?> record,
            Exception original,
            Exception failure
    ) {

        auditService.markRecoveryFailed(
                record,
                failure
        );

        metrics.incrementRecoveryFailed();
    }
}