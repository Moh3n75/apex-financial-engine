package com.apex.infrastructure.messaging.consumer;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;

import org.springframework.stereotype.Component;

@Component
public class ConsumerMetrics {

    private final Counter failedDeliveryCounter;

    private final Counter deadLetterCounter;

    private final Counter recoveryFailedCounter;


    public ConsumerMetrics(
            MeterRegistry registry
    ) {

        this.failedDeliveryCounter =
                Counter.builder(
                                "apex.consumer.delivery.failed"
                        )
                        .description(
                                "Number of failed Kafka event delivery attempts"
                        )
                        .register(registry);


        this.deadLetterCounter =
                Counter.builder(
                                "apex.consumer.deadletter.published"
                        )
                        .description(
                                "Number of events successfully recovered to DLT"
                        )
                        .register(registry);


        this.recoveryFailedCounter =
                Counter.builder(
                                "apex.consumer.deadletter.publish.failed"
                        )
                        .description(
                                "Number of failed DLT recovery attempts"
                        )
                        .register(registry);
    }


    public void incrementFailedDelivery() {

        failedDeliveryCounter.increment();
    }


    public void incrementDeadLettered() {

        deadLetterCounter.increment();
    }


    public void incrementRecoveryFailed() {

        recoveryFailedCounter.increment();
    }
}