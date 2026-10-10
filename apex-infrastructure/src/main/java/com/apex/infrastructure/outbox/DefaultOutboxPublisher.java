package com.apex.infrastructure.outbox;


import com.apex.infrastructure.messaging.kafka.KafkaPlatformConfiguration;
import com.apex.platform.messaging.kafka.KafkaMessagePublisher;
import com.apex.platform.messaging.outbox.OutboxEventMapper;
import com.apex.platform.messaging.outbox.OutboxPublisher;
import com.apex.platform.messaging.outbox.OutboxRecord;
import com.apex.platform.messaging.outbox.OutboxStore;
import com.apex.platform.messaging.outbox.RetryPolicy;

import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

import tools.jackson.databind.json.JsonMapper;

import java.time.Duration;
import java.time.Instant;


@Component
public class DefaultOutboxPublisher
        implements OutboxPublisher {


    private final String TOPIC;

    private final OutboxStore outboxStore;

    private final KafkaMessagePublisher kafkaPublisher;

    private final JsonMapper jsonMapper;

    private final OutboxEventMapper outboxEventMapper;

    private final RetryPolicy retryPolicy;

    private final OutboxMetrics outboxMetrics;

    private final String publisherInstanceId;

    private final Duration leaseDuration =
            Duration.ofMinutes(5);


    public DefaultOutboxPublisher(

            OutboxStore outboxStore,

            KafkaMessagePublisher kafkaPublisher,

            JsonMapper jsonMapper,

            OutboxEventMapper outboxEventMapper,

            RetryPolicy retryPolicy,

            OutboxMetrics outboxMetrics,

            @Qualifier("outboxPublisherInstanceId")
            String publisherInstanceId

    ) {

        this.outboxStore = outboxStore;

        this.kafkaPublisher = kafkaPublisher;

        this.jsonMapper = jsonMapper;

        this.outboxEventMapper = outboxEventMapper;

        this.retryPolicy = retryPolicy;

        this.outboxMetrics = outboxMetrics;

        this.publisherInstanceId = publisherInstanceId;

        TOPIC = KafkaPlatformConfiguration.FINANCIAL_EVENTS_TOPIC;

    }


    @Override
    public void publishBatch() {


        var events =
                outboxStore.claimBatch(
                        100,
                        publisherInstanceId,
                        leaseDuration
                );


        for (OutboxRecord event : events) {


            try {


                var envelope =
                        outboxEventMapper.map(event);


                String message =
                        jsonMapper.writeValueAsString(
                                envelope
                        );


                kafkaPublisher.publish(

                                TOPIC,

                                event.aggregateType()
                                        + ":"
                                        + event.aggregateId(),

                                message

                        )
                        .toCompletableFuture()
                        .join();


                outboxStore.markPublished(
                        event.id(),
                        event.claimToken()
                );


                outboxMetrics.incrementPublished();

            } catch (Exception exception) {


                if (
                        retryPolicy.canRetry(
                                event.retryCount()
                        )
                ) {

                    outboxStore.markFailed(
                            event.id(),
                            event.claimToken(),
                            exception.getMessage(),
                            retryPolicy.nextRetryTime(
                                    event.retryCount()
                            )
                    );

                    outboxMetrics.incrementFailed();

                } else {

                    outboxStore.moveToDeadLetter(
                            event.id(),
                            event.claimToken(),
                            exception.getMessage()
                    );

                    outboxMetrics.incrementDeadLetter();

                }

            }


        }

    }

}