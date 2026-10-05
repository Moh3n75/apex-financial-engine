package com.apex.infrastructure.outbox;


import com.apex.infrastructure.messaging.kafka.KafkaPlatformConfiguration;
import com.apex.platform.messaging.kafka.KafkaMessagePublisher;
import com.apex.platform.messaging.outbox.OutboxEventMapper;
import com.apex.platform.messaging.outbox.OutboxPublisher;
import com.apex.platform.messaging.outbox.OutboxRecord;
import com.apex.platform.messaging.outbox.OutboxStore;
import com.apex.platform.messaging.outbox.RetryPolicy;

import org.springframework.stereotype.Component;

import tools.jackson.databind.json.JsonMapper;

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


    public DefaultOutboxPublisher(

            OutboxStore outboxStore,

            KafkaMessagePublisher kafkaPublisher,

            JsonMapper jsonMapper,

            OutboxEventMapper outboxEventMapper,

            RetryPolicy retryPolicy

    ) {

        this.outboxStore = outboxStore;

        this.kafkaPublisher = kafkaPublisher;

        this.jsonMapper = jsonMapper;

        this.outboxEventMapper = outboxEventMapper;

        this.retryPolicy = retryPolicy;

        TOPIC = KafkaPlatformConfiguration.FINANCIAL_EVENTS_TOPIC;

    }


    @Override
    public void publishBatch() {


        var events =
                outboxStore.findBatch(100);


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
                        event.id()
                );


            } catch (Exception exception) {


                if (
                        retryPolicy.canRetry(
                                event.retryCount()
                        )
                ) {

                    outboxStore.markFailed(
                            event.id(),
                            exception.getMessage(),
                            retryPolicy.nextRetryTime(
                                    event.retryCount()
                            )
                    );

                } else {

                    outboxStore.moveToDeadLetter(
                            event.id(),
                            exception.getMessage()
                    );

                }

            }


        }

    }

}