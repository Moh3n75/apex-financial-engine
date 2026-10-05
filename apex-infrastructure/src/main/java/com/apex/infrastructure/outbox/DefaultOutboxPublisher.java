package com.apex.infrastructure.outbox;


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


    private static final String TOPIC =
            "apex.financial.events.v1";


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

                                event.eventId()
                                        .toString(),

                                message

                        )
                        .toCompletableFuture()
                        .join();



                outboxStore.markPublished(
                        event.id()
                );


            }

            catch (Exception exception) {



                Instant nextRetryAt =
                        retryPolicy.nextRetryTime(
                                event.retryCount()
                        );



                outboxStore.markFailed(

                        event.id(),

                        exception.getMessage(),

                        nextRetryAt

                );

            }

        }

    }

}