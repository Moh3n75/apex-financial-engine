package com.apex.infrastructure.outbox;



import com.apex.platform.messaging.kafka.KafkaMessagePublisher;
import com.apex.platform.messaging.outbox.OutboxPublisher;
import com.apex.platform.messaging.outbox.OutboxRecord;
import com.apex.platform.messaging.outbox.OutboxStore;
import org.springframework.stereotype.Component;

import tools.jackson.databind.json.JsonMapper;

@Component
public class DefaultOutboxPublisher
        implements OutboxPublisher {


    private static final String TOPIC =
            "apex.financial.events.v1";


    private final OutboxStore outboxStore;

    private final KafkaMessagePublisher kafkaPublisher;

    private final JsonMapper jsonMapper;


    public DefaultOutboxPublisher(
            OutboxStore outboxStore,
            KafkaMessagePublisher kafkaPublisher,
            JsonMapper jsonMapper
    ) {
        this.outboxStore = outboxStore;
        this.kafkaPublisher = kafkaPublisher;
        this.jsonMapper = jsonMapper;
    }


    @Override
    public void publishBatch() {


        var events =
                outboxStore.findBatch(100);


        for (OutboxRecord event : events) {


            try {

                String message =
                        jsonMapper.writeValueAsString(
                                event
                        );


                kafkaPublisher.publish(

                                TOPIC,

                                event.eventId()
                                        .toString(),

                                message

                        ).toCompletableFuture()
                        .join();


                outboxStore.markPublished(
                        event.id()
                );


            }
            catch (Exception exception) {


                outboxStore.markFailed(

                        event.id(),

                        exception.getMessage()

                );

            }

        }

    }
}