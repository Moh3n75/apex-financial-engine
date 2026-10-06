package com.apex.infrastructure.messaging.consumer;

import com.apex.platform.messaging.consumer.ConsumerContext;
import com.apex.platform.messaging.consumer.DispatchResult;
import com.apex.platform.messaging.consumer.EventMessageDispatcher;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

import java.time.Clock;
import java.time.Instant;

@Component
public class KafkaEventConsumer {

    private static final Logger log =
            LoggerFactory.getLogger(
                    KafkaEventConsumer.class
            );

    private final EventMessageDispatcher dispatcher;

    private final Clock clock;

    private final String consumerGroup;

    public KafkaEventConsumer(
            EventMessageDispatcher dispatcher,
            Clock clock,
            @Value("${spring.kafka.consumer.group-id}")
            String consumerGroup
    ) {

        this.dispatcher = dispatcher;
        this.clock = clock;
        this.consumerGroup = consumerGroup;
    }

    @KafkaListener(
            topics = "${apex.messaging.topics.financial-events}",
            groupId = "${spring.kafka.consumer.group-id}",
            concurrency = "${apex.messaging.consumer.concurrency:3}"
    )
    public void consume(
            ConsumerRecord<String, String> record,
            Acknowledgment acknowledgment
    ) {

        ConsumerContext context =
                new ConsumerContext(
                        consumerGroup,
                        record.topic(),
                        record.partition(),
                        record.offset(),
                        record.key(),
                        Instant.now(clock)
                );

        DispatchResult result =
                dispatcher.dispatch(
                        record.value(),
                        context
                );

        if (result == DispatchResult.DUPLICATE) {

            log.debug(
                    "Duplicate Kafka event ignored topic={} partition={} offset={}",
                    record.topic(),
                    record.partition(),
                    record.offset()
            );
        }

        if (result == DispatchResult.IGNORED) {

            log.debug(
                    "No handler registered topic={} partition={} offset={}",
                    record.topic(),
                    record.partition(),
                    record.offset()
            );
        }

        /*
         * ACK only after successful dispatch.
         *
         * If dispatcher/handler throws,
         * this line is never reached.
         */
        acknowledgment.acknowledge();
    }
}