package com.apex.platform.messaging.jdbc;

import java.time.Clock;
import java.time.Instant;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;

import com.apex.platform.messaging.consumer.ConsumerContext;
import com.apex.platform.messaging.consumer.DispatchResult;
import com.apex.platform.messaging.consumer.EventMessageDispatcher;

/**
 * Kafka listener with configurable topic(s). ACK only after successful dispatch.
 */
public class ConfigurableKafkaEventConsumer {

    private static final Logger log = LoggerFactory.getLogger(ConfigurableKafkaEventConsumer.class);

    private final EventMessageDispatcher dispatcher;
    private final Clock clock;
    private final String consumerGroup;

    public ConfigurableKafkaEventConsumer(
            EventMessageDispatcher dispatcher,
            Clock clock,
            String consumerGroup
    ) {
        this.dispatcher = dispatcher;
        this.clock = clock;
        this.consumerGroup = consumerGroup;
    }

    @KafkaListener(
            topics = "${apex.messaging.consumer.topics}",
            groupId = "${spring.kafka.consumer.group-id}",
            concurrency = "${apex.messaging.consumer.concurrency:1}"
    )
    public void consume(ConsumerRecord<String, String> record, Acknowledgment acknowledgment) {
        ConsumerContext context = new ConsumerContext(
                consumerGroup,
                record.topic(),
                record.partition(),
                record.offset(),
                record.key(),
                Instant.now(clock)
        );

        DispatchResult result = dispatcher.dispatch(record.value(), context);

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

        acknowledgment.acknowledge();
    }
}
