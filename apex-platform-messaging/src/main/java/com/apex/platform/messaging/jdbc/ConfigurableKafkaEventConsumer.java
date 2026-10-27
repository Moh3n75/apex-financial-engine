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
import com.apex.platform.messaging.consumer.UnsupportedEventContractException;
import tools.jackson.databind.json.JsonMapper;

/**
 * Kafka listener with configurable topic(s). ACK only after successful dispatch.
 */
public class ConfigurableKafkaEventConsumer {

    private static final Logger log = LoggerFactory.getLogger(ConfigurableKafkaEventConsumer.class);

    private final EventMessageDispatcher dispatcher;
    private final Clock clock;
    private final String consumerGroup;
    private final boolean reliable;
    private final JsonMapper jsonMapper;

    public ConfigurableKafkaEventConsumer(
            EventMessageDispatcher dispatcher,
            Clock clock,
            String consumerGroup
    ) {
        this(dispatcher, clock, consumerGroup, false, null);
    }

    public ConfigurableKafkaEventConsumer(EventMessageDispatcher dispatcher, Clock clock,
            String consumerGroup, boolean reliable, JsonMapper jsonMapper) {
        this.dispatcher = dispatcher;
        this.clock = clock;
        this.consumerGroup = consumerGroup;
        this.reliable = reliable;
        this.jsonMapper = jsonMapper;
    }

    public boolean isReliable() {
        return reliable;
    }

    @KafkaListener(
            topics = "#{T(com.apex.platform.messaging.jdbc.ConsumerTopics).parse('${apex.messaging.consumer.topics}')}",
            groupId = "${spring.kafka.consumer.group-id}",
            containerFactory = "#{${apex.messaging.consumer.reliability.enabled:false} ? 'apexReliableKafkaListenerContainerFactory' : ''}",
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
            if (reliable) {
                // Also guard custom dispatchers that still return IGNORED.
                String type = null;
                Integer version = null;
                try {
                    var metadata = jsonMapper.readTree(record.value()).path("metadata");
                    if (metadata.path("eventType").isTextual()) {
                        type = metadata.path("eventType").asText();
                    }
                    if (metadata.path("eventVersion").isIntegralNumber()) {
                        version = metadata.path("eventVersion").asInt();
                    }
                } catch (RuntimeException ignored) {
                    // Identification is best-effort; the raw record is preserved only in DLT.
                }
                throw new UnsupportedEventContractException(type, version, context);
            }
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
