package com.apex.platform.messaging.jdbc;

import java.time.Duration;

import com.apex.platform.messaging.kafka.KafkaMessagePublisher;
import com.apex.platform.messaging.outbox.OutboxEventMapper;
import com.apex.platform.messaging.outbox.OutboxPublisher;
import com.apex.platform.messaging.outbox.OutboxRecord;
import com.apex.platform.messaging.outbox.OutboxStore;
import com.apex.platform.messaging.outbox.RetryPolicy;

import tools.jackson.databind.json.JsonMapper;

/**
 * Lease-based Outbox publisher with configurable topic (Cell-owned routing).
 */
public class ConfigurableOutboxPublisher implements OutboxPublisher {

    private final OutboxStore outboxStore;
    private final KafkaMessagePublisher kafkaPublisher;
    private final JsonMapper jsonMapper;
    private final OutboxEventMapper outboxEventMapper;
    private final RetryPolicy retryPolicy;
    private final String publisherInstanceId;
    private final String topic;
    private final Duration leaseDuration = Duration.ofMinutes(5);

    public ConfigurableOutboxPublisher(
            OutboxStore outboxStore,
            KafkaMessagePublisher kafkaPublisher,
            JsonMapper jsonMapper,
            OutboxEventMapper outboxEventMapper,
            RetryPolicy retryPolicy,
            String publisherInstanceId,
            String topic
    ) {
        this.outboxStore = outboxStore;
        this.kafkaPublisher = kafkaPublisher;
        this.jsonMapper = jsonMapper;
        this.outboxEventMapper = outboxEventMapper;
        this.retryPolicy = retryPolicy;
        this.publisherInstanceId = publisherInstanceId;
        this.topic = topic;
    }

    @Override
    public void publishBatch() {
        var events = outboxStore.claimBatch(100, publisherInstanceId, leaseDuration);
        if (events.isEmpty()) {
            return;
        }

        for (OutboxRecord event : events) {
            try {
                var envelope = outboxEventMapper.map(event);
                String message = jsonMapper.writeValueAsString(envelope);
                String key = event.aggregateType() + ":" + event.aggregateId();

                kafkaPublisher.publish(topic, key, message)
                        .toCompletableFuture()
                        .join();

                outboxStore.markPublished(event.id(), event.claimToken());
            } catch (Exception exception) {
                if (retryPolicy.canRetry(event.retryCount())) {
                    outboxStore.markFailed(
                            event.id(),
                            event.claimToken(),
                            exception.getMessage(),
                            retryPolicy.nextRetryTime(event.retryCount())
                    );
                } else {
                    outboxStore.moveToDeadLetter(
                            event.id(),
                            event.claimToken(),
                            exception.getMessage()
                    );
                }
            }
        }
    }
}
