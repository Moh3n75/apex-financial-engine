package com.apex.platform.messaging.jdbc;

import java.time.Duration;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

    private static final Logger log = LoggerFactory.getLogger(ConfigurableOutboxPublisher.class);

    private final OutboxStore outboxStore;
    private final KafkaMessagePublisher kafkaPublisher;
    private final JsonMapper jsonMapper;
    private final OutboxEventMapper outboxEventMapper;
    private final RetryPolicy retryPolicy;
    private final String publisherInstanceId;
    private final String topic;
    private final PublisherRouting routing;
    private final com.apex.platform.messaging.observation.MessagingObservation observations;
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
        this(outboxStore,kafkaPublisher,jsonMapper,outboxEventMapper,retryPolicy,publisherInstanceId,topic,
                new PublisherRouting(false, java.util.List.of()));
    }

    public ConfigurableOutboxPublisher(OutboxStore outboxStore, KafkaMessagePublisher kafkaPublisher,
            JsonMapper jsonMapper, OutboxEventMapper outboxEventMapper, RetryPolicy retryPolicy,
            String publisherInstanceId, String topic, PublisherRouting routing) {
        this(outboxStore,kafkaPublisher,jsonMapper,outboxEventMapper,retryPolicy,publisherInstanceId,topic,routing,
            com.apex.platform.messaging.observation.MessagingObservation.NONE);
    }
    public ConfigurableOutboxPublisher(OutboxStore outboxStore, KafkaMessagePublisher kafkaPublisher,
            JsonMapper jsonMapper, OutboxEventMapper outboxEventMapper, RetryPolicy retryPolicy,
            String publisherInstanceId, String topic, PublisherRouting routing,
            com.apex.platform.messaging.observation.MessagingObservation observations) {
        this.outboxStore = outboxStore;
        this.kafkaPublisher = kafkaPublisher;
        this.jsonMapper = jsonMapper;
        this.outboxEventMapper = outboxEventMapper;
        this.retryPolicy = retryPolicy;
        this.publisherInstanceId = publisherInstanceId;
        this.topic = topic;
        this.routing = routing;
        this.observations = observations;
    }

    @Override
    public void publishBatch() {
        var claim=observations.start("outbox.claim.attempt");
        java.util.List<com.apex.platform.messaging.outbox.OutboxRecord> events;
        try {events=outboxStore.claimBatch(100,publisherInstanceId,leaseDuration);claim.finish("OBSERVED");}
        catch(RuntimeException failure){claim.finish("FAILED");throw failure;}
        if (events.isEmpty()) {
            return;
        }

        for (OutboxRecord event : events) {
            String destination = topic;
            var attempt=observations.start("outbox.send");
            try {
                var envelope = outboxEventMapper.map(event);
                String message = jsonMapper.writeValueAsString(envelope);
                String key = event.aggregateType() + ":" + event.aggregateId();

                destination = routing.destination(event.eventType(), event.eventVersion(), topic);
                kafkaPublisher.publish(destination, key, message)
                        .toCompletableFuture()
                        .join();
                attempt.finish("SUCCEEDED"); // Transport completion, NOT owner mark or consumer success.

                var mark=observations.start("outbox.mark.attempt");
                try {mark.finish(outboxStore.markPublished(event.id(),event.claimToken())?"OBSERVED":"UNKNOWN");}
                catch(RuntimeException failure){mark.finish("UNKNOWN");throw failure;}
            } catch (Exception exception) {
                attempt.finish("FAILED");
                String diagnostic = failureDiagnostic(exception);
                // Do not log the producer record or payload: KafkaProducerException carries it.
                log.warn("Outbox publication failed eventId={} correlationId={} topic={} failure={}",
                        event.eventId(), event.correlationId(), destination, diagnostic);
                if (retryPolicy.canRetry(event.retryCount())) {
                    outboxStore.markFailed(
                            event.id(),
                            event.claimToken(),
                            diagnostic,
                            retryPolicy.nextRetryTime(event.retryCount())
                    );
                } else {
                    outboxStore.moveToDeadLetter(
                            event.id(),
                            event.claimToken(),
                            diagnostic
                    );
                }
            }
        }
    }

    private static String failureDiagnostic(Throwable failure) {
        StringBuilder chain = new StringBuilder();
        Throwable current = failure;
        for (int depth = 0; current != null && depth < 12; depth++) {
            if (!chain.isEmpty()) chain.append(" -> ");
            chain.append(current.getClass().getName());
            // Exception class chains expose nested causes without serializing customer data.
            if (current.getCause() == current) break;
            current = current.getCause();
        }
        return chain.toString();
    }
}
