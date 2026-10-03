package com.apex.platform.events;

import com.apex.platform.core.context.ExecutionContext;

import java.time.Clock;
import java.time.Instant;
import java.util.UUID;

public final class EventEnvelopeFactory {

    private final Clock clock;

    public EventEnvelopeFactory(Clock clock) {
        this.clock = clock;
    }

    public <T extends IntegrationEvent> EventEnvelope<T> create(
            String eventType,
            int eventVersion,
            String aggregateId,
            String aggregateType,
            ExecutionContext context,
            T payload
    ) {

        EventMetadata metadata = new EventMetadata(
                UUID.randomUUID(),
                eventType,
                eventVersion,
                context.correlationId(),
                context.causationId(),
                aggregateId,
                aggregateType,
                context.sourceService(),
                context.cellId(),
                Instant.now(clock)
        );

        return new EventEnvelope<>(
                metadata,
                payload
        );
    }
}