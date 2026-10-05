package com.apex.platform.messaging.outbox;


import java.time.Instant;
import java.util.UUID;


public record OutboxRecord(

        Long id,

        UUID eventId,

        String eventType,

        int eventVersion,

        UUID correlationId,

        UUID causationId,

        String aggregateId,

        String aggregateType,

        String sourceService,

        String cellId,

        Instant occurredAt,

        String payload,

        int retryCount,

        Instant nextRetryAt

) {
}