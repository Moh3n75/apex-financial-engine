package com.apex.platform.messaging.outbox;

import java.util.UUID;

public record OutboxRecord(

        Long id,

        UUID eventId,

        String eventType,

        int eventVersion,

        UUID correlationId,

        String aggregateId,

        String payload,

        int retryCount

) {}
