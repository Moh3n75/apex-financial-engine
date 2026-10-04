package com.apex.platform.outbox;

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
