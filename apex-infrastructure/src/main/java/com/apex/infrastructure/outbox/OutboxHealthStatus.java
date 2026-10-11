package com.apex.infrastructure.outbox;


public record OutboxHealthStatus(

        boolean healthy,

        long pendingEvents,

        long oldestEventAgeSeconds,

        String reason

) {}