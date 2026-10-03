package com.apex.platform.events;

import java.time.Instant;
import java.util.UUID;

public record EventMetadata(
        UUID eventId,
        String eventType,
        int eventVersion,
        UUID correlationId,
        UUID causationId,
        String aggregateId,
        String aggregateType,
        String sourceService,
        String cellId,
        Instant occurredAt
) {

    public EventMetadata {
        if (eventId == null) {
            throw new IllegalArgumentException("eventId is required");
        }

        if (eventType == null || eventType.isBlank()) {
            throw new IllegalArgumentException("eventType is required");
        }

        if (eventVersion <= 0) {
            throw new IllegalArgumentException(
                    "eventVersion must be greater than zero"
            );
        }

        if (correlationId == null) {
            throw new IllegalArgumentException(
                    "correlationId is required"
            );
        }

        if (aggregateId == null || aggregateId.isBlank()) {
            throw new IllegalArgumentException(
                    "aggregateId is required"
            );
        }

        if (aggregateType == null || aggregateType.isBlank()) {
            throw new IllegalArgumentException(
                    "aggregateType is required"
            );
        }

        if (sourceService == null || sourceService.isBlank()) {
            throw new IllegalArgumentException(
                    "sourceService is required"
            );
        }

        if (cellId == null || cellId.isBlank()) {
            throw new IllegalArgumentException(
                    "cellId is required"
            );
        }

        if (occurredAt == null) {
            throw new IllegalArgumentException(
                    "occurredAt is required"
            );
        }
    }
}