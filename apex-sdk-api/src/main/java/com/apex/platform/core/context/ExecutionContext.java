package com.apex.platform.core.context;

import java.util.UUID;

public record ExecutionContext(
        UUID correlationId,
        UUID causationId,
        String sourceService,
        String cellId
) {

    public ExecutionContext {
        if (correlationId == null) {
            throw new IllegalArgumentException("correlationId is required");
        }

        if (sourceService == null || sourceService.isBlank()) {
            throw new IllegalArgumentException("sourceService is required");
        }

        if (cellId == null || cellId.isBlank()) {
            throw new IllegalArgumentException("cellId is required");
        }
    }

    public static ExecutionContext root(
            String sourceService,
            String cellId
    ) {
        return new ExecutionContext(
                UUID.randomUUID(),
                null,
                sourceService,
                cellId
        );
    }
}