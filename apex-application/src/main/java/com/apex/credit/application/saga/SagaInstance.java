package com.apex.credit.application.saga;

import java.time.Instant;
import java.util.UUID;


public record SagaInstance(

        UUID id,

        String sagaType,

        String correlationId,

        SagaStatus status,

        Instant startedAt,

        Instant completedAt,

        String claimedBy,

        Instant claimedAt

) {
}