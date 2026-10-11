package com.apex.credit.application.saga;

import java.time.Instant;
import java.util.UUID;


public record SagaStep(

        UUID id,

        UUID sagaId,

        String stepName,

        SagaStepStatus status,

        Instant startedAt,

        Instant completedAt,

        String error

) {}
