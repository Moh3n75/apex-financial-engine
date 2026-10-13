package com.apex.platform.events.saga;


import java.time.Instant;
import java.util.UUID;


public record SagaTimeoutDetectedEvent(

        UUID eventId,

        UUID sagaId,

        String sagaType,

        UUID correlationId,

        Instant occurredAt,

        String reason

) implements SagaEvent {


}