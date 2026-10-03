package com.apex.platform.events;

public record EventEnvelope<T extends IntegrationEvent>(
        EventMetadata metadata,
        T payload
) {

    public EventEnvelope {
        if (metadata == null) {
            throw new IllegalArgumentException(
                    "metadata is required"
            );
        }

        if (payload == null) {
            throw new IllegalArgumentException(
                    "payload is required"
            );
        }
    }
}