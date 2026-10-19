
package com.apex.platform.events.api;

import java.util.Objects;

public record EventContract<T>(
        String eventType,
        int eventVersion,
        String aggregateType,
        Class<T> payloadType,
        String schemaId
) {

    public EventContract {
        if (eventType == null || eventType.isBlank()) {
            throw new IllegalArgumentException(
                    "Event type must not be blank"
            );
        }

        if (eventVersion < 1) {
            throw new IllegalArgumentException(
                    "Event version must be positive"
            );
        }

        if (aggregateType == null || aggregateType.isBlank()) {
            throw new IllegalArgumentException(
                    "Aggregate type must not be blank"
            );
        }

        Objects.requireNonNull(
                payloadType,
                "Payload type must not be null"
        );

        if (schemaId == null || schemaId.isBlank()) {
            throw new IllegalArgumentException(
                    "Schema ID must not be blank"
            );
        }
    }
}
