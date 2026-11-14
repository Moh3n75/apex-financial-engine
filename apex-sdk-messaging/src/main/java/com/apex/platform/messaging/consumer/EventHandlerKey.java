package com.apex.platform.messaging.consumer;

public record EventHandlerKey(
        String eventType,
        int eventVersion
) {

    public EventHandlerKey {

        if (eventType == null || eventType.isBlank()) {
            throw new IllegalArgumentException(
                    "eventType is required"
            );
        }

        if (eventVersion <= 0) {
            throw new IllegalArgumentException(
                    "eventVersion must be greater than zero"
            );
        }
    }
}