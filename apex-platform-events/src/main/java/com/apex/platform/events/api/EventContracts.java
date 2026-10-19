
package com.apex.platform.events.api;

public final class EventContracts {

    private EventContracts() {
    }

    public static <T> EventContract<T> define(
            String eventType,
            int eventVersion,
            String aggregateType,
            Class<T> payloadType,
            String schemaId
    ) {
        return new EventContract<>(
                eventType,
                eventVersion,
                aggregateType,
                payloadType,
                schemaId
        );
    }
}
