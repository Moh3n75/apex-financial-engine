package com.apex.platform.messaging.consumer;

import com.apex.platform.events.EventEnvelope;

public interface EventHandler<T> {

    String eventType();

    int eventVersion();

    Class<T> payloadType();

    void handle(
            EventEnvelope<T> event,
            ConsumerContext context
    );

    default EventHandlerKey key() {

        return new EventHandlerKey(
                eventType(),
                eventVersion()
        );
    }
}