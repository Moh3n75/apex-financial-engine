package com.apex.platform.messaging.consumer;

import java.util.Optional;

public interface EventHandlerRegistry {

    Optional<EventHandler<?>> find(
            String eventType,
            int eventVersion
    );
}