package com.apex.platform.messaging.consumer;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

public final class DefaultEventHandlerRegistry
        implements EventHandlerRegistry {

    private final Map<EventHandlerKey, EventHandler<?>> handlers;

    public DefaultEventHandlerRegistry(
            Collection<EventHandler<?>> handlers
    ) {

        Map<EventHandlerKey, EventHandler<?>> registry =
                new HashMap<>();

        for (EventHandler<?> handler : handlers) {

            EventHandlerKey key = handler.key();

            EventHandler<?> existing =
                    registry.putIfAbsent(
                            key,
                            handler
                    );

            if (existing != null) {

                throw new IllegalStateException(
                        "Duplicate event handler registered for "
                                + key.eventType()
                                + " version "
                                + key.eventVersion()
                );
            }
        }

        this.handlers = Map.copyOf(registry);
    }

    @Override
    public Optional<EventHandler<?>> find(
            String eventType,
            int eventVersion
    ) {

        return Optional.ofNullable(
                handlers.get(
                        new EventHandlerKey(
                                eventType,
                                eventVersion
                        )
                )
        );
    }
}