package com.apex.platform.messaging.consumer;

import java.util.Optional;

public interface EventHandlerRegistry {
    /** Sanitized registered keys; no handler instances or payload classes. */
    default java.util.List<EventHandlerKey> descriptors() {
        throw new UnsupportedOperationException("Registry does not expose contract descriptors");
    }

    Optional<EventHandler<?>> find(
            String eventType,
            int eventVersion
    );
}
