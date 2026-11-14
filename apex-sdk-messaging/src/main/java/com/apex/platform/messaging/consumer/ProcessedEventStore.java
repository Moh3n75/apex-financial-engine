package com.apex.platform.messaging.consumer;

import com.apex.platform.events.EventMetadata;

public interface ProcessedEventStore {

    boolean registerIfAbsent(
            EventMetadata metadata,
            ConsumerContext context
    );
}