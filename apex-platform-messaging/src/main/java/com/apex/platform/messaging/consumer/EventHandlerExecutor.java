package com.apex.platform.messaging.consumer;

import com.apex.platform.events.EventEnvelope;

public interface EventHandlerExecutor {

    <T> DispatchResult execute(
            EventHandler<T> handler,
            EventEnvelope<T> event,
            ConsumerContext context
    );
}