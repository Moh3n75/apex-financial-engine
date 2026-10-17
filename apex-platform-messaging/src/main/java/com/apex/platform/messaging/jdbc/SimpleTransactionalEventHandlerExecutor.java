package com.apex.platform.messaging.jdbc;

import org.springframework.transaction.annotation.Transactional;

import com.apex.platform.events.EventEnvelope;
import com.apex.platform.messaging.consumer.ConsumerContext;
import com.apex.platform.messaging.consumer.DispatchResult;
import com.apex.platform.messaging.consumer.EventHandler;
import com.apex.platform.messaging.consumer.EventHandlerExecutor;
import com.apex.platform.messaging.consumer.ProcessedEventStore;

/**
 * Inbox claim + handler in one DB transaction. Kafka ACK happens only after this returns.
 */
public class SimpleTransactionalEventHandlerExecutor implements EventHandlerExecutor {

    private final ProcessedEventStore processedEventStore;

    public SimpleTransactionalEventHandlerExecutor(ProcessedEventStore processedEventStore) {
        this.processedEventStore = processedEventStore;
    }

    @Override
    @Transactional
    public <T> DispatchResult execute(
            EventHandler<T> handler,
            EventEnvelope<T> event,
            ConsumerContext context
    ) {
        boolean firstProcessing = processedEventStore.registerIfAbsent(event.metadata(), context);
        if (!firstProcessing) {
            return DispatchResult.DUPLICATE;
        }

        handler.handle(event, context);
        return DispatchResult.HANDLED;
    }
}
