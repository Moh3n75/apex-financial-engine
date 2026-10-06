package com.apex.infrastructure.messaging.consumer;

import com.apex.platform.events.EventEnvelope;

import com.apex.platform.messaging.consumer.ConsumerContext;
import com.apex.platform.messaging.consumer.DispatchResult;
import com.apex.platform.messaging.consumer.EventHandler;
import com.apex.platform.messaging.consumer.EventHandlerExecutor;
import com.apex.platform.messaging.consumer.ProcessedEventStore;

import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

@Component
public class TransactionalEventHandlerExecutor
        implements EventHandlerExecutor {

    private final ProcessedEventStore processedEventStore;

    public TransactionalEventHandlerExecutor(
            ProcessedEventStore processedEventStore
    ) {

        this.processedEventStore =
                processedEventStore;
    }

    @Override
    @Transactional
    public <T> DispatchResult execute(
            EventHandler<T> handler,
            EventEnvelope<T> event,
            ConsumerContext context
    ) {

        boolean firstProcessing =
                processedEventStore.registerIfAbsent(
                        event.metadata(),
                        context
                );

        if (!firstProcessing) {

            return DispatchResult.DUPLICATE;
        }

        handler.handle(
                event,
                context
        );

        return DispatchResult.HANDLED;
    }
}