package com.apex.infrastructure.messaging.consumer;

import com.apex.platform.events.EventEnvelope;

import com.apex.platform.messaging.consumer.*;

import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

@Component
public class TransactionalEventHandlerExecutor
        implements EventHandlerExecutor {

    private final ProcessedEventStore processedEventStore;

    private final ConsumerFailureRecoveryStore failureRecoveryStore;

    private final ConsumerHandlerMetrics handlerMetrics;

    public TransactionalEventHandlerExecutor(
            ProcessedEventStore processedEventStore,
            ConsumerFailureRecoveryStore failureRecoveryStore,
            ConsumerHandlerMetrics handlerMetrics
    ) {

        this.processedEventStore =
                processedEventStore;

        this.failureRecoveryStore =
                failureRecoveryStore;

        this.handlerMetrics = handlerMetrics;
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

        return handlerMetrics.record(
                () -> {

                    handler.handle(
                            event,
                            context
                    );

                    failureRecoveryStore.markRecovered(
                            event.metadata().eventId(),
                            context.consumerGroup()
                    );

                    return DispatchResult.HANDLED;
                }
        );
    }
}