
package com.apex.platform.messaging.jdbc;

import com.apex.platform.core.context.ExecutionContext;
import com.apex.platform.core.context.ExecutionContextProvider;
import com.apex.platform.events.EventEnvelope;
import com.apex.platform.messaging.consumer.ConsumerContext;
import com.apex.platform.messaging.consumer.DispatchResult;
import com.apex.platform.messaging.consumer.EventHandler;
import com.apex.platform.messaging.consumer.EventHandlerExecutor;
import com.apex.platform.messaging.consumer.ProcessedEventStore;
import org.springframework.transaction.annotation.Transactional;

import java.util.Objects;

public class SimpleTransactionalEventHandlerExecutor
        implements EventHandlerExecutor {

    private final ProcessedEventStore processedEventStore;
    private final ExecutionContextProvider contextProvider;
    private final String sourceService;
    private final String cellId;

    public SimpleTransactionalEventHandlerExecutor(
            ProcessedEventStore processedEventStore,
            ExecutionContextProvider contextProvider,
            String sourceService,
            String cellId
    ) {
        this.processedEventStore =
                Objects.requireNonNull(processedEventStore);

        this.contextProvider =
                Objects.requireNonNull(contextProvider);

        this.sourceService = requireText(sourceService, "sourceService");
        this.cellId = requireText(cellId, "cellId");
    }

    @Override
    @Transactional
    public <T> DispatchResult execute(
            EventHandler<T> handler,
            EventEnvelope<T> event,
            ConsumerContext context
    ) {
        boolean firstProcessing = processedEventStore.registerIfAbsent(
                event.metadata(),
                context
        );

        if (!firstProcessing) {
            return DispatchResult.DUPLICATE;
        }

        ExecutionContext executionContext = new ExecutionContext(
                event.metadata().correlationId(),
                event.metadata().eventId(),
                sourceService,
                cellId
        );

        try (ExecutionContextProvider.Scope scope =
                     contextProvider.open(executionContext)) {

            handler.handle(event, context);
        }

        return DispatchResult.HANDLED;
    }

    private static String requireText(String value, String name) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(name + " must not be blank");
        }

        return value;
    }
}
