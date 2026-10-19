
package com.apex.platform.messaging.unit;

import com.apex.platform.core.context.ExecutionContext;
import com.apex.platform.core.context.ThreadLocalExecutionContextProvider;
import com.apex.platform.events.EventEnvelope;
import com.apex.platform.events.EventMetadata;
import com.apex.platform.messaging.consumer.*;
import com.apex.platform.messaging.jdbc.SimpleTransactionalEventHandlerExecutor;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

@Tag("unit")
class SimpleTransactionalEventHandlerExecutorTest {

    private final ThreadLocalExecutionContextProvider contextProvider =
            new ThreadLocalExecutionContextProvider();

    private final UUID eventId = UUID.randomUUID();
    private final UUID correlationId = UUID.randomUUID();

    private final EventEnvelope<String> envelope =
            new EventEnvelope<>(
                    new EventMetadata(
                            eventId,
                            "customer.created",
                            1,
                            correlationId,
                            null,
                            UUID.randomUUID().toString(),
                            "customer",
                            "customer-cell",
                            "customer-cell",
                            Instant.now()
                    ),
                    "sample-payload"
            );

    private final ConsumerContext consumerContext =
            new ConsumerContext(
                    "reporting-group",
                    "customer-events",
                    0,
                    10L,
                    "customer-key",
                    Instant.now()
            );

    @Test
    void shouldPreserveCorrelationId() {
        AtomicReference<ExecutionContext> captured =
                new AtomicReference<>();

        executeWithStore(true, event -> {
            captured.set(contextProvider.requireCurrent());
        });

        assertEquals(
                correlationId,
                captured.get().correlationId()
        );
    }

    @Test
    void shouldSetCausationToIncomingEventId() {
        AtomicReference<ExecutionContext> captured =
                new AtomicReference<>();

        executeWithStore(true, event -> {
            captured.set(contextProvider.requireCurrent());
        });

        assertEquals(
                eventId,
                captured.get().causationId()
        );
    }

    @Test
    void shouldUseConsumerCellIdentity() {
        AtomicReference<ExecutionContext> captured =
                new AtomicReference<>();

        executeWithStore(true, event -> {
            captured.set(contextProvider.requireCurrent());
        });

        assertEquals(
                "reporting-cell",
                captured.get().sourceService()
        );

        assertEquals(
                "reporting",
                captured.get().cellId()
        );
    }

    @Test
    void shouldCleanContextAfterHandlerException() {
        assertThrows(
                IllegalStateException.class,
                () -> executeWithStore(true, event -> {
                    assertNotNull(contextProvider.requireCurrent());
                    throw new IllegalStateException("business failure");
                })
        );

        assertThrows(
                IllegalStateException.class,
                contextProvider::requireCurrent
        );
    }

    @Test
    void shouldSkipDuplicateEvent() {
        AtomicInteger invocations = new AtomicInteger();

        DispatchResult result = executeWithStore(
                false,
                event -> invocations.incrementAndGet()
        );

        assertEquals(DispatchResult.DUPLICATE, result);
        assertEquals(0, invocations.get());

        assertThrows(
                IllegalStateException.class,
                contextProvider::requireCurrent
        );
    }

    private DispatchResult executeWithStore(
            boolean firstProcessing,
            java.util.function.Consumer<EventEnvelope<String>> action
    ) {
        ProcessedEventStore store =
                (metadata, context) -> firstProcessing;

        var executor = new SimpleTransactionalEventHandlerExecutor(
                store,
                contextProvider,
                "reporting-cell",
                "reporting"
        );

        EventHandler<String> handler = new EventHandler<>() {

            @Override
            public String eventType() {
                return "customer.created";
            }

            @Override
            public int eventVersion() {
                return 1;
            }

            @Override
            public Class<String> payloadType() {
                return String.class;
            }

            @Override
            public void handle(
                    EventEnvelope<String> event,
                    ConsumerContext context
            ) {
                action.accept(event);
            }
        };

        return executor.execute(
                handler,
                envelope,
                consumerContext
        );
    }
}
