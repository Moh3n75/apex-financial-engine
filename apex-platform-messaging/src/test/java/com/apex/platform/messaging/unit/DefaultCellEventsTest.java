
package com.apex.platform.messaging.unit;

import com.apex.platform.core.context.ExecutionContext;
import com.apex.platform.core.context.ThreadLocalExecutionContextProvider;
import com.apex.platform.events.EventEnvelope;
import com.apex.platform.events.EventEnvelopeFactory;
import com.apex.platform.events.IntegrationEvent;
import com.apex.platform.events.api.EventContract;
import com.apex.platform.messaging.internal.DefaultCellEvents;
import com.apex.platform.messaging.outbox.OutboxAppender;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.springframework.transaction.support.TransactionSynchronizationManager;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

@Tag("unit")
class DefaultCellEventsTest {

    private final ThreadLocalExecutionContextProvider contextProvider =
            new ThreadLocalExecutionContextProvider();

    private final AtomicReference<EventEnvelope<? extends IntegrationEvent>>
            captured = new AtomicReference<>();

    private final OutboxAppender outboxAppender = captured::set;

    private final EventEnvelopeFactory envelopeFactory =
            new EventEnvelopeFactory(
                    Clock.fixed(
                            Instant.parse("2026-10-19T10:00:00Z"),
                            ZoneOffset.UTC
                    )
            );

    private final DefaultCellEvents cellEvents =
            new DefaultCellEvents(
                    envelopeFactory,
                    outboxAppender,
                    contextProvider::requireCurrent
            );

    private static final EventContract<TestEvent> CONTRACT =
            new EventContract<>(
                    "test.created",
                    1,
                    "test",
                    TestEvent.class,
                    "test.created.v1"
            );

    @AfterEach
    void cleanup() {
        TransactionSynchronizationManager.setActualTransactionActive(false);
    }

    @Test
    void shouldAppendEventWithCorrectMetadata() {
        UUID aggregateId = UUID.randomUUID();
        UUID correlationId = UUID.randomUUID();
        UUID causationId = UUID.randomUUID();

        ExecutionContext context = new ExecutionContext(
                correlationId,
                causationId,
                "customer-cell",
                "customer"
        );

        TransactionSynchronizationManager
                .setActualTransactionActive(true);

        try (var scope = contextProvider.open(context)) {
            cellEvents.append(
                    CONTRACT,
                    aggregateId,
                    new TestEvent("created")
            );
        }

        var envelope = captured.get();

        assertNotNull(envelope);
        assertEquals("test.created", envelope.metadata().eventType());
        assertEquals(1, envelope.metadata().eventVersion());
        assertEquals("test", envelope.metadata().aggregateType());
        assertEquals(aggregateId.toString(), envelope.metadata().aggregateId());

        assertEquals(correlationId, envelope.metadata().correlationId());
        assertEquals(causationId, envelope.metadata().causationId());
        assertEquals("customer-cell", envelope.metadata().sourceService());
        assertEquals("customer", envelope.metadata().cellId());
        assertEquals(
                Instant.parse("2026-10-19T10:00:00Z"),
                envelope.metadata().occurredAt()
        );

        assertNotNull(envelope.metadata().eventId());
        assertEquals(new TestEvent("created"), envelope.payload());
    }

    @Test
    void shouldRejectAppendWithoutTransaction() {
        ExecutionContext context =
                ExecutionContext.root("customer-cell", "customer");

        try (var scope = contextProvider.open(context)) {
            assertThrows(
                    IllegalStateException.class,
                    () -> cellEvents.append(
                            CONTRACT,
                            UUID.randomUUID(),
                            new TestEvent("created")
                    )
            );
        }

        assertNull(captured.get());
    }

    @Test
    void shouldRejectAppendWithoutExecutionContext() {
        TransactionSynchronizationManager
                .setActualTransactionActive(true);

        assertThrows(
                IllegalStateException.class,
                () -> cellEvents.append(
                        CONTRACT,
                        UUID.randomUUID(),
                        new TestEvent("created")
                )
        );

        assertNull(captured.get());
    }

    private record TestEvent(String value)
            implements IntegrationEvent {
    }
}
