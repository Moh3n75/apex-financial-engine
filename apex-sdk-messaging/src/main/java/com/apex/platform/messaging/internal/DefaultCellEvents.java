
package com.apex.platform.messaging.internal;

import com.apex.platform.core.context.ExecutionContext;
import com.apex.platform.events.EventEnvelope;
import com.apex.platform.events.EventEnvelopeFactory;
import com.apex.platform.events.IntegrationEvent;
import com.apex.platform.events.api.CellEvents;
import com.apex.platform.events.api.EventContract;
import com.apex.platform.messaging.outbox.OutboxAppender;
import org.springframework.transaction.support.TransactionSynchronizationManager;

import java.util.Objects;
import java.util.UUID;
import java.util.function.Supplier;

public final class DefaultCellEvents implements CellEvents {

    private final EventEnvelopeFactory envelopeFactory;
    private final OutboxAppender outboxAppender;
    private final Supplier<ExecutionContext> contextProvider;

    public DefaultCellEvents(
            EventEnvelopeFactory envelopeFactory,
            OutboxAppender outboxAppender,
            Supplier<ExecutionContext> contextProvider
    ) {
        this.envelopeFactory = Objects.requireNonNull(envelopeFactory);
        this.outboxAppender = Objects.requireNonNull(outboxAppender);
        this.contextProvider = Objects.requireNonNull(contextProvider);
    }

    @Override
    public <T extends IntegrationEvent> void append(
            EventContract<T> contract,
            UUID aggregateId,
            T payload
    ) {
        Objects.requireNonNull(contract, "contract");
        Objects.requireNonNull(aggregateId, "aggregateId");
        Objects.requireNonNull(payload, "payload");

        if (!TransactionSynchronizationManager
                .isActualTransactionActive()) {
            throw new IllegalStateException(
                    "Event append requires an active business transaction"
            );
        }

        if (!contract.payloadType().isInstance(payload)) {
            throw new IllegalArgumentException(
                    "Payload does not match event contract"
            );
        }

        ExecutionContext context = Objects.requireNonNull(
                contextProvider.get(),
                "Execution context is required"
        );

        EventEnvelope<T> envelope = envelopeFactory.create(
                contract.eventType(),
                contract.eventVersion(),
                aggregateId.toString(),
                contract.aggregateType(),
                context,
                payload
        );

        outboxAppender.append(envelope);
    }
}
