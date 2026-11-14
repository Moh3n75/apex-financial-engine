package com.apex.platform.messaging.outbox;

import com.apex.platform.events.EventEnvelope;
import com.apex.platform.events.IntegrationEvent;

/**
 * Cell-facing port: append an integration event to the local Outbox
 * inside the business transaction. Kafka publish happens asynchronously.
 */
public interface OutboxAppender {

    void append(EventEnvelope<? extends IntegrationEvent> event);
}
