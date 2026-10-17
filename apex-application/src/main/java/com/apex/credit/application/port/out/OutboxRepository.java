package com.apex.credit.application.port.out;

import com.apex.platform.events.EventEnvelope;
import com.apex.platform.events.IntegrationEvent;

public interface OutboxRepository {

    void append(
            EventEnvelope<? extends IntegrationEvent> event
    );
}