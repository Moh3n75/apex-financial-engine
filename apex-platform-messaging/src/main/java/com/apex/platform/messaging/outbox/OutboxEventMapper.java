package com.apex.platform.messaging.outbox;

import com.apex.platform.events.EventEnvelope;

public interface OutboxEventMapper {

    EventEnvelope<?> map(
            OutboxRecord record
    );

}