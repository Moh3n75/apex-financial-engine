package com.apex.infrastructure.saga;


import com.apex.platform.events.saga.SagaEvent;

import java.time.Instant;
import java.util.UUID;


public class SagaEventOutboxMapper {


    public OutboxMessage map(
            SagaEvent event
    ){

        return new OutboxMessage(

                event.sagaId(),

                event.getClass()
                        .getSimpleName(),

                event.correlationId(),

                event.occurredAt()

        );

    }



    public record OutboxMessage(

            UUID aggregateId,

            String eventType,

            UUID correlationId,

            Instant occurredAt

    ){}

}