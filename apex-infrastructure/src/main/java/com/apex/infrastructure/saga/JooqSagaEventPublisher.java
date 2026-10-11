package com.apex.infrastructure.saga;


import com.apex.credit.application.saga.SagaEventPublisher;
import com.apex.platform.events.saga.SagaEvent;
import org.jooq.DSLContext;
import org.springframework.stereotype.Component;

import java.util.UUID;

import static com.apex.infrastructure.jooq.generated.Tables.OUTBOX_EVENT;


@Component
public class JooqSagaEventPublisher
        implements SagaEventPublisher {


    private final DSLContext dsl;

    private final SagaEventOutboxMapper mapper;


    public JooqSagaEventPublisher(
            DSLContext dsl,
            SagaEventOutboxMapper mapper
    ) {

        this.dsl = dsl;
        this.mapper = mapper;

    }



    @Override
    public void publish(
            SagaEvent event
    ){

        var message =
                mapper.map(event);



        dsl.insertInto(OUTBOX_EVENT)

                .set(
                        OUTBOX_EVENT.EVENT_ID,
                        UUID.randomUUID()
                )

                .set(
                        OUTBOX_EVENT.EVENT_TYPE,
                        message.eventType()
                )

                .set(
                        OUTBOX_EVENT.EVENT_VERSION,
                        1
                )

                .set(
                        OUTBOX_EVENT.CORRELATION_ID,
                        message.correlationId()
                )

                .set(
                        OUTBOX_EVENT.AGGREGATE_ID,
                        message.aggregateId()
                )

                .set(
                        OUTBOX_EVENT.AGGREGATE_TYPE,
                        "SAGA"
                )

                .set(
                        OUTBOX_EVENT.SOURCE_SERVICE,
                        "apex-financial-engine"
                )

                .set(
                        OUTBOX_EVENT.OCCURRED_AT,
                        message.occurredAt()
                                .atZone(
                                        java.time.ZoneId.systemDefault()
                                )
                                .toOffsetDateTime()
                )

                .set(
                        OUTBOX_EVENT.PAYLOAD,
                        org.jooq.JSONB.valueOf(
                                "{}"
                        )
                )

                .set(
                        OUTBOX_EVENT.PUBLISHED,
                        false
                )

                .set(
                        OUTBOX_EVENT.RETRY_COUNT,
                        0
                )

                .execute();

    }

}