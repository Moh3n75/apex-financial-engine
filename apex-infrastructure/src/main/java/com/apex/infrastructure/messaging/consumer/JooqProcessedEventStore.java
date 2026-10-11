package com.apex.infrastructure.messaging.consumer;

import com.apex.platform.events.EventMetadata;
import com.apex.platform.messaging.consumer.ConsumerContext;
import com.apex.platform.messaging.consumer.ProcessedEventStore;

import org.jooq.DSLContext;
import org.springframework.stereotype.Repository;

import java.time.Clock;
import java.time.OffsetDateTime;

import static com.apex.infrastructure.jooq.generated.Tables.PROCESSED_EVENT;

@Repository
public class JooqProcessedEventStore
        implements ProcessedEventStore {

    private final DSLContext dsl;

    private final Clock clock;

    public JooqProcessedEventStore(
            DSLContext dsl,
            Clock clock
    ) {

        this.dsl = dsl;
        this.clock = clock;
    }

    @Override
    public boolean registerIfAbsent(
            EventMetadata metadata,
            ConsumerContext context
    ) {

        int inserted =
                dsl
                        .insertInto(PROCESSED_EVENT)

                        .set(
                                PROCESSED_EVENT.CONSUMER_GROUP,
                                context.consumerGroup()
                        )

                        .set(
                                PROCESSED_EVENT.EVENT_ID,
                                metadata.eventId()
                        )

                        .set(
                                PROCESSED_EVENT.EVENT_TYPE,
                                metadata.eventType()
                        )

                        .set(
                                PROCESSED_EVENT.EVENT_VERSION,
                                metadata.eventVersion()
                        )

                        .set(
                                PROCESSED_EVENT.TOPIC,
                                context.topic()
                        )

                        .set(
                                PROCESSED_EVENT.PARTITION_ID,
                                context.partition()
                        )

                        .set(
                                PROCESSED_EVENT.OFFSET_VALUE,
                                context.offset()
                        )

                        .set(
                                PROCESSED_EVENT.PROCESSED_AT,
                                OffsetDateTime.now(clock)
                        )

                        .onConflict(
                                PROCESSED_EVENT.CONSUMER_GROUP,
                                PROCESSED_EVENT.EVENT_ID
                        )

                        .doNothing()

                        .execute();

        return inserted == 1;
    }
}