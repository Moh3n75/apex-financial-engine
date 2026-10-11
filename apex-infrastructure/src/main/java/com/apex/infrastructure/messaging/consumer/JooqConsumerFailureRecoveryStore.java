package com.apex.infrastructure.messaging.consumer;

import com.apex.platform.messaging.consumer.ConsumerFailureRecoveryStore;
import com.apex.platform.messaging.consumer.ConsumerFailureStatus;

import org.jooq.DSLContext;
import org.springframework.stereotype.Repository;

import java.time.Clock;
import java.time.OffsetDateTime;
import java.util.UUID;

import static com.apex.infrastructure.jooq.generated.tables.ConsumerEventFailure.CONSUMER_EVENT_FAILURE;

@Repository
public class JooqConsumerFailureRecoveryStore
        implements ConsumerFailureRecoveryStore {

    private final DSLContext dsl;
    private final Clock clock;

    public JooqConsumerFailureRecoveryStore(
            DSLContext dsl,
            Clock clock
    ) {

        this.dsl = dsl;
        this.clock = clock;
    }

    @Override
    public void markRecovered(
            UUID eventId,
            String consumerGroup
    ) {

        if (eventId == null) {
            return;
        }

        dsl.update(
                        CONSUMER_EVENT_FAILURE
                )
                .set(
                        CONSUMER_EVENT_FAILURE.STATUS,
                        ConsumerFailureStatus.RECOVERED.name()
                )
                .set(
                        CONSUMER_EVENT_FAILURE.RECOVERED_AT,
                        OffsetDateTime.now(clock)
                )
                .where(
                        CONSUMER_EVENT_FAILURE.CONSUMER_GROUP
                                .eq(consumerGroup)
                )
                .and(
                        CONSUMER_EVENT_FAILURE.EVENT_ID
                                .eq(eventId)
                )
                .and(
                        CONSUMER_EVENT_FAILURE.STATUS.in(
                                ConsumerFailureStatus.RETRYING.name(),
                                ConsumerFailureStatus.DEAD_LETTERED.name(),
                                ConsumerFailureStatus.RECOVERY_FAILED.name(),
                                ConsumerFailureStatus.REPLAYING.name(),
                                ConsumerFailureStatus.REPLAY_FAILED.name()
                        )
                )
                .execute();
    }
}