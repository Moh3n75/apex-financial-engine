package com.apex.infrastructure.messaging.consumer;

import com.apex.platform.messaging.consumer.ConsumerFailureReplay;
import com.apex.platform.messaging.consumer.ConsumerFailureStatus;
import com.apex.platform.messaging.kafka.KafkaMessagePublisher;

import org.jooq.DSLContext;

import org.springframework.stereotype.Service;

import java.time.Clock;
import java.time.OffsetDateTime;

import static com.apex.infrastructure.jooq.generated.tables.ConsumerEventFailure.CONSUMER_EVENT_FAILURE;

@Service
public class ConsumerFailureReplayService
        implements ConsumerFailureReplay {

    private final DSLContext dsl;

    private final KafkaMessagePublisher kafkaPublisher;

    private final Clock clock;


    public ConsumerFailureReplayService(
            DSLContext dsl,
            KafkaMessagePublisher kafkaPublisher,
            Clock clock
    ) {

        this.dsl = dsl;
        this.kafkaPublisher = kafkaPublisher;
        this.clock = clock;
    }


    @Override
    public void replay(
            long failureId
    ) {

        var failure =
                dsl.selectFrom(
                                CONSUMER_EVENT_FAILURE
                        )
                        .where(
                                CONSUMER_EVENT_FAILURE.ID
                                        .eq(failureId)
                        )
                        .fetchOne();

        if (failure == null) {

            throw new IllegalArgumentException(
                    "Consumer failure not found: "
                            + failureId
            );
        }


        String status =
                failure.getStatus();


        boolean replayable =
                ConsumerFailureStatus.DEAD_LETTERED
                        .name()
                        .equals(status)

                        ||

                        ConsumerFailureStatus.REPLAY_FAILED
                                .name()
                                .equals(status);


        if (!replayable) {

            throw new IllegalStateException(
                    "Consumer failure cannot be replayed from status: "
                            + status
            );
        }


        String payload =
                failure.getOriginalPayload();


        if (payload == null) {

            throw new IllegalStateException(
                    "Original event payload is not available"
            );
        }


        OffsetDateTime now =
                OffsetDateTime.now(clock);


        /*
         * Atomic claim.
         *
         * Prevent two operators/nodes from replaying
         * the same failure simultaneously.
         */
        int claimed =
                dsl.update(
                                CONSUMER_EVENT_FAILURE
                        )
                        .set(
                                CONSUMER_EVENT_FAILURE.STATUS,
                                ConsumerFailureStatus.REPLAYING.name()
                        )
                        .set(
                                CONSUMER_EVENT_FAILURE.REPLAY_REQUESTED_AT,
                                now
                        )
                        .set(
                                CONSUMER_EVENT_FAILURE.LAST_REPLAY_ERROR,
                                (String) null
                        )
                        .where(
                                CONSUMER_EVENT_FAILURE.ID
                                        .eq(failureId)
                        )
                        .and(
                                CONSUMER_EVENT_FAILURE.STATUS
                                        .eq(status)
                        )
                        .execute();


        if (claimed != 1) {

            throw new IllegalStateException(
                    "Consumer failure is already being replayed"
            );
        }


        try {

            kafkaPublisher.publish(
                            failure.getTopic(),
                            failure.getMessageKey(),
                            payload
                    )
                    .toCompletableFuture()
                    .join();


            dsl.update(
                            CONSUMER_EVENT_FAILURE
                    )
                    .set(
                            CONSUMER_EVENT_FAILURE.REPLAY_COUNT,
                            CONSUMER_EVENT_FAILURE.REPLAY_COUNT.plus(1)
                    )
                    .set(
                            CONSUMER_EVENT_FAILURE.LAST_REPLAYED_AT,
                            OffsetDateTime.now(clock)
                    )
                    .where(
                            CONSUMER_EVENT_FAILURE.ID
                                    .eq(failureId)
                    )
                    .execute();


        } catch (Exception exception) {

            Throwable cause =
                    rootCause(exception);


            dsl.update(
                            CONSUMER_EVENT_FAILURE
                    )
                    .set(
                            CONSUMER_EVENT_FAILURE.STATUS,
                            ConsumerFailureStatus.REPLAY_FAILED.name()
                    )
                    .set(
                            CONSUMER_EVENT_FAILURE.LAST_REPLAY_ERROR,
                            cause.getMessage() != null
                                    ? cause.getMessage()
                                    : cause.toString()
                    )
                    .where(
                            CONSUMER_EVENT_FAILURE.ID
                                    .eq(failureId)
                    )
                    .execute();


            throw new IllegalStateException(
                    "Failed to replay consumer event",
                    cause
            );
        }
    }


    private Throwable rootCause(
            Throwable throwable
    ) {

        Throwable current =
                throwable;

        while (
                current.getCause() != null
                        && current.getCause() != current
        ) {

            current =
                    current.getCause();
        }

        return current;
    }
}