package com.apex.infrastructure.messaging.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import org.jooq.DSLContext;

import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

import java.time.Clock;
import java.time.OffsetDateTime;
import java.util.UUID;

import static com.apex.infrastructure.jooq.generated.tables.ConsumerEventFailure.CONSUMER_EVENT_FAILURE;

@Service
public class ConsumerFailureAuditService {

    private static final String RETRYING =
            "RETRYING";

    private static final String DEAD_LETTERED =
            "DEAD_LETTERED";

    private static final String RECOVERY_FAILED =
            "RECOVERY_FAILED";


    private final DSLContext dsl;

    private final JsonMapper jsonMapper;

    private final Clock clock;

    private final String consumerGroup;


    public ConsumerFailureAuditService(
            DSLContext dsl,
            JsonMapper jsonMapper,
            Clock clock,
            org.springframework.core.env.Environment environment
    ) {

        this.dsl = dsl;
        this.jsonMapper = jsonMapper;
        this.clock = clock;

        this.consumerGroup =
                environment.getRequiredProperty(
                        "spring.kafka.consumer.group-id"
                );
    }


    @Transactional(
            propagation = Propagation.REQUIRES_NEW
    )
    public void recordFailure(
            ConsumerRecord<?, ?> record,
            Exception exception,
            int deliveryAttempt
    ) {

        EventIdentity identity =
                extractIdentity(record);

        Throwable rootCause =
                rootCause(exception);

        OffsetDateTime now =
                OffsetDateTime.now(clock);

        dsl.insertInto(
                        CONSUMER_EVENT_FAILURE
                )

                .set(
                        CONSUMER_EVENT_FAILURE.CONSUMER_GROUP,
                        consumerGroup
                )

                .set(
                        CONSUMER_EVENT_FAILURE.EVENT_ID,
                        identity.eventId()
                )

                .set(
                        CONSUMER_EVENT_FAILURE.EVENT_TYPE,
                        identity.eventType()
                )

                .set(
                        CONSUMER_EVENT_FAILURE.EVENT_VERSION,
                        identity.eventVersion()
                )

                .set(
                        CONSUMER_EVENT_FAILURE.TOPIC,
                        record.topic()
                )

                .set(
                        CONSUMER_EVENT_FAILURE.PARTITION_ID,
                        record.partition()
                )

                .set(
                        CONSUMER_EVENT_FAILURE.OFFSET_VALUE,
                        record.offset()
                )

                .set(
                        CONSUMER_EVENT_FAILURE.ATTEMPT_COUNT,
                        deliveryAttempt
                )

                .set(
                        CONSUMER_EVENT_FAILURE.ERROR_TYPE,
                        rootCause.getClass().getName()
                )

                .set(
                        CONSUMER_EVENT_FAILURE.LAST_ERROR,
                        errorMessage(rootCause)
                )

                .set(
                        CONSUMER_EVENT_FAILURE.STATUS,
                        RETRYING
                )

                .set(
                        CONSUMER_EVENT_FAILURE.FIRST_FAILED_AT,
                        now
                )

                .set(
                        CONSUMER_EVENT_FAILURE.LAST_FAILED_AT,
                        now
                )

                .onConflict(
                        CONSUMER_EVENT_FAILURE.CONSUMER_GROUP,
                        CONSUMER_EVENT_FAILURE.TOPIC,
                        CONSUMER_EVENT_FAILURE.PARTITION_ID,
                        CONSUMER_EVENT_FAILURE.OFFSET_VALUE
                )

                .doUpdate()

                .set(
                        CONSUMER_EVENT_FAILURE.ATTEMPT_COUNT,
                        deliveryAttempt
                )

                .set(
                        CONSUMER_EVENT_FAILURE.ERROR_TYPE,
                        rootCause.getClass().getName()
                )

                .set(
                        CONSUMER_EVENT_FAILURE.LAST_ERROR,
                        errorMessage(rootCause)
                )

                .set(
                        CONSUMER_EVENT_FAILURE.STATUS,
                        RETRYING
                )

                .set(
                        CONSUMER_EVENT_FAILURE.LAST_FAILED_AT,
                        now
                )

                .execute();
    }


    @Transactional(
            propagation = Propagation.REQUIRES_NEW
    )
    public void markDeadLettered(
            ConsumerRecord<?, ?> record,
            Exception exception
    ) {

        Throwable rootCause =
                rootCause(exception);

        OffsetDateTime now =
                OffsetDateTime.now(clock);

        dsl.update(
                        CONSUMER_EVENT_FAILURE
                )

                .set(
                        CONSUMER_EVENT_FAILURE.STATUS,
                        DEAD_LETTERED
                )

                .set(
                        CONSUMER_EVENT_FAILURE.ERROR_TYPE,
                        rootCause.getClass().getName()
                )

                .set(
                        CONSUMER_EVENT_FAILURE.LAST_ERROR,
                        errorMessage(rootCause)
                )

                .set(
                        CONSUMER_EVENT_FAILURE.LAST_FAILED_AT,
                        now
                )

                .set(
                        CONSUMER_EVENT_FAILURE.DEAD_LETTER_TOPIC,
                        record.topic() + "-dlt"
                )

                .set(
                        CONSUMER_EVENT_FAILURE.DEAD_LETTERED_AT,
                        now
                )

                .where(
                        CONSUMER_EVENT_FAILURE.CONSUMER_GROUP
                                .eq(consumerGroup)
                )

                .and(
                        CONSUMER_EVENT_FAILURE.TOPIC
                                .eq(record.topic())
                )

                .and(
                        CONSUMER_EVENT_FAILURE.PARTITION_ID
                                .eq(record.partition())
                )

                .and(
                        CONSUMER_EVENT_FAILURE.OFFSET_VALUE
                                .eq(record.offset())
                )

                .execute();
    }


    @Transactional(
            propagation = Propagation.REQUIRES_NEW
    )
    public void markRecoveryFailed(
            ConsumerRecord<?, ?> record,
            Exception recoveryException
    ) {

        Throwable rootCause =
                rootCause(recoveryException);

        dsl.update(
                        CONSUMER_EVENT_FAILURE
                )

                .set(
                        CONSUMER_EVENT_FAILURE.STATUS,
                        RECOVERY_FAILED
                )

                .set(
                        CONSUMER_EVENT_FAILURE.ERROR_TYPE,
                        rootCause.getClass().getName()
                )

                .set(
                        CONSUMER_EVENT_FAILURE.LAST_ERROR,
                        errorMessage(rootCause)
                )

                .set(
                        CONSUMER_EVENT_FAILURE.LAST_FAILED_AT,
                        OffsetDateTime.now(clock)
                )

                .where(
                        CONSUMER_EVENT_FAILURE.CONSUMER_GROUP
                                .eq(consumerGroup)
                )

                .and(
                        CONSUMER_EVENT_FAILURE.TOPIC
                                .eq(record.topic())
                )

                .and(
                        CONSUMER_EVENT_FAILURE.PARTITION_ID
                                .eq(record.partition())
                )

                .and(
                        CONSUMER_EVENT_FAILURE.OFFSET_VALUE
                                .eq(record.offset())
                )

                .execute();
    }


    private EventIdentity extractIdentity(
            ConsumerRecord<?, ?> record
    ) {

        if (!(record.value() instanceof String value)) {

            return EventIdentity.empty();
        }

        try {

            JsonNode root =
                    jsonMapper.readTree(value);

            JsonNode metadata =
                    root.get("metadata");

            if (metadata == null) {

                return EventIdentity.empty();
            }

            UUID eventId =
                    readUuid(
                            metadata.get("eventId")
                    );

            String eventType =
                    readText(
                            metadata.get("eventType")
                    );

            Integer eventVersion =
                    readInteger(
                            metadata.get("eventVersion")
                    );

            return new EventIdentity(
                    eventId,
                    eventType,
                    eventVersion
            );

        } catch (Exception ignored) {

            /*
             * Failure auditing must also work
             * for malformed events.
             */
            return EventIdentity.empty();
        }
    }


    private UUID readUuid(
            JsonNode node
    ) {

        if (node == null || node.isNull()) {
            return null;
        }

        try {
            return UUID.fromString(
                    node.asText()
            );
        } catch (Exception ignored) {
            return null;
        }
    }


    private String readText(
            JsonNode node
    ) {

        if (node == null || node.isNull()) {
            return null;
        }

        return node.asText();
    }


    private Integer readInteger(
            JsonNode node
    ) {

        if (node == null || node.isNull()) {
            return null;
        }

        return node.asInt();
    }


    private Throwable rootCause(
            Throwable throwable
    ) {

        Throwable current = throwable;

        while (
                current.getCause() != null
                        && current.getCause() != current
        ) {

            current = current.getCause();
        }

        return current;
    }


    private String errorMessage(
            Throwable throwable
    ) {

        if (throwable.getMessage() != null) {
            return throwable.getMessage();
        }

        return throwable.toString();
    }


    private record EventIdentity(
            UUID eventId,
            String eventType,
            Integer eventVersion
    ) {

        private static EventIdentity empty() {

            return new EventIdentity(
                    null,
                    null,
                    null
            );
        }
    }
}