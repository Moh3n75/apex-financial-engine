package com.apex.infrastructure.saga;


import com.apex.credit.application.saga.*;
import com.apex.infrastructure.jooq.generated.tables.records.SagaInstanceRecord;
import com.apex.infrastructure.jooq.generated.tables.records.SagaStepRecord;


import org.jooq.DSLContext;
import org.springframework.stereotype.Repository;


import java.time.Clock;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.Optional;
import java.util.UUID;


import static com.apex.infrastructure.jooq.generated.tables.SagaInstance.SAGA_INSTANCE;
import static com.apex.infrastructure.jooq.generated.tables.SagaStep.SAGA_STEP;


@Repository
public class JooqSagaRepository
        implements SagaRepository {


    private final DSLContext dsl;
    private final Clock clock;


    public JooqSagaRepository(
            DSLContext dsl,
            Clock clock) {
        this.dsl = dsl;
        this.clock = clock;
    }


    @Override
    public void save(
            SagaInstance saga
    ) {

        dsl.insertInto(SAGA_INSTANCE)

                .set(
                        SAGA_INSTANCE.ID,
                        saga.id()
                )

                .set(
                        SAGA_INSTANCE.SAGA_TYPE,
                        saga.sagaType()
                )

                .set(
                        SAGA_INSTANCE.CORRELATION_ID,
                        saga.correlationId()
                )

                .set(
                        SAGA_INSTANCE.STATUS,
                        saga.status().name()
                )

                .set(
                        SAGA_INSTANCE.STARTED_AT,
                        saga.startedAt() != null
                                ?
                                saga.startedAt()
                                        .atZone(clock.getZone())
                                        .toOffsetDateTime()
                                :
                                null
                )

                .set(
                        SAGA_INSTANCE.COMPLETED_AT,
                        saga.completedAt() != null
                                ?
                                saga.completedAt()
                                        .atZone(clock.getZone())
                                        .toOffsetDateTime()
                                :
                                null
                )

                .execute();

    }


    @Override
    public Optional<SagaInstance> findById(
            UUID id
    ) {

        SagaInstanceRecord record =
                dsl.selectFrom(SAGA_INSTANCE)

                        .where(
                                SAGA_INSTANCE.ID.eq(id)
                        )

                        .fetchOne();


        if (record == null) {
            return Optional.empty();
        }


        return Optional.of(
                map(record)
        );

    }

    @Override
    public void update(
            SagaInstance saga
    ){

        dsl.update(SAGA_INSTANCE)

                .set(
                        SAGA_INSTANCE.STATUS,
                        saga.status().name()
                )

                .set(
                        SAGA_INSTANCE.COMPLETED_AT,
                        saga.completedAt() != null
                                ?
                                saga.completedAt()
                                        .atZone(clock.getZone())
                                        .toOffsetDateTime()
                                :
                                null
                )

                .where(
                        SAGA_INSTANCE.ID.eq(
                                saga.id()
                        )
                )

                .execute();

    }

    @Override
    public void saveStep(
            SagaStep step
    ){

        dsl.insertInto(SAGA_STEP)

                .set(
                        SAGA_STEP.ID,
                        step.id()
                )

                .set(
                        SAGA_STEP.SAGA_ID,
                        step.sagaId()
                )

                .set(
                        SAGA_STEP.STEP_NAME,
                        step.stepName()
                )

                .set(
                        SAGA_STEP.STATUS,
                        step.status().name()
                )

                .set(
                        SAGA_STEP.STARTED_AT,
                        step.startedAt() != null
                                ?
                                step.startedAt()
                                        .atZone(clock.getZone())
                                        .toOffsetDateTime()
                                :
                                null
                )

                .set(
                        SAGA_STEP.COMPLETED_AT,
                        step.completedAt() != null
                                ?
                                step.completedAt()
                                        .atZone(clock.getZone())
                                        .toOffsetDateTime()
                                :
                                null
                )

                .set(
                        SAGA_STEP.ERROR,
                        step.error()
                )

                .execute();

    }

    @Override
    public Optional<SagaStep> findStepById(
            UUID id
    ){

        SagaStepRecord record =
                dsl.selectFrom(SAGA_STEP)

                        .where(
                                SAGA_STEP.ID.eq(id)
                        )

                        .fetchOne();



        if(record == null){

            return Optional.empty();

        }


        return Optional.of(
                mapStep(record)
        );

    }

    private SagaStep mapStep(
            SagaStepRecord record
    ){

        return new SagaStep(

                record.getId(),

                record.getSagaId(),

                record.getStepName(),

                SagaStepStatus.valueOf(
                        record.getStatus()
                ),

                record.getStartedAt() != null
                        ?
                        record.getStartedAt()
                                .toInstant()
                        :
                        null,

                record.getCompletedAt() != null
                        ?
                        record.getCompletedAt()
                                .toInstant()
                        :
                        null,

                record.getError()

        );

    }

    private SagaInstance map(
            SagaInstanceRecord record
    ) {

        return new SagaInstance(

                record.getId(),

                record.getSagaType(),

                record.getCorrelationId(),

                SagaStatus.valueOf(
                        record.getStatus()
                ),

                record.getStartedAt().toInstant(),

                record.getCompletedAt() != null
                        ?
                        record.getCompletedAt().toInstant()
                        :
                        null

        );

    }


}