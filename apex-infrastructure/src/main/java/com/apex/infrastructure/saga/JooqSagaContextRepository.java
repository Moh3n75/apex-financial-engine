package com.apex.infrastructure.saga;


import com.apex.credit.application.saga.SagaContextRepository;
import com.apex.credit.application.saga.SagaExecutionContext;
import org.jooq.DSLContext;
import org.jooq.JSONB;
import org.springframework.stereotype.Repository;
import tools.jackson.databind.json.JsonMapper;


import java.time.OffsetDateTime;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;


import static com.apex.infrastructure.jooq.generated.Tables.SAGA_CONTEXT;



@Repository
public class JooqSagaContextRepository
        implements SagaContextRepository {


    private final DSLContext dsl;

    private final JsonMapper jsonMapper;


    public JooqSagaContextRepository(
            DSLContext dsl,
            JsonMapper jsonMapper
    ){

        this.dsl = dsl;
        this.jsonMapper = jsonMapper;

    }



    @Override
    public void save(
            SagaExecutionContext context
    ){

        String json;

        try {

            json =
                    jsonMapper.writeValueAsString(
                            context.data()
                    );

        }
        catch (Exception e){

            throw new IllegalStateException(
                    "Failed to serialize saga context",
                    e
            );

        }



        dsl.insertInto(
                        SAGA_CONTEXT
                )

                .set(
                        SAGA_CONTEXT.ID,
                        UUID.randomUUID()
                )

                .set(
                        SAGA_CONTEXT.SAGA_ID,
                        context.sagaId()
                )

                .set(
                        SAGA_CONTEXT.CONTEXT_DATA,
                        JSONB.jsonb(json)
                )

                .set(
                        SAGA_CONTEXT.CREATED_AT,
                        OffsetDateTime.now()
                )

                .set(
                        SAGA_CONTEXT.UPDATED_AT,
                        OffsetDateTime.now()
                )

                .onDuplicateKeyUpdate()

                .set(
                        SAGA_CONTEXT.CONTEXT_DATA,
                        JSONB.jsonb(json)
                )

                .set(
                        SAGA_CONTEXT.UPDATED_AT,
                        OffsetDateTime.now()
                )

                .execute();

    }



    @Override
    public Optional<SagaExecutionContext> findBySagaId(
            UUID sagaId
    ){

        var record =
                dsl.selectFrom(
                                SAGA_CONTEXT
                        )

                        .where(
                                SAGA_CONTEXT.SAGA_ID.eq(
                                        sagaId
                                )
                        )

                        .fetchOne();



        if(record == null){

            return Optional.empty();

        }



        Map<String,Object> data;

        try {

            data =
                    jsonMapper.readValue(
                            record.getContextData().data(),
                            Map.class
                    );

        }
        catch(Exception e){

            throw new IllegalStateException(
                    "Failed to deserialize saga context",
                    e
            );

        }



        SagaExecutionContext context =
                new SagaExecutionContext(
                        sagaId
                );


        data.forEach(
                context::put
        );


        return Optional.of(context);

    }

}