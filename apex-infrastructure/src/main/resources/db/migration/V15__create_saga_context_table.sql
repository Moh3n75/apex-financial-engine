CREATE TABLE saga_context
(
    id UUID PRIMARY KEY,

    saga_id UUID NOT NULL,

    context_data JSONB NOT NULL,

    created_at TIMESTAMP WITH TIME ZONE NOT NULL,

    updated_at TIMESTAMP WITH TIME ZONE NOT NULL,

    CONSTRAINT fk_saga_context_instance
        FOREIGN KEY (saga_id)
            REFERENCES saga_instance(id)
);