CREATE TABLE saga_instance
(
    id UUID PRIMARY KEY,

    saga_type VARCHAR(100) NOT NULL,

    correlation_id VARCHAR(100) NOT NULL,

    status VARCHAR(50) NOT NULL,

    started_at TIMESTAMP WITH TIME ZONE NOT NULL,

    completed_at TIMESTAMP WITH TIME ZONE
);

CREATE TABLE saga_step
(
    id UUID PRIMARY KEY,

    saga_id UUID NOT NULL,

    step_name VARCHAR(100) NOT NULL,

    status VARCHAR(50) NOT NULL,

    started_at TIMESTAMP WITH TIME ZONE,

    completed_at TIMESTAMP WITH TIME ZONE,

    error TEXT,


    CONSTRAINT fk_saga_step_instance

        FOREIGN KEY (saga_id)

            REFERENCES saga_instance(id)

);

CREATE INDEX idx_saga_instance_correlation_id
    ON saga_instance(correlation_id);

CREATE INDEX idx_saga_step_saga_id
    ON saga_step(saga_id);