package com.apex.credit.application.saga;

public enum SagaStatus {

    STARTED,

    RUNNING,

    COMPLETED,

    FAILED,

    COMPENSATING,

    COMPENSATED

}