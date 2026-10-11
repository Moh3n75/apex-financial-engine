package com.apex.credit.application.saga;

public enum SagaStepStatus {

    PENDING,

    RUNNING,

    COMPLETED,

    FAILED,

    COMPENSATED

}