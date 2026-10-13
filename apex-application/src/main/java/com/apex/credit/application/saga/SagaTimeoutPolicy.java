package com.apex.credit.application.saga;


public enum SagaTimeoutPolicy {


    RETRY,

    COMPENSATE,

    FAIL,

    ALERT

}