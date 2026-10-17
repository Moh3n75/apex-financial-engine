package com.apex.credit.application.saga;


public interface SagaTimeoutPolicyResolver {


    SagaTimeoutPolicy resolve(
            SagaInstance saga
    );

}