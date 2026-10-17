package com.apex.credit.application.saga;


public class DefaultSagaTimeoutPolicyResolver
        implements SagaTimeoutPolicyResolver {


    @Override
    public SagaTimeoutPolicy resolve(
            SagaInstance saga
    ){

        return SagaTimeoutPolicy.COMPENSATE;

    }

}