package com.apex.credit.application.saga;


public interface SagaActionRegistry {


    SagaAction getAction(
            String eventType
    );


}