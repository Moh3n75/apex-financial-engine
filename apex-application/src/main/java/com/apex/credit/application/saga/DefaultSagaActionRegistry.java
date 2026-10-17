package com.apex.credit.application.saga;


import java.util.Map;


public class DefaultSagaActionRegistry
        implements SagaActionRegistry {


    private final Map<String, SagaAction> actions;



    public DefaultSagaActionRegistry(
            Map<String, SagaAction> actions
    ){

        this.actions = actions;

    }



    @Override
    public SagaAction getAction(
            String eventType
    ){

        return actions.get(eventType);

    }

}