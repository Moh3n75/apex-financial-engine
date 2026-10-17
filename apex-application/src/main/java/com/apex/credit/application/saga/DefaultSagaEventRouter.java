package com.apex.credit.application.saga;


import com.apex.platform.events.saga.SagaEvent;

import java.util.Map;


public class DefaultSagaEventRouter
        implements SagaEventRouter {


    private final SagaActionRegistry registry;



    public DefaultSagaEventRouter(
            SagaActionRegistry registry
    ){

        this.registry = registry;

    }



    @Override
    public void route(
            SagaEvent event
    ){

        SagaAction action =
                registry.getAction(
                        event.getClass()
                                .getSimpleName()
                );


        if(action == null){

            throw new IllegalStateException(
                    "No saga action registered for event: "
                            + event.getClass().getSimpleName()
            );

        }


        action.execute();

    }

}