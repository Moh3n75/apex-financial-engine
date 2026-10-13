package com.apex.credit.application.saga;


import com.apex.platform.events.saga.SagaEvent;


public interface SagaEventRouter {


    void route(
            SagaEvent event
    );

}