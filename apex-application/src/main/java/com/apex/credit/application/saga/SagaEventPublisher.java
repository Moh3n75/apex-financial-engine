package com.apex.credit.application.saga;


import com.apex.platform.events.saga.SagaEvent;


public interface SagaEventPublisher {


    void publish(
            SagaEvent event
    );


}