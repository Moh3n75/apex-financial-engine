package com.apex.apexbootstrap.saga;


import com.apex.credit.application.saga.SagaTimeoutService;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;


@Component
public class SagaTimeoutScheduler {


    private final SagaTimeoutService timeoutService;


    public SagaTimeoutScheduler(
            SagaTimeoutService timeoutService
    ){

        this.timeoutService = timeoutService;

    }



    @Scheduled(
            fixedDelayString = "PT5M"
    )
    public void detect(){

        timeoutService.detectTimeouts();

    }

}