package com.apex.apexbootstrap.saga;


import com.apex.credit.application.saga.SagaRecoveryService;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;


@Component
public class SagaRecoveryStartupListener {


    private final SagaRecoveryService recoveryService;


    public SagaRecoveryStartupListener(
            SagaRecoveryService recoveryService
    ){

        this.recoveryService = recoveryService;

    }



    @EventListener(ApplicationReadyEvent.class)
    public void recover(){

        recoveryService.recover();

    }

}