package com.apex.apexbootstrap.saga;


import com.apex.credit.application.saga.SagaRecoveryService;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;


@Component
public class SagaRecoveryScheduler {


    private final SagaRecoveryService recoveryService;


    public SagaRecoveryScheduler(
            SagaRecoveryService recoveryService
    ){

        this.recoveryService = recoveryService;

    }



    @Scheduled(
            fixedDelayString = "PT5M"
    )
    public void recover(){

        recoveryService.recover();

    }

}