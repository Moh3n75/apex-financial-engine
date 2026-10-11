package com.apex.credit.application.saga;


public class SagaOrchestrator {


    private final SagaLifecycleService lifecycleService;

    private final SagaStepService stepService;



    public SagaOrchestrator(
            SagaLifecycleService lifecycleService,
            SagaStepService stepService
    ){

        this.lifecycleService = lifecycleService;
        this.stepService = stepService;

    }



    public SagaInstance start(
            String sagaType,
            String correlationId
    ){

        return lifecycleService.start(
                sagaType,
                correlationId
        );

    }



    public SagaStep executeStep(
            SagaInstance saga,
            String stepName
    ){

        return stepService.startStep(
                saga.id(),
                stepName
        );

    }
}