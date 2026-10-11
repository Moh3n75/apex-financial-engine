package com.apex.credit.application.saga;


public class SagaStepExecutor {


    private final SagaStepService stepService;



    public SagaStepExecutor(
            SagaStepService stepService
    ){

        this.stepService = stepService;

    }



    public SagaStep execute(
            SagaStep step,
            SagaAction action
    ){

        try {

            action.execute();


            return stepService.completeStep(
                    step
            );


        } catch (Exception ex) {


            return stepService.failStep(
                    step,
                    ex.getMessage()
            );

        }

    }


}