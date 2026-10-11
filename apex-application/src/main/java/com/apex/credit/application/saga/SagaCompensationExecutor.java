package com.apex.credit.application.saga;


public class SagaCompensationExecutor {


    public void compensate(
            SagaStep failedStep,
            CompensationAction action
    ){


        try {

            action.compensate();


        } catch (Exception ex) {


            throw new IllegalStateException(
                    "Compensation failed for step: "
                            + failedStep.stepName(),
                    ex
            );

        }

    }

}