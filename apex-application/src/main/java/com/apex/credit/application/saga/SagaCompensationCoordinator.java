package com.apex.credit.application.saga;


import java.util.List;


public class SagaCompensationCoordinator {


    private final SagaCompensationExecutor executor;


    public SagaCompensationCoordinator(
            SagaCompensationExecutor executor
    ){

        this.executor = executor;

    }



    public void compensate(
            List<CompensationStep> steps
    ){


        steps.reversed()
                .forEach(step ->
                        executor.compensate(
                                step.sagaStep(),
                                step.action()
                        )
                );

    }



    public record CompensationStep(

            SagaStep sagaStep,

            CompensationAction action

    ){}

}