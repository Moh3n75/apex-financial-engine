package com.apex.credit.application.saga;


import java.util.List;


public class SagaResumeDecisionEngine {


    public SagaRecoveryDecision decide(
            List<SagaStep> steps
    ){


        return steps.stream()

                .filter(
                        step ->
                                step.status()
                                        == SagaStepStatus.RUNNING
                )

                .findFirst()

                .map(
                        step ->
                                new SagaRecoveryDecision(
                                        RecoveryAction.RESUME_STEP,
                                        step
                                )
                )

                .orElse(
                        new SagaRecoveryDecision(
                                RecoveryAction.COMPLETE,
                                null
                        )
                );

    }



    public enum RecoveryAction {

        RESUME_STEP,

        COMPENSATE,

        COMPLETE

    }



    public record SagaRecoveryDecision(

            RecoveryAction action,

            SagaStep step

    ){}

}