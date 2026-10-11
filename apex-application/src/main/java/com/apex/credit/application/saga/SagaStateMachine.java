package com.apex.credit.application.saga;


public class SagaStateMachine {


    public SagaStatus transition(
            SagaStatus current,
            SagaStatus next
    ){


        if(!isAllowed(current, next)){

            throw new IllegalStateException(
                    "Invalid saga transition: "
                            + current
                            + " -> "
                            + next
            );

        }


        return next;

    }



    private boolean isAllowed(
            SagaStatus current,
            SagaStatus next
    ){


        return switch (current){

            case STARTED ->
                    next == SagaStatus.RUNNING
                            ||
                            next == SagaStatus.FAILED;


            case RUNNING ->
                    next == SagaStatus.COMPLETED
                            ||
                            next == SagaStatus.FAILED
                            ||
                            next == SagaStatus.COMPENSATING;


            case FAILED ->
                    next == SagaStatus.COMPENSATING;


            case COMPENSATING ->
                    next == SagaStatus.COMPENSATED
                            ||
                            next == SagaStatus.FAILED;


            case COMPLETED,
                 COMPENSATED ->
                    false;

        };

    }

}