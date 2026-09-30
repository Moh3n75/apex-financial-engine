package com.apex.credit.domain.credit.hold.model;


import com.apex.credit.domain.credit.hold.event.CreditHoldCreatedEvent;
import com.apex.credit.domain.credit.hold.valueobject.HoldType;

import java.util.UUID;


public final class CreditHold {


    private UUID id;


    private long creditAccountId;


    private long transactionId;


    private long originalAmount;


    private long remainingAmount;


    private long consumedAmount;


    private HoldType holdType;


    private HoldStatus status;


    private long version;



    private CreditHold(){

    }



    public static CreditHold create(

            long creditAccountId,

            long transactionId,

            long amount,

            HoldType holdType

    ){

        if(amount <=0){
            throw new IllegalArgumentException();
        }


        CreditHold hold =
                new CreditHold();


        hold.id =
                UUID.randomUUID();


        hold.raise(
                new CreditHoldCreatedEvent(
                        UUID.randomUUID(),
                        hold.id,
                        creditAccountId,
                        transactionId,
                        amount,
                        holdType,
                        1
                )
        );


        return hold;
    }



    private void raise(
            CreditHoldCreatedEvent event
    ){

        apply(event);

    }



    private void apply(
            CreditHoldCreatedEvent event
    ){

        this.id =
                event.holdId();

        this.creditAccountId =
                event.creditAccountId();

        this.transactionId =
                event.transactionId();

        this.originalAmount =
                event.amount();

        this.remainingAmount =
                event.amount();

        this.consumedAmount =
                0;

        this.holdType =
                event.holdType();

        this.status =
                HoldStatus.ACTIVE;

        this.version =
                event.version();

    }

}