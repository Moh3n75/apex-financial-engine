package com.apex.credit.domain.credit.hold.model;


import com.apex.credit.domain.credit.hold.event.CreditHoldConsumedEvent;
import com.apex.credit.domain.credit.hold.event.CreditHoldCreatedEvent;
import com.apex.credit.domain.credit.hold.event.CreditHoldEvent;
import com.apex.credit.domain.credit.hold.valueobject.HoldType;
import lombok.Getter;

import java.util.UUID;

@Getter
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


        if(amount <= 0){

            throw new IllegalArgumentException(
                    "Amount must be greater than zero"
            );
        }


        CreditHold hold =
                new CreditHold();


        hold.raise(

                new CreditHoldCreatedEvent(

                        UUID.randomUUID(),

                        UUID.randomUUID(),

                        creditAccountId,

                        transactionId,

                        amount,

                        holdType,

                        1

                )

        );


        return hold;
    }



    public void consume(long amount){


        requirePositive(amount);


        if(amount > remainingAmount){

            throw new IllegalStateException(
                    "Cannot consume more than remaining amount"
            );
        }


        raise(

                new CreditHoldConsumedEvent(

                        UUID.randomUUID(),

                        id,

                        amount,

                        version + 1

                )

        );
    }



    private void raise(
            CreditHoldEvent event
    ){

        apply(event);

    }



    private void apply(
            CreditHoldEvent event
    ){

        switch(event){


            case CreditHoldCreatedEvent e ->

                    apply(e);



            case CreditHoldConsumedEvent e ->

                    apply(e);

            default -> throw new IllegalStateException("Unexpected value: " + event);
        }

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



    private void apply(
            CreditHoldConsumedEvent event
    ){


        this.remainingAmount -=
                event.amount();


        this.consumedAmount +=
                event.amount();



        if(this.remainingAmount == 0){

            this.status =
                    HoldStatus.CONSUMED;

        }



        this.version =
                event.version();

    }



    private void requirePositive(
            long amount
    ){

        if(amount <= 0){

            throw new IllegalArgumentException(
                    "Amount must be greater than zero"
            );
        }

    }



}