package com.apex.credit.domain.model;

import com.apex.credit.domain.event.*;
import com.apex.credit.domain.exception.*;
import com.apex.credit.domain.valueobject.*;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;


public class CreditAccount {


    private final CreditAccountId id;


    private final MemberId memberId;


    private Money available;


    private Money blocked;


    private long version;


    private final List<DomainEvent> changes =
            new ArrayList<>();



    public CreditAccount(
            CreditAccountId id,
            MemberId memberId
    ){

        this.id = id;
        this.memberId = memberId;

        this.available =
                new Money(java.math.BigDecimal.ZERO);

        this.blocked =
                new Money(java.math.BigDecimal.ZERO);


        this.version = 0;


    }


    public void block(
            Money amount,
            String referenceId
    ){

        if(!available.greaterOrEqual(amount)){

            throw new InsufficientCreditException(
                    "Not enough credit"
            );

        }


        CreditBlockedEvent event =
                new CreditBlockedEvent(
                        UUID.randomUUID(),
                        id,
                        amount,
                        referenceId,
                        version + 1
                );


        apply(event);

        changes.add(event);

    }

    public void addCredit(
            Money amount
    ){

        CreditAddedEvent event =
                new CreditAddedEvent(
                        UUID.randomUUID(),
                        id,
                        amount,
                        version + 1
                );


        apply(event);

        changes.add(event);

    }



    private void apply(
            CreditBlockedEvent event
    ){

        available =
                available.subtract(
                        event.amount()
                );


        blocked =
                blocked.add(
                        event.amount()
                );


        version =
                event.version();

    }


    private void apply(
            CreditAddedEvent event
    ){

        available =
                available.add(
                        event.amount()
                );


        version =
                event.version();

    }

    public List<DomainEvent> getChanges(){

        return List.copyOf(changes);

    }


}