package com.apex.credit.domain.model;

import com.apex.credit.domain.event.*;
import com.apex.credit.domain.exception.InsufficientCreditException;
import com.apex.credit.domain.valueobject.*;

import lombok.AccessLevel;
import lombok.Getter;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

@Getter
public final class CreditAccount {

    private CreditAccountId id;

    private MemberId memberId;

    private CreditTypeId creditTypeId;

    private CreditAmount available =
            CreditAmount.ZERO;

    private CreditAmount blocked =
            CreditAmount.ZERO;

    private long version;

    @Getter(AccessLevel.NONE)
    private final List<CreditAccountEvent>
            uncommittedEvents =
            new ArrayList<>();


    private CreditAccount() {
    }


    public static CreditAccount create(
            CreditAccountId id,
            MemberId memberId,
            CreditTypeId creditTypeId
    ) {

        CreditAccount account =
                new CreditAccount();

        account.raise(
                new CreditAccountCreatedEvent(
                        UUID.randomUUID(),
                        id,
                        memberId,
                        creditTypeId,
                        1
                )
        );

        return account;
    }


    public static CreditAccount restore(
            CreditAccountId id,
            MemberId memberId,
            CreditTypeId creditTypeId,
            CreditAmount available,
            CreditAmount blocked,
            long version
    ) {

        CreditAccount account =
                new CreditAccount();

        account.id = id;
        account.memberId = memberId;
        account.creditTypeId = creditTypeId;
        account.available = available;
        account.blocked = blocked;
        account.version = version;

        return account;
    }


    public void addCredit(
            CreditAmount amount,
            String referenceId
    ) {

        requirePositive(amount);

        raise(
                new CreditAddedEvent(
                        UUID.randomUUID(),
                        id,
                        amount,
                        referenceId,
                        version + 1
                )
        );
    }


    public void block(
            CreditAmount amount,
            String referenceId
    ) {

        requirePositive(amount);

        if (!available.greaterOrEqual(amount)) {

            throw new InsufficientCreditException(
                    "Not enough available credit"
            );
        }

        raise(
                new CreditBlockedEvent(
                        UUID.randomUUID(),
                        id,
                        amount,
                        referenceId,
                        version + 1
                )
        );
    }

    public void debit(
            CreditAmount amount,
            String referenceId
    ) {

        requirePositive(amount);

        if (!available.greaterOrEqual(amount)) {
            throw new InsufficientCreditException(
                    "Not enough available credit"
            );
        }

        raise(
                new CreditDebitedEvent(
                        UUID.randomUUID(),
                        id,
                        amount,
                        referenceId,
                        version + 1
                )
        );
    }


    private void requirePositive(
            CreditAmount amount
    ) {

        if (amount == null ||
                !amount.isPositive()) {

            throw new IllegalArgumentException(
                    "Amount must be greater than zero"
            );
        }
    }


    private void raise(
            CreditAccountEvent event
    ) {

        apply(event);

        uncommittedEvents.add(event);
    }


    private void apply(
            CreditAccountEvent event
    ) {

        switch (event) {

            case CreditAccountCreatedEvent e ->
                    on(e);

            case CreditAddedEvent e ->
                    on(e);

            case CreditBlockedEvent e ->
                    on(e);
            case CreditDebitedEvent e -> on(e);
        }
    }


    private void on(
            CreditAccountCreatedEvent event
    ) {

        this.id =
                event.aggregateId();

        this.memberId =
                event.memberId();

        this.creditTypeId =
                event.creditTypeId();

        this.available =
                CreditAmount.ZERO;

        this.blocked =
                CreditAmount.ZERO;

        this.version =
                event.version();
    }


    private void on(
            CreditAddedEvent event
    ) {

        this.available =
                available.add(
                        event.amount()
                );

        this.version =
                event.version();
    }


    private void on(
            CreditBlockedEvent event
    ) {

        this.available =
                available.subtract(
                        event.amount()
                );

        this.blocked =
                blocked.add(
                        event.amount()
                );

        this.version =
                event.version();
    }

    private void on(
            CreditDebitedEvent event
    ) {

        available =
                available.subtract(event.amount());

        version =
                event.version();
    }

    public List<CreditAccountEvent>
    uncommittedEvents() {

        return List.copyOf(
                uncommittedEvents
        );
    }


    public long persistedVersion() {

        return version -
                uncommittedEvents.size();
    }


    public void markChangesAsCommitted() {

        uncommittedEvents.clear();
    }
}