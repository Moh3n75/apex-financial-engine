package com.apex.credit.domain.transaction.model;

import com.apex.credit.domain.transaction.event.*;
import com.apex.credit.domain.transaction.valueobject.TransactionId;
import com.apex.credit.domain.transaction.valueobject.TransactionType;
import com.apex.credit.domain.valueobject.CreditAmount;

import lombok.AccessLevel;
import lombok.Getter;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

@Getter
public final class FinancialTransaction {

    private TransactionId id;

    private TransactionType type;

    private CreditAmount requestedAmount;

    private String referenceId;

    private TransactionStatus status;

    private String failureReason;

    private long version;

    @Getter(AccessLevel.NONE)
    private final List<FinancialTransactionEvent> uncommittedEvents =
            new ArrayList<>();


    private FinancialTransaction() {
    }


    public static FinancialTransaction create(
            TransactionType type,
            CreditAmount requestedAmount,
            String referenceId
    ) {

        if (type == null) {
            throw new IllegalArgumentException(
                    "Transaction type cannot be null"
            );
        }

        if (requestedAmount == null ||
                !requestedAmount.isPositive()) {

            throw new IllegalArgumentException(
                    "Transaction amount must be greater than zero"
            );
        }

        if (referenceId == null ||
                referenceId.isBlank()) {

            throw new IllegalArgumentException(
                    "Reference id cannot be empty"
            );
        }

        FinancialTransaction transaction =
                new FinancialTransaction();

        TransactionId transactionId =
                TransactionId.generate();

        transaction.raise(
                new TransactionCreatedEvent(
                        UUID.randomUUID(),
                        transactionId,
                        type,
                        requestedAmount,
                        referenceId,
                        1
                )
        );

        return transaction;
    }


    public static FinancialTransaction restore(
            TransactionId id,
            TransactionType type,
            CreditAmount requestedAmount,
            String referenceId,
            TransactionStatus status,
            String failureReason,
            long version
    ) {

        FinancialTransaction transaction =
                new FinancialTransaction();

        transaction.id = id;
        transaction.type = type;
        transaction.requestedAmount = requestedAmount;
        transaction.referenceId = referenceId;
        transaction.status = status;
        transaction.failureReason = failureReason;
        transaction.version = version;

        return transaction;
    }


    public void start() {

        if (status != TransactionStatus.CREATED) {
            throw new IllegalStateException(
                    "Only CREATED transaction can be started"
            );
        }

        raise(
                new TransactionStartedEvent(
                        UUID.randomUUID(),
                        id,
                        version + 1
                )
        );
    }


    public void complete() {

        if (status != TransactionStatus.PROCESSING) {
            throw new IllegalStateException(
                    "Only PROCESSING transaction can be completed"
            );
        }

        raise(
                new TransactionCompletedEvent(
                        UUID.randomUUID(),
                        id,
                        version + 1
                )
        );
    }


    public void fail(String reasonCode) {

        if (reasonCode == null ||
                reasonCode.isBlank()) {

            throw new IllegalArgumentException(
                    "Failure reason cannot be empty"
            );
        }

        if (status == TransactionStatus.COMPLETED ||
                status == TransactionStatus.FAILED) {

            throw new IllegalStateException(
                    "Terminal transaction cannot fail"
            );
        }

        raise(
                new TransactionFailedEvent(
                        UUID.randomUUID(),
                        id,
                        reasonCode,
                        version + 1
                )
        );
    }


    private void raise(
            FinancialTransactionEvent event
    ) {

        apply(event);

        uncommittedEvents.add(event);
    }


    private void apply(
            FinancialTransactionEvent event
    ) {

        switch (event) {

            case TransactionCreatedEvent e ->
                    on(e);

            case TransactionStartedEvent e ->
                    on(e);

            case TransactionCompletedEvent e ->
                    on(e);

            case TransactionFailedEvent e ->
                    on(e);
        }
    }


    private void on(
            TransactionCreatedEvent event
    ) {

        this.id = event.aggregateId();

        this.type = event.type();

        this.requestedAmount =
                event.requestedAmount();

        this.referenceId =
                event.referenceId();

        this.status =
                TransactionStatus.CREATED;

        this.failureReason = null;

        this.version =
                event.version();
    }


    private void on(
            TransactionStartedEvent event
    ) {

        this.status =
                TransactionStatus.PROCESSING;

        this.version =
                event.version();
    }


    private void on(
            TransactionCompletedEvent event
    ) {

        this.status =
                TransactionStatus.COMPLETED;

        this.version =
                event.version();
    }


    private void on(
            TransactionFailedEvent event
    ) {

        this.status =
                TransactionStatus.FAILED;

        this.failureReason =
                event.reasonCode();

        this.version =
                event.version();
    }


    public List<FinancialTransactionEvent>
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