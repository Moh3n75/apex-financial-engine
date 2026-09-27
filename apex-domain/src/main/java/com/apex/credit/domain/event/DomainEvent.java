package com.apex.credit.domain.event;

import com.apex.credit.domain.valueobject.CreditAccountId;

import java.util.UUID;

public interface DomainEvent {

    UUID eventId();


    CreditAccountId aggregateId();


    long version();
}
