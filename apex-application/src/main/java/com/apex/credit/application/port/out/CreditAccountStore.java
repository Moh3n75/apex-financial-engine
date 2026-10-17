package com.apex.credit.application.port.out;

import com.apex.credit.domain.model.CreditAccount;
import com.apex.credit.domain.valueobject.CreditAccountId;

import java.util.Optional;

public interface CreditAccountStore {

    Optional<CreditAccount> findById(
            CreditAccountId id
    );

    void save(
            CreditAccount account
    );
}