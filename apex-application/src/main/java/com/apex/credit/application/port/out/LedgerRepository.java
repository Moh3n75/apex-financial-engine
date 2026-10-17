package com.apex.credit.application.port.out;

import com.apex.credit.domain.ledger.LedgerTransaction;

public interface LedgerRepository {

    void append(
            LedgerTransaction ledgerTransaction
    );
}