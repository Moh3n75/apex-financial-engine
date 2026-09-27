package com.apex.credit.domain;


import com.apex.credit.domain.model.CreditAccount;
import com.apex.credit.domain.valueobject.*;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.UUID;


import static org.junit.jupiter.api.Assertions.*;


class CreditAccountTest {


    @Test
    void should_block_credit_successfully(){


        CreditAccount account =
                new CreditAccount(
                        new CreditAccountId(UUID.randomUUID()),
                        new MemberId(UUID.randomUUID())
                );


        account.addCredit(
                new Money(
                        new BigDecimal("100000")
                )
        );


        account.block(
                new Money(
                        new BigDecimal("30000")
                ),
                "SHIPMENT-100"
        );


        assertEquals(
                2,
                account.getChanges().size()
        );

    }

}