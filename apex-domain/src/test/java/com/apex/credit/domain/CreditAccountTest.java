package com.apex.credit.domain;


import com.apex.credit.domain.model.CreditAccount;
import com.apex.credit.domain.valueobject.*;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.UUID;


import static org.assertj.core.api.Assertions.assertThat;


class CreditAccountTest {


    @Test
    void should_add_and_block_credit(){


        CreditAccount account =
                CreditAccount.create(
                        new CreditAccountId(
                                UUID.randomUUID()
                        ),
                        new MemberId(
                                UUID.randomUUID()
                        )
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


        assertThat(
                account.uncommittedEvents()
        )
                .hasSize(3);

    }

}