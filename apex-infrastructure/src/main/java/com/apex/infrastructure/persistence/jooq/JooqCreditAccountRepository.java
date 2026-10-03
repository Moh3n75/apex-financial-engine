package com.apex.infrastructure.persistence.jooq;

import com.apex.credit.application.port.out.CreditAccountRepository;
import com.apex.credit.domain.model.CreditAccount;
import com.apex.credit.domain.valueobject.CreditAccountId;
import com.apex.credit.domain.valueobject.CreditAmount;
import com.apex.credit.domain.valueobject.CreditTypeId;
import com.apex.credit.domain.valueobject.MemberId;

import org.jooq.DSLContext;
import org.springframework.stereotype.Repository;

import java.time.LocalDateTime;
import java.util.Optional;

import static com.apex.infrastructure.jooq.generated.Tables.CREDIT_ACCOUNT_STATE;


@Repository
public class JooqCreditAccountRepository implements CreditAccountRepository {


    private final DSLContext dsl;


    public JooqCreditAccountRepository(
            DSLContext dsl
    ) {

        this.dsl = dsl;
    }


    @Override
    public Optional<CreditAccount> findById(
            CreditAccountId id
    ) {

        return dsl
                .selectFrom(
                        CREDIT_ACCOUNT_STATE
                )
                .where(
                        CREDIT_ACCOUNT_STATE.PUBLIC_ID.eq(
                                id.value()
                        )
                )
                .fetchOptional(
                        record ->
                                CreditAccount.restore(

                                        new CreditAccountId(
                                                record.getPublicId()
                                        ),

                                        new MemberId(
                                                record.getMemberPublicId()
                                        ),

                                        new CreditTypeId(
                                                record.getCreditTypePublicId()
                                        ),

                                        new CreditAmount(
                                                record.getAvailableUnits()
                                        ),

                                        new CreditAmount(
                                                record.getBlockedUnits()
                                        ),

                                        record.getVersion()

                                )
                );
    }


    @Override
    public void save(
            CreditAccount account
    ) {

        long expectedVersion =
                account.persistedVersion();


        int updated =
                dsl
                        .update(
                                CREDIT_ACCOUNT_STATE
                        )

                        .set(
                                CREDIT_ACCOUNT_STATE.AVAILABLE_UNITS,
                                account
                                        .getAvailable()
                                        .units()
                        )

                        .set(
                                CREDIT_ACCOUNT_STATE.BLOCKED_UNITS,
                                account
                                        .getBlocked()
                                        .units()
                        )

                        .set(
                                CREDIT_ACCOUNT_STATE.VERSION,
                                account.getVersion()
                        )

                        .set(
                                CREDIT_ACCOUNT_STATE.UPDATED_AT,
                                LocalDateTime.now()
                        )

                        .where(
                                CREDIT_ACCOUNT_STATE.PUBLIC_ID.eq(
                                        account
                                                .getId()
                                                .value()
                                )
                        )

                        .and(
                                CREDIT_ACCOUNT_STATE.VERSION.eq(
                                        expectedVersion
                                )
                        )

                        .execute();


        if (updated != 1) {

            throw new IllegalStateException(
                    "Credit account concurrent modification detected: "
                            + account.getId()
            );
        }


        account.markChangesAsCommitted();
    }
}