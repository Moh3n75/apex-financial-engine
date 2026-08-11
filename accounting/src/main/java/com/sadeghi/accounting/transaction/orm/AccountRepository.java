package com.sadeghi.accounting.transaction.orm;

import jakarta.persistence.LockModeType;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Lock;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import java.math.BigDecimal;
import java.util.Optional;

public interface AccountRepository extends JpaRepository<Account, Integer> {

    @Lock(LockModeType.PESSIMISTIC_WRITE)
    @Query("select a from Account a where a.id = :id")
    Optional<Account> findByIdForUpdate(@Param("id") Integer id);


    @Modifying
    @Query(value = """
            UPDATE credit
            SET available_amount = available_amount - :amount,
                total_amount = total_amount - :amount
            WHERE id = :accountId
              AND available_amount >= :amount
            """, nativeQuery = true)
    int decreaseBalance(
            @Param("accountId") Integer accountId,
            @Param("amount") BigDecimal amount
    );

    @Modifying
    @Query(value = """
            UPDATE credit
            SET available_amount = available_amount + :amount,
                total_amount = total_amount + :amount
            WHERE id = :accountId
              AND available_amount >= :amount
            """, nativeQuery = true)
    int increaseBalance(
            @Param("accountId") Integer accountId,
            @Param("amount") BigDecimal amount
    );
}
