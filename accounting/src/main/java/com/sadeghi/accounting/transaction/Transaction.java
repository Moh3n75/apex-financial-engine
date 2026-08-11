package com.sadeghi.accounting.transaction;

import jakarta.persistence.*;
import lombok.Getter;
import lombok.Setter;

import java.math.BigDecimal;
import java.util.Date;

@Table(name = "CREDIT_TRANSACTION")
@Entity
@Getter
@Setter
public class Transaction {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Integer id;

    private Date createAt;

    private BigDecimal amount;

    @ManyToOne(fetch = FetchType.LAZY)
    @JoinColumn(name = "credit_id")
    private Account sourceAccount;
}
