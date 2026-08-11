package com.sadeghi.accounting.transaction.orm;

import jakarta.persistence.*;
import lombok.Getter;
import lombok.Setter;

import java.math.BigDecimal;

@Table(name = "credit")
@Entity
@Getter
@Setter
public class Account {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Integer id;

    private BigDecimal availableAmount;

    private BigDecimal totalAmount;

    private BigDecimal blockAmount;
}
