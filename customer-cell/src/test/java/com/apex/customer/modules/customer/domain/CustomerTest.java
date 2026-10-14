package com.apex.customer.modules.customer.domain;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Instant;

import org.junit.jupiter.api.Test;

class CustomerTest {

    private final Instant now = Instant.parse("2026-10-14T12:00:00Z");

    @Test
    void createsActiveCustomerWithGeneratedId() {
        Customer customer = Customer.create("CUST-10001", "مشتری آزمایشی", now);

        assertThat(customer.id()).isNotNull();
        assertThat(customer.externalReference()).isEqualTo("CUST-10001");
        assertThat(customer.displayName()).isEqualTo("مشتری آزمایشی");
        assertThat(customer.status()).isEqualTo(CustomerStatus.ACTIVE);
        assertThat(customer.createdAt()).isEqualTo(now);
    }

    @Test
    void trimsAndPreservesPersianUtf8() {
        Customer customer = Customer.create("  CUST-FA  ", "  مشتری آزمایشی  ", now);

        assertThat(customer.externalReference()).isEqualTo("CUST-FA");
        assertThat(customer.displayName()).isEqualTo("مشتری آزمایشی");
    }

    @Test
    void rejectsBlankExternalReference() {
        assertThatThrownBy(() -> Customer.create("  ", "Name", now))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("customer.validation.externalReference.required");
    }

    @Test
    void rejectsOversizedExternalReference() {
        String tooLong = "X".repeat(65);
        assertThatThrownBy(() -> Customer.create(tooLong, "Name", now))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("customer.validation.externalReference.required");
    }

    @Test
    void rejectsBlankDisplayName() {
        assertThatThrownBy(() -> Customer.create("CUST-1", " ", now))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("customer.validation.displayName.required");
    }

    @Test
    void rejectsOversizedDisplayName() {
        String tooLong = "ن".repeat(151);
        assertThatThrownBy(() -> Customer.create("CUST-1", tooLong, now))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("customer.validation.displayName.required");
    }
}
