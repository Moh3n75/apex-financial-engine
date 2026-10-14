package com.apex.customer.modules.customer.infrastructure;

import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.Optional;
import java.util.UUID;

import org.springframework.dao.DuplicateKeyException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Repository;

import com.apex.customer.modules.customer.domain.Customer;
import com.apex.customer.modules.customer.domain.CustomerRepository;
import com.apex.customer.modules.customer.domain.CustomerStatus;
import com.apex.customer.modules.customer.domain.DuplicateCustomerReferenceException;

@Repository
public class JdbcCustomerRepository implements CustomerRepository {

    private static final RowMapper<Customer> ROW_MAPPER = (rs, rowNum) -> {
        OffsetDateTime created = rs.getObject("created_at", OffsetDateTime.class);
        return Customer.restore(
                rs.getObject("id", UUID.class),
                rs.getString("external_reference"),
                rs.getString("display_name"),
                CustomerStatus.valueOf(rs.getString("status")),
                created.toInstant()
        );
    };

    private final JdbcTemplate jdbcTemplate;

    public JdbcCustomerRepository(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    @Override
    public Customer save(Customer customer) {
        try {
            jdbcTemplate.update(
                    """
                    INSERT INTO customers (id, external_reference, display_name, status, created_at)
                    VALUES (?, ?, ?, ?, ?)
                    """,
                    customer.id(),
                    customer.externalReference(),
                    customer.displayName(),
                    customer.status().name(),
                    OffsetDateTime.ofInstant(customer.createdAt(), ZoneOffset.UTC)
            );
            return customer;
        } catch (DuplicateKeyException ex) {
            throw new DuplicateCustomerReferenceException(customer.externalReference());
        }
    }

    @Override
    public Optional<Customer> findById(UUID id) {
        var rows = jdbcTemplate.query(
                """
                SELECT id, external_reference, display_name, status, created_at
                FROM customers
                WHERE id = ?
                """,
                ROW_MAPPER,
                id
        );
        return rows.stream().findFirst();
    }

    @Override
    public long countByExternalReference(String externalReference) {
        Long count = jdbcTemplate.queryForObject(
                "SELECT COUNT(*) FROM customers WHERE external_reference = ?",
                Long.class,
                externalReference
        );
        return count == null ? 0L : count;
    }
}
