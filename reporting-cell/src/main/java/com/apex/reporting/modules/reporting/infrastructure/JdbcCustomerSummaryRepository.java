package com.apex.reporting.modules.reporting.infrastructure;

import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.Optional;
import java.util.UUID;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Repository;

import com.apex.reporting.modules.reporting.domain.CustomerSummary;
import com.apex.reporting.modules.reporting.domain.CustomerSummaryRepository;

@Repository
public class JdbcCustomerSummaryRepository implements CustomerSummaryRepository {

    private static final RowMapper<CustomerSummary> MAPPER = (rs, rowNum) -> new CustomerSummary(
            rs.getObject("customer_id", UUID.class),
            rs.getString("external_reference"),
            rs.getString("display_name"),
            rs.getString("status"),
            rs.getObject("created_at", OffsetDateTime.class).toInstant(),
            rs.getObject("projected_at", OffsetDateTime.class).toInstant()
    );

    private final JdbcTemplate jdbcTemplate;

    public JdbcCustomerSummaryRepository(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    @Override
    public void upsert(CustomerSummary summary) {
        jdbcTemplate.update(
                """
                INSERT INTO customer_summary (
                    customer_id, external_reference, display_name, status, created_at, projected_at
                ) VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT (customer_id) DO UPDATE SET
                    external_reference = EXCLUDED.external_reference,
                    display_name = EXCLUDED.display_name,
                    status = EXCLUDED.status,
                    created_at = EXCLUDED.created_at,
                    projected_at = EXCLUDED.projected_at
                """,
                summary.customerId(),
                summary.externalReference(),
                summary.displayName(),
                summary.status(),
                OffsetDateTime.ofInstant(summary.createdAt(), ZoneOffset.UTC),
                OffsetDateTime.ofInstant(summary.projectedAt(), ZoneOffset.UTC)
        );
    }

    @Override
    public Optional<CustomerSummary> findByCustomerId(UUID customerId) {
        var rows = jdbcTemplate.query(
                """
                SELECT customer_id, external_reference, display_name, status, created_at, projected_at
                FROM customer_summary WHERE customer_id = ?
                """,
                MAPPER,
                customerId
        );
        return rows.stream().findFirst();
    }

    @Override
    public long count() {
        Long count = jdbcTemplate.queryForObject("SELECT COUNT(*) FROM customer_summary", Long.class);
        return count == null ? 0L : count;
    }
}
