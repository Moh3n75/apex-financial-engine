package com.apex.order.modules.order.infrastructure;

import java.time.OffsetDateTime;
import java.time.ZoneOffset;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import com.apex.order.modules.order.domain.Order;
import com.apex.order.modules.order.domain.OrderRepository;

@Repository
public class JdbcOrderRepository implements OrderRepository {
    private final JdbcTemplate jdbc;

    public JdbcOrderRepository(JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    @Override
    public void save(Order order) {
        jdbc.update("INSERT INTO orders (id, external_reference, description, created_at) VALUES (?, ?, ?, ?)",
                order.id(), order.externalReference(), order.description(),
                OffsetDateTime.ofInstant(order.createdAt(), ZoneOffset.UTC));
    }
}
