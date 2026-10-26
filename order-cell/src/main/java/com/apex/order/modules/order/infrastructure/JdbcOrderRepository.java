
package com.apex.order.modules.order.infrastructure;

import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.support.TransactionSynchronizationManager;

import com.apex.order.modules.order.domain.Order;
import com.apex.order.modules.order.domain.OrderRepository;
import com.apex.order.modules.order.domain.OrderStatus;

@Repository
public class JdbcOrderRepository implements OrderRepository {

    private final JdbcTemplate jdbc;

    public JdbcOrderRepository(JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    @Override
    public void save(Order order) {

        jdbc.update("""
                INSERT INTO orders (
                    id,
                    external_reference,
                    description,
                    status,
                    created_at
                )
                VALUES (?, ?, ?, ?, ?)
                """,
                order.id(),
                order.externalReference(),
                order.description(),
                order.status().name(),
                OffsetDateTime.ofInstant(
                        order.createdAt(),
                        ZoneOffset.UTC
                )
        );

        if (order.items().isEmpty()) {
            return;
        }

        List<Object[]> batchArgs = order.items()
                .stream()
                .map(item -> new Object[]{
                        order.id(),
                        item.productId(),
                        item.quantity()
                })
                .toList();

        jdbc.batchUpdate("""
                INSERT INTO order_items (
                    order_id,
                    product_id,
                    quantity
                )
                VALUES (?, ?, ?)
                """, batchArgs);
    }

    @Override
    public Optional<OrderStatus> findStatus(UUID orderId) {
        return jdbc.query("SELECT status FROM orders WHERE id = ?",
                (rs, row) -> OrderStatus.valueOf(rs.getString(1)), orderId)
                .stream().findFirst();
    }

    @Override
    public void applyInventoryOutcome(UUID orderId, OrderStatus target) {
        if (!TransactionSynchronizationManager.isActualTransactionActive()) {
            throw new IllegalStateException("Inventory outcome requires the Inbox transaction");
        }
        if (target != OrderStatus.PENDING_PAYMENT && target != OrderStatus.CANCELLED) {
            throw new IllegalArgumentException("Unsupported Inventory outcome target: " + target);
        }
        int updated = jdbc.update("UPDATE orders SET status = ? WHERE id = ? AND status = 'PENDING_INVENTORY'",
                target.name(), orderId);
        if (updated == 1) {
            return;
        }
        var current = findStatus(orderId).orElseThrow(() ->
                new IllegalStateException("Inventory outcome refers to missing order: " + orderId));
        if (current != target) {
            throw new IllegalStateException("Conflicting or stale Inventory outcome for order " + orderId
                    + ": current=" + current + ", target=" + target);
        }
    }
}
