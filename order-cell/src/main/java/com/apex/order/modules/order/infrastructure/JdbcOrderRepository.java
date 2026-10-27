
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
                    created_at,
                    status_entered_at
                )
                VALUES (?, ?, ?, ?, ?, clock_timestamp())
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
    public boolean applySagaOutcome(UUID orderId, String outcome) {
        if (!TransactionSynchronizationManager.isActualTransactionActive()) { throw new IllegalStateException("Saga transaction required"); }
        String set; String condition;
        switch (outcome) {
            case "INVENTORY_RESERVED" -> { set="status='PENDING_PAYMENT',inventory_decision='RESERVED'"; condition="status='PENDING_INVENTORY' AND inventory_decision IS NULL"; }
            case "INVENTORY_RESERVATION_REJECTED" -> { set="status='CANCELLED',inventory_decision='REJECTED'"; condition="status='PENDING_INVENTORY' AND inventory_decision IS NULL"; }
            case "PAYMENT_AUTHORIZED" -> { set="status='CONFIRMED',payment_decision='AUTHORIZED'"; condition="status='PENDING_PAYMENT' AND inventory_decision='RESERVED' AND payment_decision IS NULL"; }
            case "PAYMENT_DECLINED" -> { set="status='CANCELLING',payment_decision='DECLINED'"; condition="status='PENDING_PAYMENT' AND inventory_decision='RESERVED' AND payment_decision IS NULL"; }
            case "INVENTORY_RELEASED" -> { set="status='CANCELLED',release_acknowledged=true"; condition="status='CANCELLING' AND payment_decision='DECLINED' AND inventory_decision='RESERVED' AND NOT release_acknowledged"; }
            default -> throw new IllegalArgumentException("Unknown Saga outcome");
        }
        if (jdbc.update("UPDATE orders SET " + set + ",status_entered_at=clock_timestamp() WHERE id=? AND " + condition, orderId)==1) { return true; }
        var row = jdbc.queryForMap("SELECT inventory_decision,payment_decision,release_acknowledged FROM orders WHERE id=?", orderId);
        boolean duplicate = switch(outcome) {
            case "INVENTORY_RESERVED" -> "RESERVED".equals(row.get("inventory_decision"));
            case "INVENTORY_RESERVATION_REJECTED" -> "REJECTED".equals(row.get("inventory_decision"));
            case "PAYMENT_AUTHORIZED" -> "AUTHORIZED".equals(row.get("payment_decision"));
            case "PAYMENT_DECLINED" -> "DECLINED".equals(row.get("payment_decision"));
            case "INVENTORY_RELEASED" -> Boolean.TRUE.equals(row.get("release_acknowledged"));
            default -> false;
        };
        if (!duplicate) { throw new IllegalStateException("Conflicting or early Saga outcome: " + outcome); }
        return false;
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
        int updated = jdbc.update("UPDATE orders SET status = ?,status_entered_at=clock_timestamp() WHERE id = ? AND status = 'PENDING_INVENTORY'",
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
