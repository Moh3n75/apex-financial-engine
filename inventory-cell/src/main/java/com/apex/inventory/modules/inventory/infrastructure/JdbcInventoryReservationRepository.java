
package com.apex.inventory.modules.inventory.infrastructure;

import com.apex.inventory.modules.inventory.domain.*;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.support.TransactionSynchronizationManager;

import java.util.List;
import java.util.UUID;

@Repository
public class JdbcInventoryReservationRepository
        implements InventoryReservationRepository {

    private final JdbcTemplate jdbc;

    public JdbcInventoryReservationRepository(JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    @Override
    public ReservationResult reserve(ReservationRequest request) {
        requireTransaction();

        // 1. Claim the business operation.
        int inserted = jdbc.update("""
                INSERT INTO inventory_reservations (
                    order_id, status, created_at
                )
                VALUES (?, 'RESERVED', CURRENT_TIMESTAMP)
                ON CONFLICT (order_id) DO NOTHING
                """, request.orderId());

        // 2. Existing operation: verify payload and return old result.
        if (inserted == 0) {
            return existingReservation(request);
        }

        // 3. New operation: persist requested items.
        insertItems(request);

        // 4. Lock and validate all stock before updating anything.
        if (!hasEnoughStock(request)) {
            jdbc.update("""
                    UPDATE inventory_reservations
                    SET status = 'REJECTED'
                    WHERE order_id = ?
                    """, request.orderId());

            return new ReservationResult(
                    request.orderId(),
                    ReservationStatus.REJECTED
            );
        }

        // 5. Apply all stock changes.
        for (ReservationItem item : request.items()) {
            int updated = jdbc.update("""
                    UPDATE inventory_stock
                    SET available_quantity = available_quantity - ?,
                        reserved_quantity = reserved_quantity + ?
                    WHERE product_id = ?
                      AND available_quantity >= ?
                    """,
                    item.quantity(),
                    item.quantity(),
                    item.productId(),
                    item.quantity()
            );

            if (updated != 1) {
                throw new IllegalStateException(
                        "Stock update failed: " + item.productId()
                );
            }
        }

        return new ReservationResult(
                request.orderId(),
                ReservationStatus.RESERVED
        );
    }

    private ReservationResult existingReservation(
            ReservationRequest request
    ) {
        List<ReservationItem> existingItems = jdbc.query("""
                SELECT product_id, quantity
                FROM inventory_reservation_items
                WHERE order_id = ?
                ORDER BY product_id
                """,
                reservationItemMapper(),
                request.orderId()
        );

        // PostgreSQL UUID ordering is unsigned; ReservationRequest uses Java UUID.compareTo.
        // Compare both sides in the same order when detecting a repeated business request.
        existingItems = existingItems.stream()
                .sorted(java.util.Comparator.comparing(ReservationItem::productId))
                .toList();
        if (!existingItems.equals(request.items())) {
            throw new ReservationConflictException(
                    request.orderId()
            );
        }

        String status = jdbc.queryForObject("""
                SELECT status
                FROM inventory_reservations
                WHERE order_id = ?
                """,
                String.class,
                request.orderId()
        );

        return new ReservationResult(
                request.orderId(),
                ReservationStatus.valueOf(status)
        );
    }

    private void insertItems(ReservationRequest request) {
        List<Object[]> args = request.items().stream()
                .map(item -> new Object[]{
                        request.orderId(),
                        item.productId(),
                        item.quantity()
                })
                .toList();

        jdbc.batchUpdate("""
                INSERT INTO inventory_reservation_items (
                    order_id, product_id, quantity
                )
                VALUES (?, ?, ?)
                """, args);
    }

    private boolean hasEnoughStock(ReservationRequest request) {
        // ReservationRequest already sorts items by productId.
        // Lock rows in that same deterministic order.
        for (ReservationItem item : request.items()) {
            List<Integer> available = jdbc.query("""
                    SELECT available_quantity
                    FROM inventory_stock
                    WHERE product_id = ?
                    FOR UPDATE
                    """,
                    (rs, rowNum) -> rs.getInt(1),
                    item.productId()
            );

            if (available.isEmpty()
                    || available.getFirst() < item.quantity()) {
                return false;
            }
        }

        return true;
    }

    private RowMapper<ReservationItem> reservationItemMapper() {
        return (rs, rowNum) -> new ReservationItem(
                rs.getObject("product_id", UUID.class),
                rs.getInt("quantity")
        );
    }

    private void requireTransaction() {
        if (!TransactionSynchronizationManager
                .isActualTransactionActive()) {
            throw new IllegalStateException(
                    "Inventory reservation requires an active transaction"
            );
        }
    }
}
