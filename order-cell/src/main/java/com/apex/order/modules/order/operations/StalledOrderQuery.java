package com.apex.order.modules.order.operations;

import java.time.Instant;
import java.time.OffsetDateTime;
import java.util.List;
import java.util.UUID;

import org.springframework.context.annotation.Profile;
import org.springframework.http.HttpStatus;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.server.ResponseStatusException;

/** Order-owned observation only. Age is never interpreted as a terminal business decision. */
@Service
@Profile("operations")
public class StalledOrderQuery {
    private final JdbcTemplate jdbc;
    private final OrderOperationsProperties properties;

    public StalledOrderQuery(JdbcTemplate jdbc, OrderOperationsProperties properties) {
        this.jdbc = jdbc;
        this.properties = properties;
    }

    @Transactional(readOnly = true)
    public Page find(int limit, int offset) {
        if (limit < 1 || limit > 100 || offset < 0 || offset > 10_000) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "limit must be 1..100 and offset 0..10000");
        }
        Instant observedAt = jdbc.queryForObject("SELECT clock_timestamp()", OffsetDateTime.class).toInstant();
        var rows = jdbc.query("""
                SELECT o.id,o.status,o.status_entered_at,o.inventory_decision,o.payment_decision,o.release_acknowledged,
                       (SELECT count(*) FROM outbox_event e WHERE e.aggregate_id=o.id AND NOT e.published AND NOT e.dead_letter) pending,
                       (SELECT count(*) FROM outbox_event e WHERE e.aggregate_id=o.id AND e.dead_letter) dead
                FROM orders o
                WHERE o.status IN ('PENDING_INVENTORY','PENDING_PAYMENT','CANCELLING')
                  AND (o.status_entered_at IS NULL OR o.status_entered_at<=?)
                ORDER BY o.status_entered_at NULLS FIRST,o.id
                LIMIT ? OFFSET ?
                """, (rs, n) -> {
                    var entered = rs.getObject("status_entered_at", OffsetDateTime.class);
                    String status = rs.getString("status");
                    long pending = rs.getLong("pending"), dead = rs.getLong("dead");
                    String diagnostic = dead > 0 ? "ORDER_OUTBOX_REQUIRES_OPERATOR_REVIEW"
                            : pending > 0 ? "ORDER_OUTBOX_PENDING_PUBLICATION"
                            : switch (status) {
                                case "PENDING_PAYMENT" -> "PAYMENT_OUTCOME_UNKNOWN_CHECK_PAYMENT_AND_DLT";
                                case "CANCELLING" -> "RELEASE_ACK_OVERDUE_CHECK_INVENTORY_AND_DLT";
                                default -> "INVENTORY_OUTCOME_UNKNOWN_CHECK_INVENTORY_AND_DLT";
                            };
                    return new Row(rs.getObject("id", UUID.class), status,
                            entered == null ? null : entered.toInstant(),
                            entered == null ? null : java.time.Duration.between(entered.toInstant(), observedAt).toMillis(),
                            entered == null ? "STATE_ENTRY_TIME_UNKNOWN" : "POTENTIALLY_STALLED",
                            rs.getString("inventory_decision"), rs.getString("payment_decision"),
                            rs.getBoolean("release_acknowledged"), pending, dead, diagnostic, "NOT_INSPECTED");
                }, OffsetDateTime.ofInstant(observedAt.minus(properties.ageThreshold()), java.time.ZoneOffset.UTC),
                limit + 1, offset);
        return new Page(observedAt, properties.ageThreshold().toMillis(), limit, offset, rows.size() > limit,
                List.copyOf(rows.subList(0, Math.min(limit, rows.size()))));
    }

    public record Page(Instant observedAt, long thresholdMillis, int limit, int offset, boolean hasMore, List<Row> orders) { }
    public record Row(UUID orderId, String status, Instant statusEnteredAt, Long ageMillis, String assessment,
                      String inventoryDecision, String paymentDecision, boolean releaseAcknowledged,
                      long pendingOutbox, long deadOutbox, String diagnostic, String quarantineInspection) { }
}
