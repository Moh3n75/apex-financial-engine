package com.apex.platform.messaging.jdbc;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.util.List;
import java.util.UUID;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.transaction.annotation.Transactional;

import com.apex.platform.messaging.outbox.OutboxRecord;
import com.apex.platform.messaging.outbox.OutboxStore;

public class JdbcOutboxStore implements OutboxStore {

    private final JdbcTemplate jdbcTemplate;
    private final Clock clock;

    private final RowMapper<OutboxRecord> rowMapper = (rs, rowNum) -> mapRow(rs);

    public JdbcOutboxStore(JdbcTemplate jdbcTemplate, Clock clock) {
        this.jdbcTemplate = jdbcTemplate;
        this.clock = clock;
    }

    @Override
    public List<OutboxRecord> findBatch(int size) {
        OffsetDateTime now = OffsetDateTime.now(clock);
        return jdbcTemplate.query(
                """
                SELECT * FROM outbox_event
                WHERE published = false
                  AND dead_letter = false
                  AND (next_retry_at IS NULL OR next_retry_at <= ?)
                  AND (claim_token IS NULL OR claimed_until <= ?)
                ORDER BY occurred_at ASC
                LIMIT ?
                FOR UPDATE SKIP LOCKED
                """,
                rowMapper,
                now,
                now,
                size
        );
    }

    @Override
    @Transactional
    public List<OutboxRecord> claimBatch(int size, String publisherInstanceId, Duration leaseDuration) {
        OffsetDateTime now = OffsetDateTime.now(clock);
        OffsetDateTime claimedUntil = now.plus(leaseDuration);
        UUID claimToken = UUID.randomUUID();

        List<Long> ids = jdbcTemplate.query(
                """
                SELECT id FROM outbox_event
                WHERE published = false
                  AND dead_letter = false
                  AND (next_retry_at IS NULL OR next_retry_at <= ?)
                  AND (claim_token IS NULL OR claimed_until <= ?)
                ORDER BY occurred_at ASC
                LIMIT ?
                FOR UPDATE SKIP LOCKED
                """,
                (rs, rowNum) -> rs.getLong("id"),
                now,
                now,
                size
        );

        if (ids.isEmpty()) {
            return List.of();
        }

        for (Long id : ids) {
            jdbcTemplate.update(
                    """
                    UPDATE outbox_event
                    SET claim_token = ?, claimed_at = ?, claimed_until = ?, publisher_instance_id = ?
                    WHERE id = ?
                    """,
                    claimToken,
                    now,
                    claimedUntil,
                    publisherInstanceId,
                    id
            );
        }

        return jdbcTemplate.query(
                "SELECT * FROM outbox_event WHERE claim_token = ?",
                rowMapper,
                claimToken
        );
    }

    @Override
    @Transactional
    public boolean markPublished(Long id, UUID claimToken) {
        return jdbcTemplate.update(
                """
                UPDATE outbox_event
                SET published = true,
                    published_at = ?,
                    claim_token = NULL,
                    claimed_until = NULL,
                    publisher_instance_id = NULL
                WHERE id = ? AND claim_token = ?
                """,
                OffsetDateTime.now(clock),
                id,
                claimToken
        ) == 1;
    }

    @Override
    @Transactional
    public boolean markFailed(Long id, UUID claimToken, String error, Instant nextRetryAt) {
        return jdbcTemplate.update(
                """
                UPDATE outbox_event
                SET retry_count = retry_count + 1,
                    last_error = ?,
                    next_retry_at = ?,
                    claim_token = NULL,
                    claimed_until = NULL,
                    publisher_instance_id = NULL
                WHERE id = ? AND claim_token = ?
                """,
                error,
                nextRetryAt == null ? null : OffsetDateTime.ofInstant(nextRetryAt, clock.getZone()),
                id,
                claimToken
        ) == 1;
    }

    @Override
    @Transactional
    public boolean moveToDeadLetter(Long id, UUID claimToken, String error) {
        return jdbcTemplate.update(
                """
                UPDATE outbox_event
                SET dead_letter = true,
                    dead_letter_at = ?,
                    last_error = ?,
                    claim_token = NULL,
                    claimed_until = NULL,
                    publisher_instance_id = NULL
                WHERE id = ? AND claim_token = ?
                """,
                OffsetDateTime.now(clock),
                error,
                id,
                claimToken
        ) == 1;
    }

    private static OutboxRecord mapRow(ResultSet rs) throws SQLException {
        OffsetDateTime occurredAt = rs.getObject("occurred_at", OffsetDateTime.class);
        OffsetDateTime nextRetryAt = rs.getObject("next_retry_at", OffsetDateTime.class);
        OffsetDateTime claimedUntil = rs.getObject("claimed_until", OffsetDateTime.class);
        Object payload = rs.getObject("payload");
        String payloadJson = payload == null ? null : payload.toString();

        return new OutboxRecord(
                rs.getLong("id"),
                rs.getObject("event_id", UUID.class),
                rs.getString("event_type"),
                rs.getInt("event_version"),
                rs.getObject("correlation_id", UUID.class),
                rs.getObject("causation_id", UUID.class),
                String.valueOf(rs.getObject("aggregate_id", UUID.class)),
                rs.getString("aggregate_type"),
                rs.getString("source_service"),
                rs.getString("cell_id"),
                occurredAt == null ? null : occurredAt.toInstant(),
                payloadJson,
                rs.getInt("retry_count"),
                nextRetryAt == null ? null : nextRetryAt.toInstant(),
                rs.getObject("claim_token", UUID.class),
                claimedUntil == null ? null : claimedUntil.toInstant(),
                rs.getString("publisher_instance_id")
        );
    }
}
