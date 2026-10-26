package com.apex.replay;

import static com.apex.replay.ReplayModel.*;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import org.flywaydb.core.Flyway;

/** Separate operator-owned audit DB. Approval is not publication or business completion. */
public final class ReplayAudit {
    private final String url;
    private final String username;
    private final String password;

    public ReplayAudit(String url, String username, String password) {
        require(url != null && url.startsWith("jdbc:postgresql:") && username != null && password != null,
                "AUDIT_CONNECTION_REQUIRED");
        this.url = url; this.username = username; this.password = password;
    }

    public void initialize() throws SQLException {
        try (var c = connection(); var statement = c.createStatement(); var result = statement.executeQuery(
                "SELECT tablename FROM pg_tables WHERE schemaname = 'public'")) {
            List<String> tables = new ArrayList<>();
            while (result.next()) { tables.add(result.getString(1)); }
            if (!tables.isEmpty()) {
                require(tables.stream().allMatch(List.of("replay_audit_identity", "replay_plan", "replay_item",
                        "replay_action", "flyway_schema_history")::contains), "AUDIT_DATABASE_NOT_EMPTY_OR_OWNED");
                requireReady(c);
            }
        }
        Flyway.configure().dataSource(url, username, password).locations("classpath:db/replay").load().migrate();
    }

    public void save(Plan plan) throws SQLException {
        try (var c = connection()) {
            requireReady(c);
            c.setAutoCommit(false);
            try {
                try (var insert = c.prepareStatement("INSERT INTO replay_plan "
                        + "(id,canonical_json,digest,actor,reason,item_count,status) VALUES (?,?,?,?,?,?,'PLANNED')")) {
                    insert.setObject(1, plan.planId()); insert.setString(2, plan.canonicalJson());
                    insert.setString(3, plan.digest()); insert.setString(4, plan.actor()); insert.setString(5, plan.reason());
                    insert.setInt(6, plan.items().size()); insert.executeUpdate();
                }
                for (Item item : plan.items()) {
                    try (var insert = c.prepareStatement("INSERT INTO replay_item "
                            + "(plan_id,origin_topic,origin_partition,origin_offset,target_group,event_id,raw_sha256,raw_length,"
                            + "dlt_topic,dlt_partition,dlt_offset,replay_topic) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)")) {
                        insert.setObject(1, plan.planId()); insert.setString(2, item.origin().topic());
                        insert.setInt(3, item.origin().partition()); insert.setLong(4, item.origin().offset());
                        insert.setString(5, item.originGroup()); insert.setObject(6, item.eventId());
                        insert.setString(7, item.rawSha256()); insert.setInt(8, item.rawLength());
                        insert.setString(9, item.dlt().topic()); insert.setInt(10, item.dlt().partition());
                        insert.setLong(11, item.dlt().offset()); insert.setString(12, item.route().replayTopic());
                        insert.executeUpdate();
                    }
                }
                action(c, plan.planId(), "PLAN", plan.actor(), plan.reason(), plan.digest());
                c.commit();
            } catch (Exception failure) { c.rollback(); throw failure; }
        }
    }

    public void approve(UUID id, String confirmation, String actor, String reason) throws SQLException {
        hash(confirmation); text(actor); text(reason);
        try (var c = connection()) {
            requireReady(c); c.setAutoCommit(false);
            try {
                var status = load(c, id, true);
                require(status.status().equals("PLANNED"), "PLAN_NOT_PLANNED");
                require(status.digest().equals(confirmation), "APPROVAL_DIGEST_MISMATCH");
                try (var update = c.prepareStatement("UPDATE replay_plan SET status='APPROVED',approval_actor=?,"
                        + "approval_reason=?,approval_at=now(),approval_digest=? WHERE id=? AND status='PLANNED'")) {
                    update.setString(1, actor); update.setString(2, reason); update.setString(3, confirmation);
                    update.setObject(4, id); require(update.executeUpdate() == 1, "CONCURRENT_APPROVAL");
                }
                action(c, id, "APPROVAL", actor, reason, confirmation); c.commit();
            } catch (Exception failure) { c.rollback(); throw failure; }
        }
    }

    public record Status(UUID planId, String status, String digest, int planned, String publication,
                         String businessProcessing) { }

    public Status status(UUID id) throws SQLException {
        try (var c = connection()) { requireReady(c); return load(c, id, false); }
    }

    private Status load(Connection c, UUID id, boolean lock) throws SQLException {
        try (var query = c.prepareStatement("SELECT canonical_json,digest,status,item_count FROM replay_plan WHERE id=?"
                + (lock ? " FOR UPDATE" : ""))) {
            query.setObject(1, id);
            try (var row = query.executeQuery()) {
                require(row.next(), "PLAN_NOT_FOUND");
                String canonical = row.getString(1);
                var plan = JSON.readValue(canonical, Plan.class);
                require(plan.planId().equals(id) && plan.digest().equals(row.getString(2))
                        && plan.canonicalJson().equals(canonical), "STORED_PLAN_DIGEST_MISMATCH");
                return new Status(id, row.getString(3), row.getString(2), row.getInt(4),
                        "NOT_IMPLEMENTED_ZERO_PUBLICATIONS", "NOT_VERIFIED");
            }
        }
    }

    private static void action(Connection c, UUID id, String kind, String actor, String reason, String digest)
            throws SQLException {
        try (var insert = c.prepareStatement("INSERT INTO replay_action(plan_id,action,actor,reason,digest) VALUES (?,?,?,?,?)")) {
            insert.setObject(1, id); insert.setString(2, kind); insert.setString(3, actor);
            insert.setString(4, reason); insert.setString(5, digest); insert.executeUpdate();
        }
    }

    private static void requireReady(Connection c) throws SQLException {
        try (var statement = c.createStatement(); var row = statement.executeQuery(
                "SELECT purpose FROM replay_audit_identity WHERE id=1")) {
            require(row.next() && row.getString(1).equals("apex-replay-audit-v1"), "AUDIT_DATABASE_NOT_INITIALIZED");
        }
    }

    private Connection connection() throws SQLException { return DriverManager.getConnection(url, username, password); }
}
