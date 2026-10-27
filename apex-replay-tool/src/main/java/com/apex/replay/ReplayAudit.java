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
                        "replay_action", "replay_run", "replay_delivery", "flyway_schema_history")::contains), "AUDIT_DATABASE_NOT_EMPTY_OR_OWNED");
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

    public record Delivery(UUID eventId, String state, String marker, String topic, int partition, Long offset, UUID claimId) { }

    public record Status(UUID planId, String status, String digest, int planned, String publication,
                         String businessProcessing, List<Delivery> deliveries) { }

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
                var deliveries = deliveries(c, id);
                boolean claimed;
                try (var run = c.prepareStatement("SELECT 1 FROM replay_run WHERE plan_id=?")) {
                    run.setObject(1, id); try (var found = run.executeQuery()) { claimed = found.next(); }
                }
                String publication = deliveries.isEmpty() ? claimed ? "NEEDS_RECONCILIATION" : "NOT_PUBLISHED"
                        : deliveries.size() == plan.items().size() && deliveries.stream().allMatch(d -> d.state().equals("PUBLISHED"))
                            ? "PUBLISHED_TO_REPLAY_TOPIC" : "NEEDS_RECONCILIATION";
                return new Status(id, row.getString(3), row.getString(2), row.getInt(4), publication,
                        "NOT_VERIFIED", deliveries);
            }
        }
    }

    public Plan approved(UUID id, String digest) throws SQLException {
        hash(digest);
        try (var c = connection()) { return approved(c, id, digest); }
    }

    private Plan approved(Connection c, UUID id, String digest) throws SQLException {
        var status = load(c, id, true);
        require(status.status().equals("APPROVED") && status.digest().equals(digest), "APPROVED_DIGEST_REQUIRED");
        try (var query = c.prepareStatement("SELECT canonical_json,approval_digest FROM replay_plan WHERE id=?")) {
            query.setObject(1, id);
            try (var row = query.executeQuery()) {
                require(row.next() && digest.equals(row.getString(2)), "APPROVED_DIGEST_REQUIRED");
                return JSON.readValue(row.getString(1), Plan.class);
            }
        }
    }

    /** One durable execution claim per plan; abandoned/incomplete runs never auto-resume. */
    public UUID claim(UUID id, String digest, String actor, String reason, String resolved) throws SQLException {
        text(actor); text(reason); text(resolved);
        try (var c = connection()) {
            c.setAutoCommit(false);
            try {
                approved(c, id, digest);
                try (var query = c.prepareStatement("SELECT claim_id FROM replay_run WHERE plan_id=?")) {
                    query.setObject(1, id);
                    try (var row = query.executeQuery()) {
                        require(!row.next(), "EXECUTION_ALREADY_CLAIMED_RECONCILE_REQUIRED");
                    }
                }
                UUID claim = UUID.randomUUID();
                try (var insert = c.prepareStatement("INSERT INTO replay_run(plan_id,claim_id,actor,reason,cause_resolved) VALUES (?,?,?,?,?)")) {
                    insert.setObject(1, id); insert.setObject(2, claim); insert.setString(3, actor);
                    insert.setString(4, reason); insert.setString(5, resolved); insert.executeUpdate();
                }
                deliveryAction(c, id, null, claim, "EXECUTION", actor, reason, digest, null, null);
                c.commit(); return claim;
            } catch (Exception failure) { c.rollback(); throw failure; }
        }
    }

    public void intent(Plan plan, Item item, UUID claim, String marker, String actor, String reason) throws SQLException {
        try (var c = connection()) {
            c.setAutoCommit(false);
            try {
                approved(c, plan.planId(), plan.digest());
                try (var insert = c.prepareStatement("INSERT INTO replay_delivery(plan_id,event_id,claim_id,marker,state,"
                        + "destination_topic,destination_partition) SELECT ?,?,?,?,'PUBLISHING',?,? FROM replay_run WHERE plan_id=? AND claim_id=?")) {
                    insert.setObject(1, plan.planId()); insert.setObject(2, item.eventId()); insert.setObject(3, claim);
                    insert.setString(4, marker); insert.setString(5, item.route().replayTopic()); insert.setInt(6, item.origin().partition());
                    insert.setObject(7, plan.planId()); insert.setObject(8, claim);
                    require(insert.executeUpdate() == 1, "EXECUTION_CLAIM_MISMATCH");
                }
                deliveryAction(c, plan.planId(), item.eventId(), claim, "PUBLISH_INTENT", actor, reason, plan.digest(), null, null);
                c.commit();
            } catch (Exception failure) { c.rollback(); throw failure; }
        }
    }

    public void finish(Plan plan, Item item, Coordinate receipt, String actor, String reason, boolean reconciled)
            throws SQLException {
        if (receipt != null) {
            require(receipt.topic().equals(item.route().replayTopic()) && receipt.partition() == item.origin().partition(), "RECEIPT_DESTINATION_MISMATCH");
        }
        try (var c = connection()) {
            c.setAutoCommit(false);
            try {
                approved(c, plan.planId(), plan.digest());
                var existing = deliveries(c, plan.planId()).stream().filter(d -> d.eventId().equals(item.eventId())).findFirst()
                        .orElseThrow(() -> new IllegalArgumentException("NO_DURABLE_PUBLISH_INTENT"));
                if (existing.state().equals("PUBLISHED")) {
                    require(receipt == null || existing.offset().equals(receipt.offset()), "CONFLICTING_RECEIPT"); c.rollback(); return;
                }
                String kind = receipt == null ? "PUBLISH_UNKNOWN" : reconciled ? "RECONCILED" : "PUBLISHED";
                try (var update = c.prepareStatement("UPDATE replay_delivery SET state=?,destination_offset=?,diagnostic_code=?,updated_at=now() "
                        + "WHERE plan_id=? AND event_id=? AND state<>'PUBLISHED'")) {
                    update.setString(1, receipt == null ? "PUBLISH_UNKNOWN" : "PUBLISHED");
                    update.setObject(2, receipt == null ? null : receipt.offset()); update.setString(3, kind);
                    update.setObject(4, plan.planId()); update.setObject(5, item.eventId()); require(update.executeUpdate() == 1, "DELIVERY_STATE_CONFLICT");
                }
                deliveryAction(c, plan.planId(), item.eventId(), existing.claimId(), kind, actor, reason, plan.digest(), receipt, kind);
                c.commit();
            } catch (Exception failure) { c.rollback(); throw failure; }
        }
    }

    public void noEvidence(Plan plan, Item item, String actor, String reason) throws SQLException {
        try (var c = connection()) {
            c.setAutoCommit(false);
            try {
                approved(c, plan.planId(), plan.digest());
                var existing = deliveries(c, plan.planId()).stream().filter(d -> d.eventId().equals(item.eventId())).findFirst().orElseThrow();
                deliveryAction(c, plan.planId(), item.eventId(), existing.claimId(), "NO_EVIDENCE", actor, reason, plan.digest(), null, "UNPROVEN_NOT_SAFE_TO_RESEND");
                c.commit();
            } catch (Exception failure) { c.rollback(); throw failure; }
        }
    }

    private List<Delivery> deliveries(Connection c, UUID id) throws SQLException {
        var result = new ArrayList<Delivery>();
        try (var query = c.prepareStatement("SELECT event_id,state,marker,destination_topic,destination_partition,destination_offset,claim_id "
                + "FROM replay_delivery WHERE plan_id=? ORDER BY event_id")) {
            query.setObject(1, id);
            try (var rows = query.executeQuery()) {
                while (rows.next()) { result.add(new Delivery(rows.getObject(1, UUID.class), rows.getString(2), rows.getString(3),
                        rows.getString(4), rows.getInt(5), rows.getObject(6, Long.class), rows.getObject(7, UUID.class))); }
            }
        }
        return List.copyOf(result);
    }

    private static void deliveryAction(Connection c, UUID id, UUID eventId, UUID claim, String kind, String actor,
            String reason, String digest, Coordinate receipt, String code) throws SQLException {
        text(actor); text(reason);
        try (var insert = c.prepareStatement("INSERT INTO replay_action(plan_id,action,actor,reason,digest,event_id,claim_id,"
                + "destination_topic,destination_partition,destination_offset,diagnostic_code) VALUES (?,?,?,?,?,?,?,?,?,?,?)")) {
            insert.setObject(1, id); insert.setString(2, kind); insert.setString(3, actor); insert.setString(4, reason);
            insert.setString(5, digest); insert.setObject(6, eventId); insert.setObject(7, claim);
            insert.setString(8, receipt == null ? null : receipt.topic()); insert.setObject(9, receipt == null ? null : receipt.partition());
            insert.setObject(10, receipt == null ? null : receipt.offset()); insert.setString(11, code); insert.executeUpdate();
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
