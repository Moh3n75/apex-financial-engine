package com.apex.replay;

import static com.apex.replay.ReplayModel.*;
import static com.apex.replay.ReplayValidationTest.*;
import static com.apex.replay.ReplayApprovalIntegrationTest.*;
import static org.assertj.core.api.Assertions.*;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.junit.jupiter.api.*;

@Tag("integration")
class ReplayExecutionIntegrationTest {
    @BeforeAll static void resources() throws Exception { start(); }
    @AfterAll static void cleanup() { stop(); }

    Plan approvedPlan() throws Exception {
        var selected = publishDlt(UUID.randomUUID());
        try (var reader = new DltReader(kafka, List.of(ROUTE))) {
            var plan = ReplayPlanner.plan(List.of(selected), ROUTE, "operator", "cause reviewed", 10, reader);
            audit.save(plan); audit.approve(plan.planId(), plan.digest(), "approver", "approved"); return plan;
        }
    }

    ReplayExecutor executor() { return new ReplayExecutor(audit, kafka, List.of(ROUTE)); }
    ReplayAudit.Status execute(ReplayExecutor executor, Plan plan) throws Exception {
        return executor.execute(plan.planId(), plan.digest(), "operator", "explicit recovery", "technical fault removed", 10, 100, 1);
    }

    @Test void positiveReceiptPreservesExactBytesKeyHeadersAndNeverWritesSource() throws Exception {
        Plan plan = approvedPlan(); Item item = plan.items().getFirst();
        long sourceEnd = end(ROUTE.originTopic()); long replayEnd = end(ROUTE.replayTopic());
        var result = execute(executor(), plan);
        assertThat(result.publication()).isEqualTo("PUBLISHED_TO_REPLAY_TOPIC");
        assertThat(result.businessProcessing()).isEqualTo("NOT_VERIFIED");
        var receipt = result.deliveries().getFirst();
        try (var consumer = consumer()) {
            var partition = new TopicPartition(receipt.topic(), receipt.partition());
            consumer.assign(List.of(partition)); consumer.seek(partition, receipt.offset());
            var records = consumer.poll(Duration.ofSeconds(5));
            var record = records.records(partition).stream().filter(r -> r.offset() == receipt.offset()).findFirst().orElseThrow();
            assertThat(sha(record.value())).isEqualTo(item.rawSha256()); assertThat(sha(record.key())).isEqualTo(item.keySha256());
            assertThat(record.partition()).isEqualTo(1);
            assertThat(new String(record.headers().lastHeader(ReplayExecutor.MARKER_HEADER).value(), java.nio.charset.StandardCharsets.UTF_8))
                    .isEqualTo(receipt.marker());
            record.headers().forEach(h -> assertThat(h.key()).doesNotStartWith("kafka_dlt-"));
        }
        assertThat(end(ROUTE.originTopic())).isEqualTo(sourceEnd);
        assertThat(end(ROUTE.replayTopic())).isEqualTo(replayEnd + 1);
        execute(executor(), plan);
        assertThat(end(ROUTE.replayTopic())).isEqualTo(replayEnd + 1);
    }

    @Test void crashAfterBrokerAckRequiresPositiveReconciliationNeverResend() throws Exception {
        Plan plan = approvedPlan(); long before = end(ROUTE.replayTopic());
        var crashing = new ReplayExecutor(audit, kafka, List.of(ROUTE), () -> ReplayExecutor.sender(kafka),
                () -> { throw new AssertionError("simulated process death after ACK"); });
        assertThatThrownBy(() -> execute(crashing, plan)).isInstanceOf(AssertionError.class);
        assertThat(audit.status(plan.planId()).deliveries().getFirst().state()).isEqualTo("PUBLISHING");
        assertThatThrownBy(() -> execute(executor(), plan)).hasMessage("PUBLISH_STATE_REQUIRES_RECONCILIATION");
        assertThat(end(ROUTE.replayTopic())).isEqualTo(before + 1);
        var result = executor().reconcile(plan.planId(), plan.digest(), "operator", "positive broker evidence", 0, 1000);
        assertThat(result.publication()).isEqualTo("PUBLISHED_TO_REPLAY_TOPIC");
        assertThat(executor().reconcile(plan.planId(), plan.digest(), "operator", "repeat review", 0, 1000)).isEqualTo(result);
        execute(executor(), plan); assertThat(end(ROUTE.replayTopic())).isEqualTo(before + 1);
    }

    @Test void timeoutWithoutPositiveEvidenceStaysUnknownAndBlocksRetransmission() throws Exception {
        Plan plan = approvedPlan(); long before = end(ROUTE.replayTopic());
        var uncertain = new ReplayExecutor(audit, kafka, List.of(ROUTE), () -> new ReplayExecutor.Sender() {
            public Coordinate send(ProducerRecord<byte[], byte[]> record) throws Exception { throw new java.util.concurrent.TimeoutException(); }
            public void close() { }
        }, () -> { });
        assertThatThrownBy(() -> execute(uncertain, plan)).hasMessage("PUBLISH_UNKNOWN_RECONCILIATION_REQUIRED");
        var result = executor().reconcile(plan.planId(), plan.digest(), "operator", "bounded evidence search", 0, 1000);
        assertThat(result.deliveries().getFirst().state()).isEqualTo("PUBLISH_UNKNOWN");
        assertThat(result.publication()).isEqualTo("NEEDS_RECONCILIATION");
        assertThatThrownBy(() -> execute(executor(), plan)).hasMessage("PUBLISH_STATE_REQUIRES_RECONCILIATION");
        assertThat(end(ROUTE.replayTopic())).isEqualTo(before);
    }

    @Test void concurrentExecutorsMakeAtMostOneDurableAttempt() throws Exception {
        Plan plan = approvedPlan(); long before = end(ROUTE.replayTopic()); var calls = new AtomicInteger();
        try (var workers = Executors.newFixedThreadPool(2)) {
            var tasks = java.util.stream.IntStream.range(0, 2).mapToObj(i -> workers.submit(() -> {
                var executor = new ReplayExecutor(audit, kafka, List.of(ROUTE), () -> {
                    calls.incrementAndGet(); return ReplayExecutor.sender(kafka);
                }, () -> { });
                try { execute(executor, plan); } catch (IllegalArgumentException claimed) {
                    assertThat(claimed.getMessage()).containsAnyOf("CLAIMED", "RECONCILIATION");
                }
                return true;
            })).toList();
            for (var task : tasks) { task.get(20, TimeUnit.SECONDS); }
        }
        assertThat(calls.get()).isEqualTo(1); assertThat(end(ROUTE.replayTopic())).isEqualTo(before + 1);
    }

    @Test void wrongDigestRouteBoundsAndUnapprovedPlanNeverSend() throws Exception {
        Plan plan = approvedPlan(); long before = end(ROUTE.replayTopic());
        assertThatThrownBy(() -> executor().execute(plan.planId(), "b".repeat(64), "op", "reason", "resolved", 10, 10, 1))
                .hasMessage("APPROVED_DIGEST_REQUIRED");
        assertThatThrownBy(() -> executor().execute(plan.planId(), plan.digest(), "op", "reason", "resolved", 10, 10, 2))
                .hasMessage("INVALID_EXECUTION_BOUND");
        assertThatThrownBy(() -> executor().execute(plan.planId(), plan.digest(), "", "reason", "resolved", 10, 10, 1))
                .hasMessage("INVALID_OPERATOR_TEXT");
        assertThatThrownBy(() -> execute(new ReplayExecutor(audit, kafka, List.of()), plan)).isInstanceOf(IllegalArgumentException.class);
        assertThat(audit.status(plan.planId()).deliveries()).isEmpty();
        assertThat(end(ROUTE.replayTopic())).isEqualTo(before);
    }

    @Test void sourceRetentionExpiryRefusesBeforeIntent() throws Exception {
        Plan plan = approvedPlan(); Item item = plan.items().getFirst();
        admin.deleteRecords(Map.of(new TopicPartition(item.dlt().topic(), 1),
                org.apache.kafka.clients.admin.RecordsToDelete.beforeOffset(item.dlt().offset() + 1))).all().get(10, TimeUnit.SECONDS);
        assertThatThrownBy(() -> execute(executor(), plan)).hasMessage("DLT_COORDINATE_UNAVAILABLE");
        assertThat(audit.status(plan.planId()).deliveries()).isEmpty();
    }

    @Test void realUnreachableProducerAfterPreflightLeavesUnknownWithoutFalseReceipt() throws Exception {
        Plan plan = approvedPlan();
        int port;
        try (var socket = new java.net.ServerSocket(0)) { port = socket.getLocalPort(); }
        var disconnected = new java.util.Properties(); disconnected.putAll(kafka);
        disconnected.setProperty("bootstrap.servers", "127.0.0.1:" + port);
        var broken = new ReplayExecutor(audit, kafka, List.of(ROUTE), () -> {
            return ReplayExecutor.sender(disconnected);
        }, () -> { });
        assertThatThrownBy(() -> execute(broken, plan)).hasMessage("PUBLISH_UNKNOWN_RECONCILIATION_REQUIRED");
        var delivery = audit.status(plan.planId()).deliveries().getFirst();
        assertThat(delivery.state()).isEqualTo("PUBLISH_UNKNOWN"); assertThat(delivery.offset()).isNull();
        assertThatThrownBy(() -> execute(executor(), plan)).hasMessage("PUBLISH_STATE_REQUIRES_RECONCILIATION");
    }

    @Test void unavailableAuditDatabaseStopsBeforeKafkaPublication() throws Exception {
        Plan plan = approvedPlan(); long before = end(ROUTE.replayTopic());
        int port;
        try (var socket = new java.net.ServerSocket(0)) { port = socket.getLocalPort(); }
        var unavailable = new ReplayAudit("jdbc:postgresql://127.0.0.1:" + port + "/postgres?connectTimeout=1", "postgres", "");
        var blocked = new ReplayExecutor(unavailable, kafka, List.of(ROUTE));
        assertThatThrownBy(() -> execute(blocked, plan)).isInstanceOf(java.sql.SQLException.class);
        assertThat(end(ROUTE.replayTopic())).isEqualTo(before);
        assertThat(audit.status(plan.planId()).deliveries()).isEmpty();
    }

    @Test void appliedV1AuditUpgradesWithoutChangingPlanOrApproval() throws Exception {
        try (var oldDb = io.zonky.test.db.postgres.embedded.EmbeddedPostgres.builder().start()) {
            String url = oldDb.getJdbcUrl("postgres", "postgres");
            org.flywaydb.core.Flyway.configure().dataSource(url, "postgres", "").locations("classpath:db/replay")
                    .target(org.flywaydb.core.api.MigrationVersion.fromVersion("1")).load().migrate();
            var oldAudit = new ReplayAudit(url, "postgres", "");
            var plan = new Plan(1, UUID.randomUUID(), java.time.Instant.now().toString(), "operator", "upgrade", 10,
                    List.of(item(UUID.randomUUID(), 0, 0)));
            oldAudit.save(plan);
            try (var c = oldDb.getPostgresDatabase().getConnection(); var update = c.prepareStatement(
                    "UPDATE replay_plan SET status='APPROVED',approval_actor='old-approver',approval_reason='approved',"
                            + "approval_at=now(),approval_digest=? WHERE id=?")) {
                update.setString(1, plan.digest()); update.setObject(2, plan.planId()); update.executeUpdate();
            }
            oldAudit.initialize();
            assertThat(oldAudit.approved(plan.planId(), plan.digest())).isEqualTo(plan);
            assertThat(oldAudit.status(plan.planId()).publication()).isEqualTo("NOT_PUBLISHED");
        }
    }

    private long end(String topic) throws Exception {
        return admin.listOffsets(Map.of(new TopicPartition(topic, 1), org.apache.kafka.clients.admin.OffsetSpec.latest()))
                .all().get(10, TimeUnit.SECONDS).values().iterator().next().offset();
    }
    private KafkaConsumer<byte[], byte[]> consumer() {
        return new KafkaConsumer<>(Map.of("bootstrap.servers", broker.getBrokersAsString(), "enable.auto.commit", false,
                "allow.auto.create.topics", false), new ByteArrayDeserializer(), new ByteArrayDeserializer());
    }
}
