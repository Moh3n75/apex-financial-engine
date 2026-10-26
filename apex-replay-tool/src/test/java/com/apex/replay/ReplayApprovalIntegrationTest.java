package com.apex.replay;

import static com.apex.replay.ReplayModel.*;
import static com.apex.replay.ReplayValidationTest.*;
import static org.assertj.core.api.Assertions.*;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.DriverManager;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import io.zonky.test.db.postgres.embedded.EmbeddedPostgres;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.kafka.test.EmbeddedKafkaKraftBroker;

@Tag("integration")
class ReplayApprovalIntegrationTest {
    static EmbeddedPostgres db;
    static EmbeddedKafkaKraftBroker broker;
    static ReplayAudit audit;
    static Admin admin;
    static Properties kafka;
    static Map<String, String> environment;
    @TempDir Path temp;

    @BeforeAll static void start() throws Exception {
        try {
            db = EmbeddedPostgres.builder().start();
            audit = new ReplayAudit(db.getJdbcUrl("postgres", "postgres"), "postgres", "");
            audit.initialize();
            broker = new EmbeddedKafkaKraftBroker(1, 2, "source", "source-dlt", "inventory-replay", "inventory-replay-dlt");
            broker.brokerProperty("auto.create.topics.enable", "false"); broker.afterPropertiesSet();
            kafka = new Properties(); kafka.setProperty("bootstrap.servers", broker.getBrokersAsString());
            admin = Admin.create(kafka);
            environment = Map.of("REPLAY_KAFKA_BOOTSTRAP_SERVERS", broker.getBrokersAsString(),
                    "REPLAY_AUDIT_DB_URL", db.getJdbcUrl("postgres", "postgres"),
                    "REPLAY_AUDIT_DB_USERNAME", "postgres", "REPLAY_AUDIT_DB_PASSWORD", "");
        } catch (Exception failure) { stop(); throw failure; }
    }

    @AfterAll static void stop() {
        try { if (admin != null) { admin.close(Duration.ofSeconds(2)); } }
        finally { try { if (broker != null) { broker.destroy(); } }
            finally { if (db != null) { try { db.close(); } catch (Exception failure) { throw new RuntimeException(failure); } } } }
    }

    static Selection publishDlt(UUID id) throws Exception {
        var original = record(id, 1, 0, Math.abs(UUID.randomUUID().getMostSignificantBits() % 1000000));
        var props = new Properties(); props.putAll(kafka); props.put("acks", "all"); props.put("enable.idempotence", true);
        try (var producer = new KafkaProducer<byte[], byte[]>(props, new ByteArraySerializer(), new ByteArraySerializer())) {
            var record = new ProducerRecord<byte[], byte[]>("source-dlt", 1, null, original.key(), original.value(), original.headers());
            var ack = producer.send(record).get(10, TimeUnit.SECONDS);
            return new Selection(new Coordinate(ack.topic(), ack.partition(), ack.offset()), id, sha(original.value()));
        }
    }

    @Test void realInspectPlanApproveHaveZeroKafkaWritesAndNoGroupOffsets() throws Exception {
        var selected = publishDlt(UUID.randomUUID());
        String group = ROUTE.targetGroup();
        var offsetsBefore = admin.listConsumerGroupOffsets(group).partitionsToOffsetAndMetadata().get(10, TimeUnit.SECONDS);
        var endsBefore = ends();
        var routes = temp.resolve("routes.json"); Files.writeString(routes, JSON.writeValueAsString(List.of(ROUTE)));
        var manifest = temp.resolve("manifest.json"); Files.writeString(manifest, JSON.writeValueAsString(List.of(selected)));
        var inspected = cli("inspect", "--routes", routes.toString(), "--dlt-topic", "source-dlt", "--partition", "1",
                "--offset", Long.toString(selected.dlt().offset()), "--allow-kafka-interaction");
        assertThat(inspected.code()).isZero();
        var inspection = JSON.readTree(inspected.output());
        assertThat(inspection.path("eventId").asText()).isEqualTo(selected.eventId().toString());
        assertThat(inspection.path("rawSha256").asText()).isEqualTo(selected.rawSha256());
        assertThat(inspected.output()).doesNotContain("نام فارسی", "payload", "password");
        var planned = cli("plan", "--routes", routes.toString(), "--selector", manifest.toString(), "--target-group", group,
                "--replay-topic", ROUTE.replayTopic(), "--actor", "claimed-operator", "--reason", "cause reviewed", "--limit", "10",
                "--allow-kafka-interaction");
        assertThat(planned.code()).as(planned.output()).isZero();
        var plan = JSON.readTree(planned.output()); UUID id = UUID.fromString(plan.path("planId").asText());
        String digest = plan.path("digest").asText();
        assertThat(audit.status(id).status()).isEqualTo("PLANNED");
        assertThat(cli("approve", "--plan-id", id.toString(), "--confirm-digest", "a".repeat(64),
                "--actor", "approver", "--reason", "wrong").code()).isEqualTo(2);
        assertThat(audit.status(id).status()).isEqualTo("PLANNED");
        assertThat(cli("approve", "--plan-id", id.toString(), "--confirm-digest", digest,
                "--actor", "approver", "--reason", "explicit approval").code()).isZero();
        assertThat(cli("status", "--plan-id", id.toString()).output()).contains("APPROVED", "NOT_IMPLEMENTED_ZERO_PUBLICATIONS");
        assertThat(ends()).isEqualTo(endsBefore);
        assertThat(admin.listConsumerGroupOffsets(group).partitionsToOffsetAndMetadata().get(10, TimeUnit.SECONDS)).isEqualTo(offsetsBefore);
        assertThat(admin.listConsumerGroups().all().get(10, TimeUnit.SECONDS)).noneMatch(g -> g.groupId().equals(group));
        assertThat(actionCount(id)).isEqualTo(2);
        Files.createDirectories(Path.of("target"));
        Files.writeString(Path.of("target/replay-approval-evidence.json"), JSON.writeValueAsString(Map.of(
                "planId", id, "digest", digest, "selectedDlt", selected.dlt(), "eventId", selected.eventId(),
                "group", group, "broker", broker.getBrokersAsString(), "status", "APPROVED", "publications", 0,
                "kafkaEndOffsetsUnchanged", true, "groupOffsetsUnchanged", true)));
    }

    @Test void duplicatePlansAndChangedHashesCannotCreateSecondAuditItem() throws Exception {
        var selected = publishDlt(UUID.randomUUID());
        try (var reader = new DltReader(kafka, List.of(ROUTE))) {
            var plan = ReplayPlanner.plan(List.of(selected, selected), ROUTE, "operator", "dedupe", 10, reader);
            assertThat(plan.items()).hasSize(1); audit.save(plan);
            var second = ReplayPlanner.plan(List.of(selected), ROUTE, "operator", "duplicate", 10, reader);
            assertThatThrownBy(() -> audit.save(second)).isInstanceOf(java.sql.SQLException.class);
            assertThatThrownBy(() -> audit.status(second.planId())).hasMessage("PLAN_NOT_FOUND");
            assertThatThrownBy(() -> reader.validate(new Selection(selected.dlt(), selected.eventId(), "a".repeat(64)), ROUTE))
                    .hasMessage("RAW_HASH_MISMATCH");
        }
    }

    @Test void concurrentApprovalAllowsOneActionAndAuditCannotBeRewritten() throws Exception {
        var plan = new Plan(1, UUID.randomUUID(), java.time.Instant.now().toString(), "actor", "reason", 10,
                List.of(item(UUID.randomUUID(), 0, Math.abs(UUID.randomUUID().getMostSignificantBits() % 1000000))));
        audit.save(plan);
        try (var workers = Executors.newFixedThreadPool(2)) {
            var tasks = java.util.stream.IntStream.range(0, 2).mapToObj(i -> workers.submit(() -> {
                try { audit.approve(plan.planId(), plan.digest(), "approver" + i, "reviewed"); return true; }
                catch (IllegalArgumentException refused) { return false; }
            })).toList();
            int successes = 0;
            for (var task : tasks) { if (task.get(10, TimeUnit.SECONDS)) { successes++; } }
            assertThat(successes).isEqualTo(1);
        }
        assertThat(actionCount(plan.planId())).isEqualTo(2);
        try (var connection = db.getPostgresDatabase().getConnection(); var statement = connection.createStatement()) {
            assertThatThrownBy(() -> statement.executeUpdate("UPDATE replay_plan SET reason='rewritten' WHERE id='" + plan.planId() + "'"))
                    .isInstanceOf(java.sql.SQLException.class);
            assertThatThrownBy(() -> statement.executeUpdate("DELETE FROM replay_action WHERE plan_id='" + plan.planId() + "'"))
                    .isInstanceOf(java.sql.SQLException.class);
            assertThatThrownBy(() -> statement.executeUpdate("UPDATE replay_item SET raw_sha256='" + "b".repeat(64)
                    + "' WHERE plan_id='" + plan.planId() + "'" )).isInstanceOf(java.sql.SQLException.class);
        }
    }

    @Test void requiredOptInLimitsAndUnsupportedCommandsRefuseWithoutInfrastructure() {
        assertThat(cli("execute").output()).contains("NOT_IMPLEMENTED");
        assertThat(cli("reconcile").code()).isEqualTo(2);
        assertThat(cli("inspect", "--routes", "missing", "--dlt-topic", "source-dlt", "--partition", "1", "--offset", "0").output())
                .contains("REQUIRED_OPTIONS_MISSING");
        assertThat(cli("status", "--plan-id", UUID.randomUUID().toString(), "--unexpected", "x").code()).isEqualTo(2);
        assertThat(cli("approve", "--plan-id", UUID.randomUUID().toString(), "--confirm-digest", "x",
                "--actor", "op", "--reason", "reason").code()).isEqualTo(2);
    }

    @Test void missingAndUndersizedReplayTopicsAndDltFailPreflight() throws Exception {
        String source = "missing-" + UUID.randomUUID();
        admin.createTopics(List.of(new NewTopic(source, 2, (short) 1), new NewTopic(source + "-dlt", 2, (short) 1)))
                .all().get(10, TimeUnit.SECONDS);
        var route = new Route(source, source + "-dlt", "another-test-group", source + "-replay", source + "-replay-dlt", List.of("ORDER_PLACED"));
        try (var reader = new DltReader(kafka, List.of(route))) {
            assertThatThrownBy(() -> reader.preflight(route)).hasMessage("TOPIC_PREFLIGHT_FAILED");
            admin.createTopics(List.of(new NewTopic(route.replayTopic(), 1, (short) 1),
                    new NewTopic(route.replayDltTopic(), 2, (short) 1))).all().get(10, TimeUnit.SECONDS);
            assertThatThrownBy(() -> reader.preflight(route)).hasMessage("INSUFFICIENT_PARTITIONS");
        }
    }

    @Test void refusesInitializingApplicationDatabase() throws Exception {
        try (var other = EmbeddedPostgres.builder().start(); var connection = other.getPostgresDatabase().getConnection();
             var statement = connection.createStatement()) {
            statement.execute("CREATE TABLE orders(id UUID PRIMARY KEY)");
            var unsafe = new ReplayAudit(other.getJdbcUrl("postgres", "postgres"), "postgres", "");
            assertThatThrownBy(unsafe::initialize).hasMessage("AUDIT_DATABASE_NOT_EMPTY_OR_OWNED");
            try (var result = statement.executeQuery("SELECT count(*) FROM pg_tables WHERE schemaname='public'")) {
                result.next(); assertThat(result.getInt(1)).isEqualTo(1);
            }
        }
    }

    private static int actionCount(UUID id) throws Exception {
        try (var connection = db.getPostgresDatabase().getConnection();
             var statement = connection.prepareStatement("SELECT count(*) FROM replay_action WHERE plan_id=?")) {
            statement.setObject(1, id); try (var row = statement.executeQuery()) { row.next(); return row.getInt(1); }
        }
    }

    private static Map<TopicPartition, Long> ends() throws Exception {
        var request = new java.util.HashMap<TopicPartition, org.apache.kafka.clients.admin.OffsetSpec>();
        for (String topic : List.of("source", "source-dlt", "inventory-replay", "inventory-replay-dlt")) {
            for (int partition = 0; partition < 2; partition++) {
                request.put(new TopicPartition(topic, partition), org.apache.kafka.clients.admin.OffsetSpec.latest());
            }
        }
        var offsets = admin.listOffsets(request).all().get(10, TimeUnit.SECONDS);
        var result = new java.util.HashMap<TopicPartition, Long>(); offsets.forEach((key, value) -> result.put(key, value.offset()));
        return result;
    }

    record CommandResult(int code, String output) { }
    private static CommandResult cli(String... args) {
        var bytes = new ByteArrayOutputStream();
        int code = ReplayToolApplication.run(args, environment, new PrintStream(bytes, true, StandardCharsets.UTF_8));
        return new CommandResult(code, bytes.toString(StandardCharsets.UTF_8).strip());
    }
}
