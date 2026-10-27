package com.apex.replay;

import static com.apex.replay.ReplayModel.*;
import static com.apex.replay.ReplayValidationTest.*;
import static org.assertj.core.api.Assertions.*;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import io.zonky.test.db.postgres.embedded.EmbeddedPostgres;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.kafka.test.EmbeddedKafkaKraftBroker;

/** Actual packaged operator CLI processes; this is NOT a two-Cell replay E2E. */
@Tag("integration")
class PackagedReplayCliIT {
    @TempDir Path temp;

    @Test void independentJarSupportsInspectPlanApproveAndRefusesExecute() throws Exception {
        var broker = new EmbeddedKafkaKraftBroker(1, 2, "source", "source-dlt", "inventory-replay", "inventory-replay-dlt");
        broker.brokerProperty("auto.create.topics.enable", "false");
        try (var db = EmbeddedPostgres.builder().start()) {
            try {
                broker.afterPropertiesSet();
                var env = Map.of("REPLAY_KAFKA_BOOTSTRAP_SERVERS", broker.getBrokersAsString(),
                        "REPLAY_AUDIT_DB_URL", db.getJdbcUrl("postgres", "postgres"),
                        "REPLAY_AUDIT_DB_USERNAME", "postgres", "REPLAY_AUDIT_DB_PASSWORD", "");
                assertThat(invoke(env, "init-audit")).contains("AUDIT_SCHEMA_INITIALIZED");
                UUID eventId = UUID.randomUUID(); var source = record(eventId, 1, 0, 7);
                var props = new Properties(); props.setProperty("bootstrap.servers", broker.getBrokersAsString());
                props.setProperty("acks", "all"); props.setProperty("enable.idempotence", "true");
                Selection selected;
                try (var producer = new KafkaProducer<byte[], byte[]>(props, new ByteArraySerializer(), new ByteArraySerializer())) {
                    var ack = producer.send(new ProducerRecord<byte[], byte[]>("source-dlt", 1, null,
                            source.key(), source.value(), source.headers())).get(10, TimeUnit.SECONDS);
                    selected = new Selection(new Coordinate(ack.topic(), ack.partition(), ack.offset()), eventId, sha(source.value()));
                }
                var routes = temp.resolve("routes.json"); Files.writeString(routes, JSON.writeValueAsString(List.of(ROUTE)));
                var manifest = temp.resolve("manifest.json"); Files.writeString(manifest, JSON.writeValueAsString(List.of(selected)));
                String inspection = invoke(env, "inspect", "--routes", routes.toString(), "--dlt-topic", "source-dlt",
                        "--partition", "1", "--offset", Long.toString(selected.dlt().offset()), "--allow-kafka-interaction");
                assertThat(inspection).contains(eventId.toString(), selected.rawSha256()).doesNotContain("نام فارسی");
                var planned = JSON.readTree(invoke(env, "plan", "--routes", routes.toString(), "--selector", manifest.toString(),
                        "--target-group", ROUTE.targetGroup(), "--replay-topic", ROUTE.replayTopic(), "--actor", "operator",
                        "--reason", "reviewed cause", "--limit", "10", "--allow-kafka-interaction"));
                String id = planned.path("planId").asText(); String digest = planned.path("digest").asText();
                assertThat(invoke(env, "approve", "--plan-id", id, "--confirm-digest", digest,
                        "--actor", "approver", "--reason", "explicit review")).contains("APPROVED");
                assertThat(invoke(env, "status", "--plan-id", id)).contains("APPROVED", "NOT_PUBLISHED");
                assertThat(invoke(env, "execute")).contains("REQUIRED_OPTIONS_MISSING");
                var execute = List.of("execute", "--plan-id", id, "--confirm-digest", digest, "--routes", routes.toString(),
                        "--target-group", ROUTE.targetGroup(), "--replay-topic", ROUTE.replayTopic(), "--actor", "operator",
                        "--reason", "explicit send", "--failure-resolved", "technical cause removed", "--max-records", "10",
                        "--rate", "10", "--attempts", "1", "--allow-kafka-interaction");
                assertThat(invoke(env, execute.toArray(String[]::new))).contains("PUBLISHED_TO_REPLAY_TOPIC");
                assertThat(invoke(env, execute.toArray(String[]::new))).contains("PUBLISHED_TO_REPLAY_TOPIC");
                assertThat(invoke(env, "reconcile", "--plan-id", id, "--confirm-digest", digest, "--routes", routes.toString(),
                        "--target-group", ROUTE.targetGroup(), "--replay-topic", ROUTE.replayTopic(), "--actor", "operator",
                        "--reason", "verify receipt", "--from-offset", "0", "--max-scan", "100", "--allow-kafka-interaction"))
                        .contains("PUBLISHED_TO_REPLAY_TOPIC");
                // Test-only crash seam after a real broker ACK; fresh packaged CLI must not resend.
                UUID crashId = UUID.randomUUID(); var crashedSource = record(crashId, 1, 0, 8);
                try (var producer = new KafkaProducer<byte[], byte[]>(props, new ByteArraySerializer(), new ByteArraySerializer())) {
                    var ack = producer.send(new ProducerRecord<byte[], byte[]>("source-dlt", 1, null,
                            crashedSource.key(), crashedSource.value(), crashedSource.headers())).get(10, TimeUnit.SECONDS);
                    Files.writeString(manifest, JSON.writeValueAsString(List.of(new Selection(
                            new Coordinate(ack.topic(), 1, ack.offset()), crashId, sha(crashedSource.value())))));
                }
                var crashPlan = JSON.readTree(invoke(env, "plan", "--routes", routes.toString(), "--selector", manifest.toString(),
                        "--target-group", ROUTE.targetGroup(), "--replay-topic", ROUTE.replayTopic(), "--actor", "operator",
                        "--reason", "crash boundary test", "--limit", "10", "--allow-kafka-interaction"));
                String crashPlanId = crashPlan.path("planId").asText(); String crashDigest = crashPlan.path("digest").asText();
                invoke(env, "approve", "--plan-id", crashPlanId, "--confirm-digest", crashDigest,
                        "--actor", "approver", "--reason", "explicit crash test approval");
                var audit = new ReplayAudit(db.getJdbcUrl("postgres", "postgres"), "postgres", "");
                var crash = new ReplayExecutor(audit, props, List.of(ROUTE), () -> ReplayExecutor.sender(props),
                        () -> { throw new AssertionError("test process lost before audit receipt"); });
                assertThatThrownBy(() -> crash.execute(UUID.fromString(crashPlanId), crashDigest, "operator", "test crash", "fault fixed", 10, 10, 1))
                        .isInstanceOf(AssertionError.class);
                assertThat(invokeExpected(env, 2, "execute", "--plan-id", crashPlanId, "--confirm-digest", crashDigest,
                        "--routes", routes.toString(), "--target-group", ROUTE.targetGroup(), "--replay-topic", ROUTE.replayTopic(),
                        "--actor", "operator", "--reason", "fresh process retry refused", "--failure-resolved", "reviewed",
                        "--max-records", "10", "--rate", "10", "--attempts", "1", "--allow-kafka-interaction"))
                        .contains("RECONCILIATION");
                assertThat(invoke(env, "reconcile", "--plan-id", crashPlanId, "--confirm-digest", crashDigest,
                        "--routes", routes.toString(), "--target-group", ROUTE.targetGroup(), "--replay-topic", ROUTE.replayTopic(),
                        "--actor", "operator", "--reason", "verify real marker and hash", "--from-offset", "0",
                        "--max-scan", "100", "--allow-kafka-interaction")).contains("PUBLISHED_TO_REPLAY_TOPIC");
            } finally { broker.destroy(); }
        }
    }

    private String invoke(Map<String, String> env, String... arguments) throws Exception {
        return invokeExpected(env, arguments.length == 1 && arguments[0].equals("execute") ? 2 : 0, arguments);
    }

    private String invokeExpected(Map<String, String> env, int expectedExit, String... arguments) throws Exception {
        Path jar = Path.of(System.getProperty("replay.tool.jar")).toAbsolutePath();
        assertThat(jar).isRegularFile();
        String executable = System.getProperty("os.name").startsWith("Windows") ? "java.exe" : "java";
        var command = new ArrayList<>(List.of(Path.of(System.getProperty("java.home"), "bin", executable).toString(),
                "-Xmx128m", "-Dfile.encoding=UTF-8", "-jar", jar.toString()));
        command.addAll(List.of(arguments));
        Path log = temp.resolve("cli-" + UUID.randomUUID() + ".log");
        var builder = new ProcessBuilder(command).redirectErrorStream(true).redirectOutput(log.toFile());
        builder.environment().keySet().removeIf(key -> key.startsWith("REPLAY_") || key.startsWith("SPRING_")
                || key.equals("JAVA_TOOL_OPTIONS") || key.equals("JDK_JAVA_OPTIONS") || key.equals("_JAVA_OPTIONS"));
        builder.environment().putAll(env);
        var process = builder.start();
        try {
            assertThat(process.waitFor(30, TimeUnit.SECONDS)).isTrue();
            String output = Files.readString(log);
            assertThat(process.exitValue()).as(output).isEqualTo(expectedExit);
            return output.lines().filter(line -> !line.isBlank()).reduce((a, b) -> b).orElseThrow();
        } finally {
            if (process.isAlive()) { process.destroy(); if (!process.waitFor(5, TimeUnit.SECONDS)) {
                process.destroyForcibly(); process.waitFor(5, TimeUnit.SECONDS);
            } }
        }
    }
}
