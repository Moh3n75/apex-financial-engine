package com.apex.inventory;

import static org.assertj.core.api.Assertions.assertThat;
import io.zonky.test.db.postgres.embedded.EmbeddedPostgres;
import java.nio.file.*;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.admin.Admin;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.test.EmbeddedKafkaKraftBroker;

/** Real prerequisite gate using the same embedded runtime and certificates as M36 live tests. */
@Tag("integration")
class ControlCenterLivePrerequisitesIT {
  @Test void startsOwnedPostgresKafkaAndMtlsMaterial() throws Exception {
    Path root = Path.of(System.getProperty("saga.repositoryRoot", "..")).toAbsolutePath().normalize();
    Path output = root.resolve("out/control-center-live"); Files.createDirectories(output);
    var receipt = new LinkedHashMap<String, Object>();
    receipt.put("startedAt", Instant.now().toString());
    receipt.put("sourceMode", "LIVE_PREREQUISITE_CHECK");
    receipt.put("cellsContacted", List.of());
    receipt.put("postgres", "NOT_STARTED"); receipt.put("kafka", "NOT_STARTED"); receipt.put("mtlsMaterial", "NOT_STARTED");
    String stage = "POSTGRES_START";
    var broker = new EmbeddedKafkaKraftBroker(1, 1, "apex.control-center.prerequisite.v1");
    broker.brokerListProperty("apex.control-center.test.brokers");
    boolean brokerStarted = false;
    try {
      try (var pg = EmbeddedPostgres.builder().setConnectConfig("sslmode", "disable").start()) {
        assertThat(new JdbcTemplate(pg.getPostgresDatabase()).queryForObject("SELECT 1", Integer.class)).isEqualTo(1);
        receipt.put("postgres", "PASS_REAL_SQL");
        stage = "KAFKA_START";
        brokerStarted = true; broker.afterPropertiesSet();
        try (var admin = Admin.create(Map.of("bootstrap.servers", broker.getBrokersAsString()))) {
          assertThat(admin.listTopics().names().get(15, TimeUnit.SECONDS)).contains("apex.control-center.prerequisite.v1");
        }
        receipt.put("kafka", "PASS_REAL_ADMIN_METADATA");
        stage = "MTLS_MATERIAL";
        try (var certificates = new Ops03bCertificates()) {
          assertThat(certificates.server).isRegularFile(); assertThat(certificates.primary).isRegularFile();
          certificates.context(certificates.primary, certificates.trust);
        }
        receipt.put("mtlsMaterial", "PASS_EPHEMERAL_CA_AND_SSL_CONTEXT");
        receipt.put("verdict", "PREREQUISITES_PASS_NOT_FIVE_CELL_ACCEPTANCE");
      }
    } catch (Exception | AssertionError failure) {
      receipt.put("verdict", "BLOCKED"); receipt.put("failedStage", stage);
      receipt.put("failureClass", failure.getClass().getName());
      // Avoid exception messages: runtime diagnostics can contain connection strings or credentials.
      throw failure;
    } finally {
      if (brokerStarted) broker.destroy();
      receipt.put("completedAt", Instant.now().toString());
      Files.writeString(output.resolve("prerequisites.json"), OrderInventorySagaIT.JSON.writerWithDefaultPrettyPrinter().writeValueAsString(receipt));
    }
  }
}
