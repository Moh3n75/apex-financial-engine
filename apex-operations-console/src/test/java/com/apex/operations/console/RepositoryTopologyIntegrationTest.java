package com.apex.operations.console;

import static org.assertj.core.api.Assertions.*;
import com.apex.operations.contracts.*;
import java.nio.file.*;
import java.time.Instant;
import java.util.*;
import org.junit.jupiter.api.Test;

/** Cross-module integration of generated repository declarations. No live process is implied. */
class RepositoryTopologyIntegrationTest {
  @Test void joinsTheFiveActualCellContractsAndExportsExample() throws Exception {
    var mapper = tools.jackson.databind.json.JsonMapper.builder().build();
    var views = new ArrayList<ContractDiscoveryPoller.View>();
    for (String cell : List.of("order-cell", "inventory-cell", "payment-cell", "customer-cell", "reporting-cell")) {
      Path path = Path.of("src/test/resources/contracts", cell + ".json");
      if (Boolean.getBoolean("apex.verifyRepositoryManifests")) path = Path.of("..", cell, "target/contract-manifest.json");
      var manifest = mapper.readValue(Files.readString(path), ContractManifest.class);
      assertThat(manifest.sourceMode()).isEqualTo("STATIC_DECLARATION");
      assertThat(manifest.service()).isEqualTo(cell);
      views.add(new ContractDiscoveryPoller.View(manifest.environment(), cell, manifest, "AVAILABLE"));
    }
    var graph = ContractTopology.join(TopologyApi.project(List.of(), Instant.now(), (e,o) -> true), views, (e,o) -> true);
    for (String contract : List.of("ORDER_PLACED:1", "INVENTORY_RESERVED:1", "INVENTORY_RESERVATION_REJECTED:1",
        "ORDER_READY_FOR_PAYMENT:1", "PAYMENT_AUTHORIZED:1", "PAYMENT_DECLINED:1", "ORDER_CANCELLATION_REQUESTED:1",
        "INVENTORY_RELEASED:1", "CUSTOMER_CREATED:1")) {
      assertThat(graph.connections().stream().filter(e -> e.contract().equals(contract))).hasSize(3);
    }
    assertThat(graph.connections()).hasSize(28); // nine full chains and one unconsumed legacy publication.
    assertThat(graph.connections()).noneSatisfy(e -> assertThat(e.protocol()).isEqualTo("GRPC"));
    assertThat(graph.nodes()).allSatisfy(n -> assertThat(n.health()).isEqualTo("UNKNOWN"));
    var json = mapper.writerWithDefaultPrettyPrinter().writeValueAsString(graph);
    assertThat(json).doesNotContain("password", "bootstrap-servers", "jdbc:", "payload");
    Files.createDirectories(Path.of("target"));
    Files.writeString(Path.of("target/repository-topology-example.json"), json);
  }
}
