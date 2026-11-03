package com.apex.order.support;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.DescribeClusterResult;
import org.awaitility.Awaitility;
import org.springframework.kafka.test.EmbeddedKafkaKraftBroker;

/**
 * KRaft fixture helpers. Topic creation must not run until the quorum controller accepts metadata
 * RPCs; otherwise {@code createTopics} can sit on the controller event queue for &gt;10s (the
 * EmbeddedKafka adminTimeout) while heartbeats pile up.
 */
public final class EmbeddedKafkaSupport {
  private EmbeddedKafkaSupport() {}

  public static void awaitController(EmbeddedKafkaKraftBroker broker) {
    Awaitility.await()
        .atMost(Duration.ofSeconds(30))
        .pollInterval(Duration.ofMillis(100))
        .ignoreExceptions()
        .until(
            () -> {
              try (Admin admin =
                  Admin.create(Map.of("bootstrap.servers", broker.getBrokersAsString()))) {
                DescribeClusterResult cluster = admin.describeCluster();
                if (cluster.nodes().get(5, TimeUnit.SECONDS).isEmpty()) {
                  return false;
                }
                return cluster.controller().get(5, TimeUnit.SECONDS) != null;
              }
            });
  }
}
