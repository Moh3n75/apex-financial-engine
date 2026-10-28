package com.apex.order.integration;

import static org.assertj.core.api.Assertions.*;

import com.apex.order.support.DisposableOrderRuntime;
import com.apex.platform.grpc.api.*;
import java.time.Instant;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.*;
import org.springframework.jdbc.core.JdbcTemplate;

@Tag("integration")
class OrderReceiptVersionTest {
  @Test
  void retainedOperationCannotBeReusedByANewContractVersionOrPrincipal() throws Exception {
    try (var runtime =
        DisposableOrderRuntime.start(
            false,
            "--spring.profiles.active=grpc",
            "--spring.grpc.server.enabled=false",
            "--spring.grpc.server.address=127.0.0.1",
            "--apex.grpc.insecure-loopback-test-only=true",
            "--apex.grpc.test-clients[0]=client")) {
      var executor = runtime.application().getBean(TransactionalRequestExecutor.class);
      var jdbc = runtime.application().getBean(JdbcTemplate.class);
      var executions = new AtomicInteger();
      assertThat(
              executor.execute(
                  context(1, "client-a", "tenant-a"),
                  "b".repeat(64),
                  () -> {
                    executions.incrementAndGet();
                    return new byte[] {1};
                  }))
          .containsExactly((byte) 1);
      jdbc.update(
          "UPDATE order_request_receipt SET created_at=TIMESTAMPTZ '2020-01-01 00:00:00Z' WHERE"
              + " idempotency_key='retained-key'");
      assertThat(
              executor.execute(
                  context(1, "client-a", "tenant-a"),
                  "b".repeat(64),
                  () -> {
                    throw new AssertionError("Cached operation must not run again");
                  }))
          .containsExactly((byte) 1);
      for (var changed :
          new RequestContext[] {
            context(2, "client-a", "tenant-a"),
            context(1, "client-b", "tenant-a"),
            context(1, "client-a", "tenant-b")
          })
        assertThatThrownBy(
                () ->
                    executor.execute(
                        changed,
                        "b".repeat(64),
                        () -> {
                          throw new AssertionError("Conflicting operation must not execute");
                        }))
            .isInstanceOfSatisfying(
                GatewayException.class,
                e -> assertThat(e.code()).isEqualTo(GatewayException.Code.ALREADY_EXISTS));
      assertThat(executions.get()).isEqualTo(1);
      assertThat(jdbc.queryForObject("SELECT count(*) FROM order_request_receipt", Long.class))
          .isEqualTo(1);
    }
  }

  static RequestContext context(int version, String principal, String tenant) {
    return new RequestContext(
        UUID.randomUUID(),
        "retained-key",
        "CREATE_ORDER",
        version,
        UUID.randomUUID(),
        null,
        "client",
        "order-cell",
        tenant,
        principal,
        Instant.now().plusSeconds(5));
  }
}
