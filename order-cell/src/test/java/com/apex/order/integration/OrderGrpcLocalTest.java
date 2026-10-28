package com.apex.order.integration;

import static org.assertj.core.api.Assertions.*;

import com.apex.order.contracts.*;
import com.apex.order.modules.order.application.PlaceOrderUseCase;
import com.apex.order.support.DisposableOrderRuntime;
import com.apex.platform.core.context.*;
import com.apex.platform.grpc.api.*;
import java.time.Instant;
import java.util.*;
import org.junit.jupiter.api.*;
import org.springframework.jdbc.core.JdbcTemplate;

@Tag("integration")
class OrderGrpcLocalTest {
  @Test
  void legacyProfileHasNoGrpcEndpointOrReceiptMutation() throws Exception {
    try (var runtime = DisposableOrderRuntime.start()) {
      assertThat(runtime.application().getBeansOfType(io.grpc.BindableService.class)).isEmpty();
      assertThat(
              runtime
                  .application()
                  .getBeansOfType(org.springframework.grpc.server.GrpcServerFactory.class))
          .isEmpty();
      assertThat(
              runtime
                  .application()
                  .getBean(JdbcTemplate.class)
                  .queryForObject("SELECT count(*) FROM order_request_receipt", Long.class))
          .isZero();
    }
  }

  @Test
  void localQueryUsesOwningRepositoryAndIdempotencySpiFailsClosedOnConflicts() throws Exception {
    try (var runtime =
        DisposableOrderRuntime.start(
            false,
            "--spring.profiles.active=grpc",
            "--spring.grpc.server.enabled=false",
            "--spring.grpc.server.address=127.0.0.1",
            "--apex.grpc.insecure-loopback-test-only=true",
            "--apex.grpc.test-clients[0]=m31-client")) {
      var app = runtime.application();
      var context = app.getBean(ExecutionContextProvider.class);
      UUID id;
      try (var scope = context.open(ExecutionContext.root("order-cell", "order-cell"))) {
        id =
            app.getBean(PlaceOrderUseCase.class)
                .execute(
                    "local-" + UUID.randomUUID(),
                    "local query",
                    List.of(
                        new com.apex.order.modules.order.domain.OrderItem(UUID.randomUUID(), 1)))
                .id();
      }
      var jdbc = app.getBean(JdbcTemplate.class);
      long before = jdbc.queryForObject("SELECT count(*) FROM outbox_event", Long.class);
      assertThat(
              app.getBean(QueryGateway.class)
                  .queryLocal(new GetOrderQuery(id), OrderView.class, QueryOptions.defaults()))
          .isEqualTo(new OrderView(id, "PENDING_INVENTORY"));
      assertThat(jdbc.queryForObject("SELECT count(*) FROM outbox_event", Long.class))
          .isEqualTo(before);
      assertThat(jdbc.queryForObject("SELECT count(*) FROM order_request_receipt", Long.class))
          .isZero();
      var executor = app.getBean(TransactionalRequestExecutor.class);
      var first =
          new RequestContext(
              UUID.randomUUID(),
              "local-idem",
              "CREATE_ORDER",
              1,
              UUID.randomUUID(),
              null,
              "client",
              "order-cell",
              "t",
              "p",
              Instant.now().plusSeconds(5));
      assertThat(executor.execute(first, "a".repeat(64), () -> new byte[] {1}))
          .containsExactly((byte) 1);
      var changed =
          new RequestContext(
              UUID.randomUUID(),
              "local-idem",
              "OTHER_CONTRACT",
              1,
              UUID.randomUUID(),
              null,
              "client",
              "order-cell",
              "t",
              "p",
              Instant.now().plusSeconds(5));
      assertThatThrownBy(
              () ->
                  executor.execute(
                      changed,
                      "a".repeat(64),
                      () -> {
                        throw new AssertionError("Must not execute changed contract");
                      }))
          .isInstanceOfSatisfying(
              GatewayException.class,
              e -> assertThat(e.code()).isEqualTo(GatewayException.Code.ALREADY_EXISTS));
    }
  }
}
