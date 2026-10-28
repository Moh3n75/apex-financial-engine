package com.apex.order.modules.order.grpc;

import com.apex.platform.grpc.api.*;
import java.time.Duration;
import java.time.Instant;
import java.util.Objects;
import java.util.function.Supplier;
import org.springframework.context.annotation.Profile;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.stereotype.Component;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.*;

/** Receipt, PlaceOrderUseCase and Outbox share this exact DataSource and outer commit. */
@Component
@Profile("grpc")
public final class JdbcOrderRequestExecutor implements TransactionalRequestExecutor {
  private final JdbcTemplate jdbc;
  private final PlatformTransactionManager manager;

  public JdbcOrderRequestExecutor(JdbcTemplate jdbc, PlatformTransactionManager manager) {
    if (!(manager instanceof DataSourceTransactionManager tx)
        || tx.getDataSource() != jdbc.getDataSource())
      throw new IllegalStateException(
          "Order RPC requires the same JDBC DataSource/transaction manager as Order persistence");
    this.jdbc = jdbc;
    this.manager = manager;
  }

  @Override
  public byte[] execute(RequestContext context, String fingerprint, Supplier<byte[]> operation) {
    if (TransactionSynchronizationManager.isActualTransactionActive())
      throw new GatewayException(GatewayException.Code.FAILED_PRECONDITION, false);
    long remaining = Duration.between(Instant.now(), context.deadline()).toMillis();
    if (remaining < 1) throw new GatewayException(GatewayException.Code.DEADLINE_EXCEEDED, true);
    var tx = new TransactionTemplate(manager);
    tx.setTimeout((int) Math.max(1, Math.min(30, (remaining + 999) / 1000)));
    try {
      return Objects.requireNonNull(
          tx.execute(
              status -> {
                jdbc.queryForObject(
                    "SELECT set_config('lock_timeout',?,true)",
                    String.class,
                    Math.min(remaining, 5000) + "ms");
                jdbc.queryForObject(
                    "SELECT set_config('statement_timeout',?,true)",
                    String.class,
                    remaining + "ms");
                int claimed =
                    jdbc.update(
                        """
                        INSERT INTO order_request_receipt(idempotency_key,client_id,tenant_id,principal,contract_type,contract_version,request_id,fingerprint)
                        VALUES (?,?,?,?,?,?,?,?) ON CONFLICT(idempotency_key) DO NOTHING
                        """,
                        context.idempotencyKey(),
                        context.originCell(),
                        context.tenant(),
                        context.principal(),
                        context.type(),
                        context.version(),
                        context.requestId(),
                        fingerprint);
                if (claimed == 0) {
                  var row =
                      jdbc.queryForMap(
                          "SELECT * FROM order_request_receipt WHERE idempotency_key=?",
                          context.idempotencyKey());
                  if (!context.originCell().equals(row.get("client_id"))
                      || !context.tenant().equals(row.get("tenant_id"))
                      || !context.principal().equals(row.get("principal"))
                      || !context.type().equals(row.get("contract_type"))
                      || context.version() != ((Number) row.get("contract_version")).intValue()
                      || !fingerprint.equals(row.get("fingerprint")))
                    throw new GatewayException(GatewayException.Code.ALREADY_EXISTS, false);
                  byte[] response = (byte[]) row.get("response");
                  if (response == null)
                    throw new GatewayException(GatewayException.Code.FAILED_PRECONDITION, false);
                  return response;
                }
                byte[] reply = Objects.requireNonNull(operation.get());
                if (jdbc.update(
                        "UPDATE order_request_receipt SET response=?,completed_at=clock_timestamp()"
                            + " WHERE idempotency_key=? AND response IS NULL",
                        reply,
                        context.idempotencyKey())
                    != 1) throw new IllegalStateException("Order RPC receipt transition failed");
                return reply;
              }));
    } catch (DuplicateKeyException conflict) {
      throw new GatewayException(GatewayException.Code.ALREADY_EXISTS, false);
    }
  }
}
