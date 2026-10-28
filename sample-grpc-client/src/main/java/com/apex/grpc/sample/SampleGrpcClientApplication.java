package com.apex.grpc.sample;

import com.apex.order.contracts.*;
import com.apex.platform.core.context.*;
import com.apex.platform.grpc.api.*;
import java.io.*;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import org.springframework.boot.*;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.core.env.Environment;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

/**
 * Interactive Java-only business client; no protobuf, stub, channel or Order implementation import.
 */
@SpringBootApplication
public class SampleGrpcClientApplication implements ApplicationRunner {
  private final RequestReplyGateway requests;
  private final QueryGateway queries;
  private final ExecutionContextProvider contexts;
  private final Environment environment;
  private final org.springframework.beans.factory.ObjectProvider<MultiResponderGateway> collections;
  private static final JsonMapper JSON = JsonMapper.builder().build();

  public SampleGrpcClientApplication(
      RequestReplyGateway r,
      QueryGateway q,
      ExecutionContextProvider c,
      Environment e,
      org.springframework.beans.factory.ObjectProvider<MultiResponderGateway> collections) {
    requests = r;
    queries = q;
    contexts = c;
    environment = e;
    this.collections = collections;
  }

  public static void main(String[] args) {
    try (var context = SpringApplication.run(SampleGrpcClientApplication.class, args)) {}
  }

  @Override
  public void run(ApplicationArguments args) throws Exception {
    System.out.println("M31_CLIENT_READY " + ProcessHandle.current().pid());
    try (var reader =
        new BufferedReader(new InputStreamReader(System.in, StandardCharsets.UTF_8))) {
      String line;
      while ((line = reader.readLine()) != null) {
        if (line.equals("EXIT")) break;
        Object result;
        try {
          var command = JSON.readTree(line);
          String cell = environment.getRequiredProperty("apex.grpc.cell-id");
          UUID corr =
              UUID.fromString(command.path("correlationId").asText(UUID.randomUUID().toString()));
          UUID parent =
              command.hasNonNull("parentCausationId")
                  ? UUID.fromString(command.path("parentCausationId").asText())
                  : null;
          try (var scope = contexts.open(new ExecutionContext(corr, parent, cell, cell))) {
            result = execute(command);
          }
        } catch (GatewayCancellationException cancelled) {
          result = Map.of("error", "CANCELLED", "outcomeUnknown", cancelled.outcomeUnknown());
        } catch (Throwable failure) {
          while ((failure instanceof ExecutionException || failure instanceof CompletionException)
              && failure.getCause() != null) failure = failure.getCause();
          if (failure instanceof CollectionFailedException c)
            result = Map.of("error", "COLLECTION_INCOMPLETE", "collection", c.result());
          else if (failure instanceof CollectionCancelledException c)
            result = Map.of("error", "COLLECTION_CANCELLED", "collection", c.result());
          else if (failure instanceof GatewayException g)
            result = Map.of("error", g.code().name(), "outcomeUnknown", g.outcomeUnknown());
          else if (failure instanceof GatewayCancellationException c)
            result = Map.of("error", "CANCELLED", "outcomeUnknown", c.outcomeUnknown());
          else if (failure instanceof TimeoutException)
            result = Map.of("error", "DEADLINE_EXCEEDED", "outcomeUnknown", true);
          else if (failure instanceof IllegalArgumentException)
            result = Map.of("error", "INVALID_ARGUMENT", "outcomeUnknown", false);
          else result = Map.of("error", "INTERNAL", "outcomeUnknown", true);
        }
        boolean clear;
        try {
          contexts.requireCurrent();
          clear = false;
        } catch (IllegalStateException absent) {
          clear = true;
        }
        System.out.println(
            "M31_REPLY "
                + JSON.writeValueAsString(Map.of("result", result, "contextCleared", clear)));
      }
    }
  }

  private Object execute(JsonNode command) throws Exception {
    String action = command.path("action").asText("create");
    Duration timeout = Duration.ofMillis(command.path("deadlineMs").asLong(3000));
    UUID requestId =
        UUID.fromString(command.path("requestId").asText(UUID.randomUUID().toString()));
    if (action.equals("collect")
        || action.equals("cancel-collection")
        || action.equals("concurrent-collections")
        || action.equals("saturation")) {
      var targets = new ArrayList<String>();
      command.path("targets").forEach(n -> targets.add(n.asText()));
      var options =
          new CollectionOptions(targets, timeout, command.path("allowPartial").asBoolean(true));
      var gateway = collections.getIfAvailable();
      if (gateway == null)
        throw new GatewayException(GatewayException.Code.FAILED_PRECONDITION, false);
      var query = new GetOrderPerspective(UUID.fromString(command.path("orderId").asText()));
      if (action.equals("concurrent-collections")) {
        int count = command.path("count").asInt(2);
        if (count < 1 || count > 16)
          throw new IllegalArgumentException("Bound concurrent sample collections to 1..16");
        var stages = new ArrayList<CompletableFuture<CollectionResult<OrderPerspective>>>();
        var correlations = new ArrayList<UUID>();
        var parent = contexts.requireCurrent();
        for (int i = 0; i < count; i++) {
          UUID correlation = UUID.randomUUID();
          correlations.add(correlation);
          try (var scope =
              contexts.open(
                  new ExecutionContext(
                      correlation,
                      parent.causationId(),
                      parent.sourceService(),
                      parent.cellId()))) {
            stages.add(
                gateway
                    .collectQueries(query, OrderPerspective.class, options)
                    .toCompletableFuture());
          }
        }
        var results = new ArrayList<Map<String, Object>>();
        long end = System.nanoTime() + timeout.plusMillis(500).toNanos();
        for (int i = 0; i < stages.size(); i++)
          results.add(
              Map.of(
                  "correlationId",
                  correlations.get(i),
                  "collection",
                  stages.get(i).get(Math.max(1, end - System.nanoTime()), TimeUnit.NANOSECONDS)));
        return results;
      }
      var pending =
          gateway.collectQueries(query, OrderPerspective.class, options).toCompletableFuture();
      if (action.equals("saturation")) {
        String code;
        try {
          gateway
              .collectQueries(query, OrderPerspective.class, options)
              .toCompletableFuture()
              .join();
          code = "UNEXPECTED_SUCCESS";
        } catch (CompletionException failure) {
          code = failure.getCause() instanceof GatewayException g ? g.code().name() : "INTERNAL";
        } finally {
          pending.cancel(true);
        }
        return Map.of("secondError", code, "firstCancelled", pending.isCancelled());
      }
      if (action.equals("cancel-collection")) {
        long delay = command.path("cancelAfterMs").asLong(0);
        if (delay < 0 || delay > 1000)
          throw new IllegalArgumentException("Bound cancellation delay to 0..1000ms");
        if (delay == 0) pending.cancel(true);
        else
          CompletableFuture.delayedExecutor(delay, TimeUnit.MILLISECONDS)
              .execute(() -> pending.cancel(true));
      }
      return pending.get(timeout.toMillis() + 500, TimeUnit.MILLISECONDS);
    }
    if (action.equals("perspective") || action.equals("perspective99")) {
      String target = command.path("target").asText("order-cell");
      UUID id = UUID.fromString(command.path("orderId").asText());
      if (action.equals("perspective99"))
        return queries.query(
            target,
            new UnsupportedPerspective(id),
            OrderPerspective.class,
            new QueryOptions(requestId, timeout));
      return queries.query(
          target,
          new GetOrderPerspective(id),
          OrderPerspective.class,
          new QueryOptions(requestId, timeout));
    }
    if (action.equals("query") || action.equals("query99")) {
      UUID order = UUID.fromString(command.path("orderId").asText());
      if (action.equals("query99"))
        return queries.query(
            "order-cell",
            new UnknownQuery(order),
            OrderView.class,
            new QueryOptions(requestId, timeout));
      return queries.query(
          "order-cell",
          new GetOrderQuery(order),
          OrderView.class,
          new QueryOptions(requestId, timeout));
    }
    String key = command.path("key").asText();
    var options = new RequestOptions(requestId, key, timeout);
    var request =
        new CreateOrderRequest(
            command.path("externalReference").asText(),
            "Java-first gRPC order",
            List.of(
                new OrderItem(
                    UUID.fromString(command.path("productId").asText()),
                    command.path("quantity").asInt(1))));
    if (action.equals("version99"))
      return requests.request(
          "order-cell",
          new UnknownRequest(request.externalReference(), request.description(), request.items()),
          CreateOrderResponse.class,
          options);
    if (action.equals("version2"))
      return requests.request(
          "order-cell",
          new VersionTwoRequest(
              request.externalReference(), request.description(), request.items()),
          CreateOrderResponse.class,
          options);
    if (action.equals("mismatch"))
      return requests.request("order-cell", request, OrderView.class, options);
    if (action.equals("concurrent")) {
      var first =
          requests
              .requestAsync("order-cell", request, CreateOrderResponse.class, options)
              .toCompletableFuture();
      var second =
          requests
              .requestAsync(
                  "order-cell",
                  request,
                  CreateOrderResponse.class,
                  options.withRequestId(UUID.randomUUID()))
              .toCompletableFuture();
      return List.of(
          first.get(timeout.toMillis() + 500, TimeUnit.MILLISECONDS),
          second.get(timeout.toMillis() + 500, TimeUnit.MILLISECONDS));
    }
    if (action.equals("async"))
      return requests
          .requestAsync("order-cell", request, CreateOrderResponse.class, options)
          .toCompletableFuture()
          .get(timeout.toMillis() + 500, TimeUnit.MILLISECONDS);
    if (action.equals("cancel")) {
      var future =
          requests
              .requestAsync("order-cell", request, CreateOrderResponse.class, options)
              .toCompletableFuture();
      future.cancel(true);
      return future.get(1, TimeUnit.SECONDS);
    }
    return requests.request("order-cell", request, CreateOrderResponse.class, options);
  }

  @CellQuery(type = "GET_ORDER_PERSPECTIVE", version = 99, response = OrderPerspective.class)
  public record UnsupportedPerspective(UUID orderId) {}

  @CellRequest(type = "CREATE_ORDER", version = 2, response = CreateOrderResponse.class)
  public record VersionTwoRequest(
      String externalReference, String description, List<OrderItem> items) {}

  @CellRequest(type = "CREATE_ORDER", version = 99, response = CreateOrderResponse.class)
  public record UnknownRequest(
      String externalReference, String description, List<OrderItem> items) {}

  @CellQuery(type = "GET_ORDER", version = 99, response = OrderView.class)
  public record UnknownQuery(UUID orderId) {}
}
