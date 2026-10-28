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
  private static final JsonMapper JSON = JsonMapper.builder().build();

  public SampleGrpcClientApplication(
      RequestReplyGateway r, QueryGateway q, ExecutionContextProvider c, Environment e) {
    requests = r;
    queries = q;
    contexts = c;
    environment = e;
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
          try (var scope = contexts.open(new ExecutionContext(corr, null, cell, cell))) {
            result = execute(command);
          }
        } catch (GatewayCancellationException cancelled) {
          result = Map.of("error", "CANCELLED", "outcomeUnknown", cancelled.outcomeUnknown());
        } catch (Throwable failure) {
          while ((failure instanceof ExecutionException || failure instanceof CompletionException)
              && failure.getCause() != null) failure = failure.getCause();
          if (failure instanceof GatewayException g)
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

  @CellRequest(type = "CREATE_ORDER", version = 99, response = CreateOrderResponse.class)
  public record UnknownRequest(
      String externalReference, String description, List<OrderItem> items) {}

  @CellQuery(type = "GET_ORDER", version = 99, response = OrderView.class)
  public record UnknownQuery(UUID orderId) {}
}
