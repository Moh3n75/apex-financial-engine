package example;

import com.apex.platform.core.context.ExecutionContext;
import com.apex.platform.core.context.ExecutionContextProvider;
import com.apex.platform.events.api.EventContracts;
import com.apex.platform.grpc.api.CellQuery;
import com.apex.platform.grpc.api.QueryGateway;
import com.apex.platform.grpc.api.RequestReplyGateway;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class ConsumerApplication {
    public record Changed(String value) implements com.apex.platform.events.IntegrationEvent {}
    public record Answer(String value) {}
    @CellQuery(type = "EXAMPLE_INSPECT", version = 1, response = Answer.class)
    public record Inspect(String id) {}

    public static void main(String[] args) throws Exception {
        try (var context = SpringApplication.run(ConsumerApplication.class, args)) {
            var provider = context.getBean(ExecutionContextProvider.class);
            try (var ignored = provider.open(ExecutionContext.root("external-consumer", "external-consumer"))) {
                if (!provider.requireCurrent().cellId().equals("external-consumer")) {
                    throw new IllegalStateException("Context contract failed");
                }
            }
            if (EventContracts.define("EXAMPLE_CHANGED", 1, "EXAMPLE", Changed.class,
                    "example.changed.v1").payloadType() != Changed.class) {
                throw new IllegalStateException("Typed event contract failed");
            }
            if (!context.getBeansOfType(QueryGateway.class).isEmpty()
                    || !context.getBeansOfType(RequestReplyGateway.class).isEmpty()) {
                throw new IllegalStateException("Transport unexpectedly active");
            }
            String port = context.getEnvironment().getProperty("local.server.port");
            if (port != null) {
                if (!context.containsBean("apexHttpExecutionContextFilter")) {
                    throw new IllegalStateException("Missing shared HTTP filter");
                }
                try (var client = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(3)).build()) {
                    var reply = client.send(HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + port + "/absent"))
                            .timeout(Duration.ofSeconds(3)).GET().build(), HttpResponse.BodyHandlers.discarding());
                    if (reply.statusCode() != 404) throw new IllegalStateException("HTTP probe failed");
                }
            }
            System.out.println("SDK_CONSUMER_ACCEPTED pid=" + ProcessHandle.current().pid()
                    + " web=" + (port != null) + " contextProviders="
                    + context.getBeansOfType(ExecutionContextProvider.class).size());
        }
    }
}
