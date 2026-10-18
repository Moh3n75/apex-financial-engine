import java.time.Duration;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.DescribeTopicsOptions;

/** Read-only Admin requests only: never creates topics, sends records or joins a consumer group. */
public class KafkaReadOnlyProbe {
    public static void main(String[] args) throws Exception {
        if (args.length != 1) throw new IllegalArgumentException("Usage: KafkaReadOnlyProbe <bootstrap-servers>");
        Properties properties = new Properties();
        properties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, args[0]);
        properties.put(AdminClientConfig.CLIENT_ID_CONFIG, "apex-m22-read-only-probe");
        properties.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, "5000");
        properties.put(AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, "12000");
        properties.put(AdminClientConfig.RETRIES_CONFIG, "0");
        try (Admin admin = Admin.create(properties)) {
            var cluster = admin.describeCluster();
            System.out.println("CLUSTER " + cluster.clusterId().get(15, TimeUnit.SECONDS));
            for (var node : cluster.nodes().get(15, TimeUnit.SECONDS)) {
                try (Socket socket = new Socket()) {
                    socket.connect(new InetSocketAddress(node.host(), node.port()), 3000);
                    System.out.println("ADVERTISED " + node + " TCP_REACHABLE=true");
                } catch (Exception failure) {
                    System.out.println("ADVERTISED " + node + " TCP_REACHABLE=false " + failure.getClass().getSimpleName());
                }
            }
            var names = admin.listTopics().names().get(15, TimeUnit.SECONDS);
            for (String topic : List.of("apex.customer.events.v1", "apex.financial.events.v1")) {
                System.out.println("TOPIC " + topic + " EXISTS=" + names.contains(topic));
                try {
                    System.out.println(admin.describeTopics(List.of(topic), new DescribeTopicsOptions().timeoutMs(10000))
                            .allTopicNames().get(15, TimeUnit.SECONDS).get(topic));
                } catch (Exception failure) {
                    for (Throwable cause = failure; cause != null; cause = cause.getCause()) {
                        System.out.println("TOPIC_DIAGNOSTIC " + topic + " " + cause.getClass().getName() + ": " + cause.getMessage());
                    }
                }
            }
        } catch (Exception failure) {
            for (Throwable cause = failure; cause != null; cause = cause.getCause()) {
                System.err.println("DIAGNOSTIC " + cause.getClass().getName() + ": " + cause.getMessage());
            }
            System.exit(2);
        }
    }
}
