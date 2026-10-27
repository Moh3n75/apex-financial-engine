package com.apex.replay;

import static com.apex.replay.ReplayModel.*;

import java.io.PrintStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;

/** Deliberate command execution, never a Spring application/listener/scheduler. */
public final class ReplayToolApplication {
    private ReplayToolApplication() { }

    public static void main(String[] args) { System.exit(run(args, System.getenv(), System.out)); }

    static int run(String[] args, Map<String, String> environment, PrintStream output) {
        try {
            require(args.length > 0, "COMMAND_REQUIRED");
            String command = args[0];
            Map<String, Set<String>> commands = Map.of(
                    "init-audit", Set.of(),
                    "status", Set.of("plan-id"),
                    "approve", Set.of("plan-id", "confirm-digest", "actor", "reason"),
                    "execute", Set.of("plan-id", "confirm-digest", "actor", "reason", "routes", "target-group", "replay-topic",
                            "failure-resolved", "max-records", "rate", "attempts", "allow-kafka-interaction"),
                    "reconcile", Set.of("plan-id", "confirm-digest", "actor", "reason", "routes", "target-group", "replay-topic",
                            "from-offset", "max-scan", "allow-kafka-interaction"),
                    "inspect", Set.of("routes", "dlt-topic", "partition", "offset", "allow-kafka-interaction"),
                    "plan", Set.of("routes", "selector", "target-group", "replay-topic", "actor", "reason", "limit",
                            "allow-kafka-interaction"));
            require(commands.containsKey(command), "UNKNOWN_COMMAND");
            var options = options(args, commands.get(command));
            if (command.equals("approve")) {
                audit(environment).approve(UUID.fromString(options.get("plan-id")), options.get("confirm-digest"),
                        options.get("actor"), options.get("reason"));
                output.println("APPROVED; publication NOT YET VERIFIED; business processing NOT VERIFIED");
            } else if (command.equals("status")) {
                output.println(JSON.writeValueAsString(audit(environment).status(UUID.fromString(options.get("plan-id")))));
            } else if (command.equals("init-audit")) {
                audit(environment).initialize(); output.println("AUDIT_SCHEMA_INITIALIZED");
            } else if (command.equals("execute") || command.equals("reconcile")) {
                var audit = audit(environment);
                UUID id = UUID.fromString(options.get("plan-id")); String digest = options.get("confirm-digest");
                var plan = audit.approved(id, digest);
                List<Route> routes = List.of(JSON.readValue(file(options.get("routes")), Route[].class));
                require(plan.items().stream().allMatch(i -> routes.contains(i.route())
                        && i.route().targetGroup().equals(options.get("target-group"))
                        && i.route().replayTopic().equals(options.get("replay-topic"))), "TARGET_NOT_ALLOWLISTED");
                var executor = new ReplayExecutor(audit, kafka(environment), routes);
                var status = command.equals("execute")
                        ? executor.execute(id, digest, options.get("actor"), options.get("reason"), options.get("failure-resolved"),
                            Integer.parseInt(options.get("max-records")), Integer.parseInt(options.get("rate")), Integer.parseInt(options.get("attempts")))
                        : executor.reconcile(id, digest, options.get("actor"), options.get("reason"),
                            Long.parseLong(options.get("from-offset")), Integer.parseInt(options.get("max-scan")));
                output.println(JSON.writeValueAsString(status));
            } else {
                List<Route> routes = List.of(JSON.readValue(file(options.get("routes")), Route[].class));
                // Validate manifest, limits and destination allowlist before opening Kafka clients.
                List<Selection> selections = command.equals("plan")
                        ? List.of(JSON.readValue(file(options.get("selector")), Selection[].class)) : List.of();
                int limit = command.equals("plan") ? Integer.parseInt(options.get("limit")) : 1;
                require(limit > 0 && limit <= HARD_LIMIT && (command.equals("inspect")
                        || (!selections.isEmpty() && selections.size() <= limit)), "SELECTION_LIMIT_EXCEEDED");
                Route route = command.equals("plan") ? routes.stream().filter(r -> r.targetGroup().equals(options.get("target-group"))
                        && r.replayTopic().equals(options.get("replay-topic"))).findFirst()
                        .orElseThrow(() -> new IllegalArgumentException("TARGET_NOT_ALLOWLISTED")) : null;
                Coordinate coordinate = command.equals("inspect") ? new Coordinate(options.get("dlt-topic"),
                        Integer.parseInt(options.get("partition")), Long.parseLong(options.get("offset"))) : null;
                try (var reader = new DltReader(kafka(environment), routes)) {
                    if (command.equals("inspect")) {
                        var record = reader.read(coordinate);
                        var view = new LinkedHashMap<String, Object>();
                        view.put("dlt", coordinate);
                        view.put("rawLength", record.value() == null ? 0 : record.value().length);
                        view.put("rawSha256", record.value() == null ? "TOMBSTONE" : sha(record.value()));
                        try {
                            String originTopic = DltReader.stringHeader(record.headers(),
                                    org.springframework.kafka.support.KafkaHeaders.DLT_ORIGINAL_TOPIC);
                            String originGroup = DltReader.stringHeader(record.headers(),
                                    org.springframework.kafka.support.KafkaHeaders.DLT_ORIGINAL_CONSUMER_GROUP);
                            topic(originTopic);
                            require(originGroup.matches("[A-Za-z0-9._-]{1,249}"), "INVALID_ORIGIN_GROUP");
                            byte[] originPartition = DltReader.header(record.headers(),
                                    org.springframework.kafka.support.KafkaHeaders.DLT_ORIGINAL_PARTITION);
                            byte[] originOffset = DltReader.header(record.headers(),
                                    org.springframework.kafka.support.KafkaHeaders.DLT_ORIGINAL_OFFSET);
                            require(originPartition.length == 4 && originOffset.length == 8, "INVALID_ORIGIN_HEADER_LENGTH");
                            view.put("origin", new Coordinate(originTopic, java.nio.ByteBuffer.wrap(originPartition).getInt(),
                                    java.nio.ByteBuffer.wrap(originOffset).getLong()));
                            view.put("originGroup", originGroup);
                            var exception = record.headers().lastHeader(org.springframework.kafka.support.KafkaHeaders.DLT_EXCEPTION_FQCN);
                            if (exception != null && exception.value() != null) {
                                String className = new String(exception.value(), java.nio.charset.StandardCharsets.UTF_8);
                                view.put("exceptionClass", className.matches("[A-Za-z0-9_.$]{1,249}") ? className : "REDACTED");
                            }
                            var metadata = JSON.readTree(record.value()).path("metadata");
                            view.put("eventId", UUID.fromString(metadata.path("eventId").asText()));
                            String type = metadata.path("eventType").asText();
                            require(type.matches("[A-Z_]{1,80}"), "UNSAFE_EVENT_TYPE");
                            view.put("eventType", type); view.put("eventVersion", metadata.path("eventVersion").asInt());
                        } catch (RuntimeException malformed) { view.put("validation", "INVALID_PROVENANCE_OR_ENVELOPE_PLAN_REFUSED"); }
                        view.put("exceptionMessage", "REDACTED");
                        output.println(JSON.writeValueAsString(view));
                    } else {
                        var plan = ReplayPlanner.plan(selections, route, options.get("actor"), options.get("reason"), limit, reader);
                        audit(environment).save(plan);
                        output.println(JSON.writeValueAsString(Map.of("planId", plan.planId(), "digest", plan.digest(),
                                "planned", plan.items().size(), "status", "PLANNED", "publication", "NOT_PUBLISHED")));
                    }
                }
            }
            return 0;
        } catch (Exception failure) {
            // Do not leak JDBC URLs, Kafka security configuration, raw payloads or nested exception messages.
            String code = failure.getMessage();
            output.println("ERROR " + (code != null && code.matches("[A-Z_]{3,100}") ? code : "OPERATION_REFUSED"));
            return 2;
        }
    }

    private static Map<String, String> options(String[] args, Set<String> allowed) {
        var options = new LinkedHashMap<String, String>();
        for (int i = 1; i < args.length; i++) {
            require(args[i].startsWith("--"), "INVALID_OPTION");
            String key = args[i].substring(2);
            require(allowed.contains(key) && !options.containsKey(key), "UNKNOWN_OR_DUPLICATE_OPTION");
            if (key.equals("allow-kafka-interaction")) { options.put(key, "true"); }
            else { require(i + 1 < args.length && !args[i + 1].startsWith("--"), "OPTION_VALUE_REQUIRED");
                options.put(key, args[++i]); }
        }
        require(options.keySet().equals(allowed), "REQUIRED_OPTIONS_MISSING");
        return options;
    }

    private static ReplayAudit audit(Map<String, String> env) {
        return new ReplayAudit(required(env, "REPLAY_AUDIT_DB_URL"), required(env, "REPLAY_AUDIT_DB_USERNAME"),
                required(env, "REPLAY_AUDIT_DB_PASSWORD"));
    }

    private static Properties kafka(Map<String, String> env) throws java.io.IOException {
        var properties = new Properties();
        String config = env.get("REPLAY_KAFKA_PROPERTIES_FILE");
        if (config != null) { try (var input = Files.newInputStream(Path.of(config))) { properties.load(input); } }
        properties.setProperty("bootstrap.servers", required(env, "REPLAY_KAFKA_BOOTSTRAP_SERVERS"));
        return properties;
    }

    private static String required(Map<String, String> env, String key) {
        require(env.containsKey(key) && (key.endsWith("PASSWORD") || !env.get(key).isBlank()), "ENVIRONMENT_REQUIRED");
        return env.get(key);
    }

    private static byte[] file(String path) throws java.io.IOException {
        require(Files.size(Path.of(path)) <= 128 * 1024, "INPUT_FILE_TOO_LARGE");
        return Files.readAllBytes(Path.of(path));
    }
}
