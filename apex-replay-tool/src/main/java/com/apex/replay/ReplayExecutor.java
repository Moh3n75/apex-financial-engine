package com.apex.replay;

import static com.apex.replay.ReplayModel.*;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.serialization.ByteArraySerializer;

/** Human-initiated one-shot execution; no source writes, commits or automatic retransmission. */
public final class ReplayExecutor {
    public static final String MARKER_HEADER = "apex-replay-marker";
    private final ReplayAudit audit;
    private final Properties connection;
    private final List<Route> routes;
    private final Supplier<Sender> senders;
    private final Runnable afterAcknowledgment;

    interface Sender extends AutoCloseable {
        Coordinate send(ProducerRecord<byte[], byte[]> record) throws Exception;
        @Override void close();
    }

    public ReplayExecutor(ReplayAudit audit, Properties connection, List<Route> routes) {
        this(audit, connection, routes, () -> sender(connection), () -> { });
    }

    // Package-private fault seam used by isolated tests only. No CLI/environment crash trigger.
    ReplayExecutor(ReplayAudit audit, Properties connection, List<Route> routes, Supplier<Sender> senders, Runnable afterAck) {
        this.audit = audit; this.connection = connection; this.routes = List.copyOf(routes);
        this.senders = senders; this.afterAcknowledgment = afterAck;
    }

    public ReplayAudit.Status execute(UUID id, String digest, String actor, String reason, String resolved,
                                      int maximum, int rate, int attempts) throws Exception {
        text(actor); text(reason); text(resolved);
        require(maximum >= 1 && maximum <= HARD_LIMIT && rate >= 1 && rate <= 100 && attempts == 1, "INVALID_EXECUTION_BOUND");
        Plan plan = audit.approved(id, digest);
        require(plan.items().size() <= maximum && maximum <= plan.limit(), "PLAN_EXCEEDS_EXECUTION_BOUND");
        var previous = audit.status(id);
        if (previous.publication().equals("PUBLISHED_TO_REPLAY_TOPIC")) { return previous; }
        require(previous.deliveries().isEmpty(), "PUBLISH_STATE_REQUIRES_RECONCILIATION");
        try (var reader = new DltReader(connection, routes)) {
            // Validate the ENTIRE batch before a durable execution claim or any send.
            for (Item item : plan.items()) { revalidate(reader, item); }
            UUID claim = audit.claim(id, digest, actor, reason, resolved);
            try (var sender = senders.get()) {
                for (Item item : plan.items()) {
                    var source = revalidate(reader, item);
                    String marker = marker(plan, item);
                    var headers = new RecordHeaders();
                    source.headers().forEach(header -> {
                        // Retain exact business headers, never accumulated DLT diagnostics/provenance.
                        if (!header.key().startsWith("kafka_dlt-") && !header.key().startsWith("apex-replay-")) {
                            headers.add(header.key(), header.value());
                        }
                    });
                    add(headers, MARKER_HEADER, marker); add(headers, "apex-replay-plan-id", id.toString());
                    add(headers, "apex-replay-event-id", item.eventId().toString());
                    add(headers, "apex-replay-origin", JSON.writeValueAsString(item.origin()));
                    add(headers, "apex-replay-dlt", JSON.writeValueAsString(item.dlt()));
                    add(headers, "apex-replay-target-group", item.originGroup()); add(headers, "apex-replay-attempt", "1");
                    var record = new ProducerRecord<byte[], byte[]>(item.route().replayTopic(), item.origin().partition(),
                            null, source.key(), source.value(), headers);
                    audit.intent(plan, item, claim, marker, actor, reason); // COMMIT before send.
                    try {
                        Coordinate receipt = sender.send(record);
                        afterAcknowledgment.run();
                        audit.finish(plan, item, receipt, actor, reason, false);
                    } catch (Exception ambiguous) {
                        try { audit.finish(plan, item, null, actor, reason, false); }
                        catch (Exception auditUnavailable) { /* PUBLISHING is already durable: reconciliation required. */ }
                        throw new IllegalStateException("PUBLISH_UNKNOWN_RECONCILIATION_REQUIRED");
                    }
                    TimeUnit.MILLISECONDS.sleep(Math.max(1L, 1000L / rate));
                }
            }
        }
        return audit.status(id);
    }

    public ReplayAudit.Status reconcile(UUID id, String digest, String actor, String reason, long from, int maximum)
            throws Exception {
        text(actor); text(reason);
        require(from >= 0 && maximum >= 1 && maximum <= 10000, "INVALID_RECONCILE_BOUND");
        Plan plan = audit.approved(id, digest);
        try (var reader = new DltReader(connection, routes)) {
            for (var delivery : audit.status(id).deliveries()) {
                Item item = plan.items().stream().filter(i -> i.eventId().equals(delivery.eventId())).findFirst().orElseThrow();
                require(routes.contains(item.route()), "ROUTE_NOT_ALLOWLISTED");
                if (!delivery.state().equals("PUBLISHED")) {
                    reader.preflight(item.route());
                    var receipt = reader.find(delivery, item, from, maximum);
                    if (receipt.isPresent()) { audit.finish(plan, item, receipt.get(), actor, reason, true); }
                    else {
                        audit.finish(plan, item, null, actor, reason, false);
                        audit.noEvidence(plan, item, actor, reason);
                    }
                }
            }
        }
        return audit.status(id);
    }

    private org.apache.kafka.clients.consumer.ConsumerRecord<byte[], byte[]> revalidate(DltReader reader, Item item) {
        require(routes.contains(item.route()), "ROUTE_NOT_ALLOWLISTED");
        var selection = new Selection(item.dlt(), item.eventId(), item.rawSha256());
        require(reader.validate(selection, item.route()).equals(item), "APPROVED_SOURCE_CHANGED");
        var record = reader.read(item.dlt());
        require(DltReader.validateRecord(selection, item.route(), record).equals(item), "APPROVED_SOURCE_CHANGED");
        return record;
    }

    static String marker(Plan plan, Item item) {
        return "v1:" + plan.planId() + ":" + item.eventId() + ":"
                + sha((item.originGroup() + ":" + item.origin()).getBytes(StandardCharsets.UTF_8));
    }

    private static void add(RecordHeaders headers, String name, String value) {
        headers.add(name, value.getBytes(StandardCharsets.UTF_8));
    }

    static Sender sender(Properties connection) {
        var properties = new Properties(); properties.putAll(connection);
        properties.remove("interceptor.classes"); properties.remove("transactional.id");
        properties.put("acks", "all"); properties.put("enable.idempotence", true);
        properties.put("retries", Integer.MAX_VALUE); properties.put("max.in.flight.requests.per.connection", 5);
        properties.put("max.block.ms", 5000L); properties.put("delivery.timeout.ms", 5000);
        properties.put("request.timeout.ms", 5000); properties.put("linger.ms", 0);
        var producer = new KafkaProducer<byte[], byte[]>(properties, new ByteArraySerializer(), new ByteArraySerializer());
        return new Sender() {
            public Coordinate send(ProducerRecord<byte[], byte[]> record) throws Exception {
                var result = producer.send(record).get(6, TimeUnit.SECONDS);
                return new Coordinate(result.topic(), result.partition(), result.offset());
            }
            public void close() { producer.close(Duration.ofSeconds(2)); }
        };
    }
}
