package com.apex.replay;

import static com.apex.replay.ReplayModel.*;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.springframework.kafka.support.KafkaHeaders;

/** Exact-coordinate reads only. No group membership, commits, topic creation or producers. */
public final class DltReader implements AutoCloseable {
    private final KafkaConsumer<byte[], byte[]> consumer;
    private final Admin admin;
    private final List<Route> routes;

    public DltReader(Properties connection, List<Route> routes) {
        this.routes = List.copyOf(routes);
        require(!routes.isEmpty(), "ROUTES_REQUIRED");
        var destinations = routes.stream().flatMap(r -> java.util.stream.Stream.of(r.replayTopic(), r.replayDltTopic())).toList();
        require(routes.stream().noneMatch(r -> destinations.contains(r.originTopic())
                || destinations.contains(r.dltTopic())), "SOURCE_DESTINATION_COLLISION");
        require(routes.stream().map(Route::targetGroup).distinct().count() == routes.size(), "DUPLICATE_ROUTE_GROUP");
        require(connection.getProperty("bootstrap.servers") != null, "BOOTSTRAP_REQUIRED");
        Map<String, Object> properties = new HashMap<>();
        connection.forEach((key, value) -> properties.put(key.toString(), value));
        // Ignore any supplied group/auto-commit/interceptor/deserializer settings.
        properties.remove("group.id"); properties.remove("group.instance.id");
        properties.remove("interceptor.classes");
        properties.put("enable.auto.commit", false);
        properties.put("allow.auto.create.topics", false);
        properties.put("key.deserializer", ByteArrayDeserializer.class);
        properties.put("value.deserializer", ByteArrayDeserializer.class);
        properties.put("default.api.timeout.ms", 10000);
        properties.put("request.timeout.ms", 5000);
        properties.put("max.poll.records", 10);
        admin = Admin.create(properties);
        try { consumer = new KafkaConsumer<>(properties); }
        catch (RuntimeException failure) { admin.close(Duration.ofSeconds(2)); throw failure; }
    }

    public ConsumerRecord<byte[], byte[]> read(Coordinate coordinate) {
        require(routes.stream().anyMatch(r -> r.dltTopic().equals(coordinate.topic())), "DLT_NOT_ALLOWLISTED");
        TopicPartition partition = new TopicPartition(coordinate.topic(), coordinate.partition());
        consumer.assign(List.of(partition));
        var start = consumer.beginningOffsets(List.of(partition)).get(partition);
        var end = consumer.endOffsets(List.of(partition)).get(partition);
        require(coordinate.offset() >= start && coordinate.offset() < end, "DLT_COORDINATE_UNAVAILABLE");
        consumer.seek(partition, coordinate.offset());
        long deadline = System.nanoTime() + Duration.ofSeconds(10).toNanos();
        while (System.nanoTime() < deadline) {
            for (var record : consumer.poll(Duration.ofMillis(200))) {
                require(record.offset() <= coordinate.offset(), "DLT_RECORD_NOT_FOUND");
                if (record.offset() == coordinate.offset()) { return record; }
            }
        }
        throw new IllegalStateException("DLT_READ_TIMEOUT");
    }

    public Item validate(Selection selection, Route route) {
        require(routes.contains(route), "ROUTE_NOT_ALLOWLISTED");
        var item = validateRecord(selection, route, read(selection.dlt()));
        require(item.origin().partition() < preflight(route), "ORIGIN_PARTITION_NOT_ON_SOURCE");
        return item;
    }

    static Item validateRecord(Selection selection, Route route, ConsumerRecord<byte[], byte[]> record) {
        require(record.topic().equals(selection.dlt().topic()) && record.partition() == selection.dlt().partition()
                && record.offset() == selection.dlt().offset(), "DLT_IDENTITY_MISMATCH");
        require(record.topic().equals(route.dltTopic()), "DLT_ROUTE_MISMATCH");
        var headers = record.headers();
        String source = stringHeader(headers, KafkaHeaders.DLT_ORIGINAL_TOPIC);
        String group = stringHeader(headers, KafkaHeaders.DLT_ORIGINAL_CONSUMER_GROUP);
        byte[] partition = header(headers, KafkaHeaders.DLT_ORIGINAL_PARTITION);
        byte[] offset = header(headers, KafkaHeaders.DLT_ORIGINAL_OFFSET);
        require(partition.length == 4 && offset.length == 8, "INVALID_ORIGIN_HEADER_LENGTH");
        Coordinate origin = new Coordinate(source, ByteBuffer.wrap(partition).getInt(), ByteBuffer.wrap(offset).getLong());
        require(source.equals(route.originTopic()) && group.equals(route.targetGroup()), "ORIGIN_ROUTE_MISMATCH");
        require(record.partition() == origin.partition(), "ORIGIN_DLT_PARTITION_MISMATCH");
        require(record.value() != null && record.value().length <= 1024 * 1024, "INVALID_RAW_LENGTH");
        String rawHash = sha(record.value());
        require(rawHash.equals(selection.rawSha256()), "RAW_HASH_MISMATCH");
        var envelope = JSON.readTree(record.value());
        require(envelope != null && envelope.isObject() && envelope.path("payload").isObject(), "INVALID_ENVELOPE");
        var metadata = envelope.path("metadata");
        UUID eventId = UUID.fromString(metadata.path("eventId").asText());
        require(eventId.equals(selection.eventId()), "EVENT_ID_MISMATCH");
        UUID.fromString(metadata.path("correlationId").asText());
        if (!metadata.path("causationId").isMissingNode() && !metadata.path("causationId").isNull()) {
            UUID.fromString(metadata.path("causationId").asText());
        }
        java.time.Instant.parse(metadata.path("occurredAt").asText());
        for (String field : List.of("aggregateId", "aggregateType", "sourceService", "cellId")) {
            require(metadata.path(field).isTextual() && !metadata.path(field).asText().isBlank(), "INVALID_METADATA");
        }
        String type = metadata.path("eventType").asText();
        require(metadata.path("eventVersion").isIntegralNumber() && metadata.path("eventVersion").asText().equals("1")
                && route.eventTypes().contains(type), "UNSUPPORTED_CONTRACT");
        var encodedHeaders = new ArrayList<String>();
        headers.forEach(h -> encodedHeaders.add(h.key() + ":" + (h.value() == null ? "null"
                : Base64.getEncoder().encodeToString(h.value()))));
        String headersHash = sha(JSON.writeValueAsBytes(encodedHeaders));
        String keyHash = record.key() == null ? "NULL" : sha(record.key());
        return new Item(origin, group, eventId, type, 1, rawHash, record.value().length,
                keyHash, headersHash, selection.dlt(), route);
    }

    public int preflight(Route route) {
        try {
            var topics = admin.describeTopics(List.of(route.originTopic(), route.dltTopic(), route.replayTopic(),
                    route.replayDltTopic())).allTopicNames().get(10, TimeUnit.SECONDS);
            int sourceCount = topics.get(route.originTopic()).partitions().size();
            int replayCount = topics.get(route.replayTopic()).partitions().size();
            require(topics.get(route.dltTopic()).partitions().size() >= sourceCount
                    && replayCount >= sourceCount
                    && topics.get(route.replayDltTopic()).partitions().size() >= replayCount, "INSUFFICIENT_PARTITIONS");
            return sourceCount;
        } catch (IllegalArgumentException failure) { throw failure; }
        catch (Exception failure) { throw new IllegalStateException("TOPIC_PREFLIGHT_FAILED"); }
    }

    static byte[] header(Headers headers, String name) {
        var values = new ArrayList<byte[]>();
        headers.headers(name).forEach(h -> values.add(h.value()));
        require(values.size() == 1 && values.getFirst() != null, "MISSING_OR_AMBIGUOUS_ORIGIN_HEADER");
        return values.getFirst();
    }

    static String stringHeader(Headers headers, String name) {
        return new String(header(headers, name), StandardCharsets.UTF_8);
    }

    @Override public void close() {
        try { consumer.close(Duration.ofSeconds(2)); } finally { admin.close(Duration.ofSeconds(2)); }
    }
}
