package com.apex.replay;

import static com.apex.replay.ReplayModel.*;
import static org.assertj.core.api.Assertions.*;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.UUID;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.kafka.support.KafkaHeaders;

@Tag("unit")
class ReplayValidationTest {
    static final Route ROUTE = new Route("source", "source-dlt", "inventory-test", "inventory-replay",
            "inventory-replay-dlt", List.of("ORDER_PLACED"));

    static byte[] raw(UUID eventId, int version) {
        return ("{\"metadata\":{\"eventId\":\"" + eventId + "\",\"eventType\":\"ORDER_PLACED\",\"eventVersion\":"
                + version + ",\"correlationId\":\"11111111-1111-4111-8111-111111111111\",\"causationId\":null,"
                + "\"aggregateId\":\"22222222-2222-4222-8222-222222222222\",\"aggregateType\":\"ORDER\","
                + "\"sourceService\":\"order-cell\",\"cellId\":\"order-cell\",\"occurredAt\":\"2026-10-26T00:00:00Z\"},"
                + "\"payload\":{\"orderId\":\"22222222-2222-4222-8222-222222222222\",\"name\":\"نام فارسی\"}}")
                .getBytes(StandardCharsets.UTF_8);
    }

    static ConsumerRecord<byte[], byte[]> record(UUID id, int version, long dltOffset, long originOffset) {
        var record = new ConsumerRecord<byte[], byte[]>("source-dlt", 1, dltOffset,
                "ORDER:key".getBytes(StandardCharsets.UTF_8), raw(id, version));
        record.headers().add(KafkaHeaders.DLT_ORIGINAL_TOPIC, "source".getBytes(StandardCharsets.UTF_8));
        record.headers().add(KafkaHeaders.DLT_ORIGINAL_CONSUMER_GROUP, "inventory-test".getBytes(StandardCharsets.UTF_8));
        record.headers().add(KafkaHeaders.DLT_ORIGINAL_PARTITION, ByteBuffer.allocate(4).putInt(1).array());
        record.headers().add(KafkaHeaders.DLT_ORIGINAL_OFFSET, ByteBuffer.allocate(8).putLong(originOffset).array());
        return record;
    }

    static Selection selection(ConsumerRecord<byte[], byte[]> record, UUID id) {
        return new Selection(new Coordinate(record.topic(), record.partition(), record.offset()), id, sha(record.value()));
    }

    static Item item(UUID id, long dltOffset, long originOffset) {
        var record = record(id, 1, dltOffset, originOffset);
        return DltReader.validateRecord(selection(record, id), ROUTE, record);
    }

    @Test void canonicalOrderingAndContentAreBoundToDigest() {
        var a = item(UUID.randomUUID(), 0, 0); var b = item(UUID.randomUUID(), 1, 1);
        UUID planId = UUID.randomUUID();
        var first = new Plan(1, planId, "2026-10-26T00:00:00Z", "operator", "fixed cause", 10, List.of(a, b));
        var reversed = new Plan(1, planId, first.createdAt(), "operator", "fixed cause", 10, List.of(b, a));
        assertThat(first.digest()).isEqualTo(reversed.digest());
        assertThat(new Plan(1, planId, first.createdAt(), "operator", "changed cause", 10, List.of(a, b)).digest())
                .isNotEqualTo(first.digest());
        assertThat(JSON.readValue(first.canonicalJson(), Plan.class).digest()).isEqualTo(first.digest());
    }

    @Test void duplicateDltCopiesDeduplicateByOriginRegardlessOfSelectionOrder() {
        UUID id = UUID.randomUUID();
        var a = item(id, 8, 3); var b = item(id, 1, 3);
        assertThat(ReplayPlanner.deduplicate(List.of(a, b, a))).containsExactly(b);
    }

    @Test void conflictingCopiesAndSameEventAtAnotherOffsetRequireReview() {
        UUID id = UUID.randomUUID(); var a = item(id, 1, 3);
        var changed = new Item(a.origin(), a.originGroup(), id, a.eventType(), 1, "a".repeat(64), a.rawLength(),
                a.keySha256(), a.headersSha256(), new Coordinate("source-dlt", 1, 2), ROUTE);
        assertThatThrownBy(() -> ReplayPlanner.deduplicate(List.of(a, changed))).hasMessageContaining("MANUAL_REVIEW");
        assertThatThrownBy(() -> ReplayPlanner.deduplicate(List.of(a, item(id, 2, 4)))).hasMessageContaining("MANUAL_REVIEW");
    }

    @ParameterizedTest
    @ValueSource(strings = {"topic", "group", "partition", "missing", "duplicate", "version", "overflow", "event", "hash", "identity", "type", "json"})
    void invalidProvenanceOrContractFailsClosed(String mutation) {
        UUID id = UUID.randomUUID(); var record = record(id, mutation.equals("version") ? 99 : 1, 0, 3);
        var selected = selection(record, id);
        switch (mutation) {
            case "topic" -> replace(record, KafkaHeaders.DLT_ORIGINAL_TOPIC, "wrong".getBytes(StandardCharsets.UTF_8));
            case "group" -> replace(record, KafkaHeaders.DLT_ORIGINAL_CONSUMER_GROUP, "unrelated".getBytes(StandardCharsets.UTF_8));
            case "partition" -> replace(record, KafkaHeaders.DLT_ORIGINAL_PARTITION, new byte[]{1});
            case "missing" -> record.headers().remove(KafkaHeaders.DLT_ORIGINAL_OFFSET);
            case "duplicate" -> record.headers().add(KafkaHeaders.DLT_ORIGINAL_OFFSET, ByteBuffer.allocate(8).putLong(3).array());
            case "event" -> selected = new Selection(selected.dlt(), UUID.randomUUID(), selected.rawSha256());
            case "hash" -> selected = new Selection(selected.dlt(), id, "a".repeat(64));
            case "identity" -> selected = new Selection(new Coordinate("source-dlt", 1, 99), id, selected.rawSha256());
            case "type", "json", "overflow" -> {
                byte[] changed = mutation.equals("json") ? "{".getBytes(StandardCharsets.UTF_8)
                        : (mutation.equals("overflow")
                            ? new String(record.value(), StandardCharsets.UTF_8).replace("\"eventVersion\":1", "\"eventVersion\":4294967297")
                            : new String(record.value(), StandardCharsets.UTF_8).replace("ORDER_PLACED", "UNKNOWN"))
                            .getBytes(StandardCharsets.UTF_8);
                var replacement = new ConsumerRecord<byte[], byte[]>(record.topic(), 1, 0, record.key(), changed);
                record.headers().forEach(h -> replacement.headers().add(h)); record = replacement;
                selected = selection(record, id);
            }
            default -> { }
        }
        var finalRecord = record; var finalSelection = selected;
        assertThatThrownBy(() -> DltReader.validateRecord(finalSelection, ROUTE, finalRecord)).isInstanceOf(RuntimeException.class);
    }

    @Test void nonzeroPartitionAndOriginalFingerprintArePreserved() {
        UUID id = UUID.randomUUID(); var record = record(id, 1, 4, 7);
        var item = DltReader.validateRecord(selection(record, id), ROUTE, record);
        assertThat(item.origin()).isEqualTo(new Coordinate("source", 1, 7));
        assertThat(item.rawSha256()).isEqualTo(sha(record.value()));
        assertThat(item.keySha256()).isEqualTo(sha(record.key()));
        assertThat(item.rawLength()).isEqualTo(record.value().length);
        assertThat(item.eventId()).isEqualTo(id);
    }

    @Test void unsafeRouteOrOversizedPlanIsRejected() {
        assertThatThrownBy(() -> new Route("source", "source-dlt", "group", "source", "source-dlt", List.of("ORDER_PLACED")))
                .hasMessage("INVALID_ROUTE_TOPOLOGY");
        assertThatThrownBy(() -> new Plan(1, UUID.randomUUID(), "2026-10-26T00:00:00Z", "a", "reason", 11,
                List.of(item(UUID.randomUUID(), 0, 0)))).hasMessage("INVALID_PLAN_BOUND");
    }

    private static void replace(ConsumerRecord<byte[], byte[]> record, String name, byte[] value) {
        record.headers().remove(name); record.headers().add(name, value);
    }
}
