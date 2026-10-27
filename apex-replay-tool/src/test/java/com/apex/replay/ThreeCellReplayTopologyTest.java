package com.apex.replay;

import static com.apex.replay.ReplayModel.*;
import static com.apex.replay.ReplayValidationTest.*;
import static org.assertj.core.api.Assertions.*;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Properties;
import java.util.UUID;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

@Tag("unit")
class ThreeCellReplayTopologyTest {
    static Route route(String type) {
        List<String> types = type.startsWith("PAYMENT_") ? List.of("PAYMENT_AUTHORIZED", "PAYMENT_DECLINED")
                : type.equals("INVENTORY_RELEASED") ? List.of("INVENTORY_RESERVED", "INVENTORY_RESERVATION_REJECTED", "INVENTORY_RELEASED")
                : List.of(type);
        return new Route("source", "source-dlt", "inventory-test", "inventory-replay", "inventory-replay-dlt", types);
    }

    static ConsumerRecord<byte[], byte[]> source(UUID id, String type, int version) {
        var original = record(id, version, 0, 0);
        String aggregate = type.startsWith("ORDER_") ? "ORDER"
                : type.startsWith("PAYMENT_") ? "PAYMENT_AUTHORIZATION" : "INVENTORY_RESERVATION";
        var changed = new ConsumerRecord<byte[], byte[]>(original.topic(), original.partition(), original.offset(), original.key(),
                new String(original.value(), StandardCharsets.UTF_8).replace("ORDER_PLACED", type)
                        .replace("\"aggregateType\":\"ORDER\"", "\"aggregateType\":\"" + aggregate + "\"")
                        .getBytes(StandardCharsets.UTF_8));
        original.headers().forEach(h -> changed.headers().add(h));
        return changed;
    }

    @ParameterizedTest
    @ValueSource(strings = {"ORDER_READY_FOR_PAYMENT", "PAYMENT_AUTHORIZED", "PAYMENT_DECLINED",
            "ORDER_CANCELLATION_REQUESTED", "INVENTORY_RELEASED"})
    void approvedV1FamiliesPreserveIdentityAndRejectUnsupportedVersion(String type) {
        UUID id = UUID.randomUUID(); var record = source(id, type, 1);
        var item = DltReader.validateRecord(selection(record, id), route(type), record);
        assertThat(item.eventId()).isEqualTo(id);
        assertThat(item.rawSha256()).isEqualTo(sha(record.value()));
        assertThat(item.eventType()).isEqualTo(type);
        var unsupported = source(id, type, 99);
        assertThatThrownBy(() -> DltReader.validateRecord(selection(unsupported, id), route(type), unsupported))
                .hasMessage("UNSUPPORTED_CONTRACT");
    }

    @Test
    void sameGroupMayHaveDistinctSourcesButEachSourceGroupAndDestinationAreUnambiguous() {
        var a = new Route("a", "a-dlt", "order-group", "a-replay", "a-replay-dlt", List.of("PAYMENT_AUTHORIZED", "PAYMENT_DECLINED"));
        var b = new Route("b", "b-dlt", "order-group", "b-replay", "b-replay-dlt", List.of("INVENTORY_RESERVED", "INVENTORY_RESERVATION_REJECTED", "INVENTORY_RELEASED"));
        assertThatThrownBy(() -> new DltReader(new Properties(), List.of(a, b))).hasMessage("BOOTSTRAP_REQUIRED");
        var duplicate = new Route("a", "a-dlt", "order-group", "other-replay", "other-replay-dlt", a.eventTypes());
        assertThatThrownBy(() -> new DltReader(new Properties(), List.of(a, duplicate))).hasMessage("DUPLICATE_SOURCE_GROUP_ROUTE");
        var shared = new Route("b", "b-dlt", "inventory-group", "a-replay", "a-replay-dlt", b.eventTypes());
        assertThatThrownBy(() -> new DltReader(new Properties(), List.of(a, shared))).hasMessage("SHARED_REPLAY_DESTINATION_FORBIDDEN");
    }

    @Test
    void cannotMixBusinessFamiliesOrApproveMismatchedBusinessIdentity() {
        assertThatThrownBy(() -> new Route("source", "source-dlt", "group", "replay", "replay-dlt",
                List.of("ORDER_PLACED", "PAYMENT_AUTHORIZED"))).hasMessage("INVALID_CONTRACT_SET");
        UUID id = UUID.randomUUID(); var valid = source(id, "ORDER_READY_FOR_PAYMENT", 1);
        var bad = new ConsumerRecord<byte[], byte[]>(valid.topic(), valid.partition(), valid.offset(), valid.key(),
                new String(valid.value(), StandardCharsets.UTF_8).replace("\"aggregateType\":\"ORDER\"",
                        "\"aggregateType\":\"PAYMENT_AUTHORIZATION\"").getBytes(StandardCharsets.UTF_8));
        valid.headers().forEach(h -> bad.headers().add(h));
        assertThatThrownBy(() -> DltReader.validateRecord(selection(bad, id), route("ORDER_READY_FOR_PAYMENT"), bad))
                .hasMessage("BUSINESS_IDENTITY_MISMATCH");
    }
}
