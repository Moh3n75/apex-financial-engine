package com.apex.replay;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.time.Instant;
import java.util.Comparator;
import java.util.HexFormat;
import java.util.List;
import java.util.UUID;

import tools.jackson.databind.json.JsonMapper;

/** Tool-local contracts; no production Cell classes or runtime auto-configuration. */
public final class ReplayModel {
    public static final int HARD_LIMIT = 10;
    static final JsonMapper JSON = JsonMapper.builder().build();

    private ReplayModel() { }

    public record Coordinate(String topic, int partition, long offset) {
        public Coordinate {
            ReplayModel.topic(topic);
            require(partition >= 0 && offset >= 0, "INVALID_COORDINATE");
        }
    }

    public record Selection(Coordinate dlt, UUID eventId, String rawSha256) {
        public Selection {
            require(dlt != null && eventId != null, "EXPECTED_ID_REQUIRED");
            hash(rawSha256);
        }
    }

    public record Route(String originTopic, String dltTopic, String targetGroup,
                        String replayTopic, String replayDltTopic, List<String> eventTypes) {
        public Route {
            topic(originTopic); topic(dltTopic); topic(replayTopic); topic(replayDltTopic);
            text(targetGroup);
            require(!originTopic.equals(replayTopic) && dltTopic.equals(originTopic + "-dlt")
                    && replayDltTopic.equals(replayTopic + "-dlt"), "INVALID_ROUTE_TOPOLOGY");
            require(eventTypes != null && !eventTypes.isEmpty(), "CONTRACT_ALLOWLIST_REQUIRED");
            eventTypes = eventTypes.stream().sorted().distinct().toList();
            require(eventTypes.stream().allMatch(type -> List.of("ORDER_PLACED", "INVENTORY_RESERVED",
                    "INVENTORY_RESERVATION_REJECTED").contains(type)), "UNSUPPORTED_CONTRACT_ALLOWLIST");
            require(eventTypes.equals(List.of("ORDER_PLACED")) || eventTypes.equals(List.of(
                    "INVENTORY_RESERVATION_REJECTED", "INVENTORY_RESERVED")), "INVALID_CONTRACT_SET");
        }
    }

    public record Item(Coordinate origin, String originGroup, UUID eventId, String eventType, int eventVersion,
                       String rawSha256, int rawLength, String keySha256, String headersSha256,
                       Coordinate dlt, Route route) {
        public Item {
            require(origin != null && dlt != null && eventId != null && route != null, "INVALID_PLAN_ITEM");
            hash(rawSha256); hash(headersSha256);
            if (!"NULL".equals(keySha256)) { hash(keySha256); }
            require(rawLength > 0 && rawLength <= 1024 * 1024 && eventVersion == 1
                    && route.eventTypes().contains(eventType) && route.targetGroup().equals(originGroup)
                    && origin.topic().equals(route.originTopic()) && dlt.topic().equals(route.dltTopic())
                    && dlt.partition() == origin.partition(), "INVALID_PLAN_ITEM_ROUTE");
        }
    }

    public record Plan(int formatVersion, UUID planId, String createdAt, String actor, String reason,
                       int limit, List<Item> items) {
        public Plan {
            require(formatVersion == 1 && planId != null, "INVALID_PLAN_VERSION");
            Instant.parse(createdAt); text(actor); text(reason);
            require(limit > 0 && limit <= HARD_LIMIT && items != null && !items.isEmpty()
                    && items.size() <= limit, "INVALID_PLAN_BOUND");
            items = items.stream().sorted(Comparator.comparing((Item i) -> i.origin().topic())
                    .thenComparingInt(i -> i.origin().partition()).thenComparingLong(i -> i.origin().offset())
                    .thenComparing(i -> i.eventId().toString())).toList();
        }

        public String canonicalJson() { return JSON.writeValueAsString(this); }
        public String digest() { return sha(canonicalJson().getBytes(StandardCharsets.UTF_8)); }
    }

    static String sha(byte[] bytes) {
        try { return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(bytes)); }
        catch (java.security.NoSuchAlgorithmException impossible) { throw new IllegalStateException(impossible); }
    }

    static void hash(String value) { require(value != null && value.matches("[a-f0-9]{64}"), "INVALID_HASH"); }
    static void text(String value) { require(value != null && !value.isBlank() && value.length() <= 500
            && value.chars().noneMatch(Character::isISOControl), "INVALID_OPERATOR_TEXT"); }
    static void topic(String value) { require(value != null && value.matches("[a-zA-Z0-9._-]{1,249}")
            && !value.equals(".") && !value.equals(".."), "INVALID_TOPIC"); }
    static void require(boolean condition, String code) {
        if (!condition) { throw new IllegalArgumentException(code); }
    }
}
