package com.apex.replay;

import static com.apex.replay.ReplayModel.*;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.UUID;

public final class ReplayPlanner {
    private ReplayPlanner() { }

    public static Plan plan(List<Selection> selections, Route route, String actor, String reason,
                            int limit, DltReader reader) {
        text(actor); text(reason);
        require(limit >= 1 && limit <= HARD_LIMIT && !selections.isEmpty() && selections.size() <= limit,
                "SELECTION_LIMIT_EXCEEDED");
        var items = new ArrayList<Item>();
        for (Selection selection : selections) { items.add(reader.validate(selection, route)); }
        return new Plan(1, UUID.randomUUID(), Instant.now().toString(), actor, reason, limit, deduplicate(items));
    }

    static List<Item> deduplicate(List<Item> items) {
        var origins = new HashMap<String, Item>();
        var eventOrigins = new HashMap<String, Coordinate>();
        for (Item item : items.stream().sorted(Comparator.comparing((Item i) -> i.dlt().topic())
                .thenComparingInt(i -> i.dlt().partition()).thenComparingLong(i -> i.dlt().offset())).toList()) {
            String originKey = item.originGroup() + ":" + item.origin();
            var previous = origins.putIfAbsent(originKey, item);
            if (previous != null) {
                require(previous.eventId().equals(item.eventId()) && previous.rawSha256().equals(item.rawSha256())
                        && previous.keySha256().equals(item.keySha256()) && previous.route().equals(item.route()),
                        "CONFLICTING_DLT_COPIES_MANUAL_REVIEW");
            }
            var previousOrigin = eventOrigins.putIfAbsent(item.originGroup() + ":" + item.eventId(), item.origin());
            require(previousOrigin == null || previousOrigin.equals(item.origin()), "EVENT_AT_MULTIPLE_OFFSETS_MANUAL_REVIEW");
        }
        return List.copyOf(origins.values());
    }
}
