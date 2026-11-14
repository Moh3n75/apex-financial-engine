package com.apex.platform.messaging.jdbc;

import java.util.List;
import java.util.HashMap;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

/** Opt-in exact contract routes. Missing routes never fall back in enabled mode. */
@ConfigurationProperties("apex.messaging.publisher.routing")
public record PublisherRouting(@DefaultValue("false") boolean enabled, List<Route> routes, List<String> requiredContracts) {
    public PublisherRouting(boolean enabled, List<Route> routes) { this(enabled,routes,List.of()); }
    public record Route(String eventType, int eventVersion, String topic) {
        public Route { topic = topic == null ? null : topic.trim(); }
    }
    @org.springframework.boot.context.properties.bind.ConstructorBinding
    public PublisherRouting {
        routes = routes == null ? List.of() : List.copyOf(routes);
        requiredContracts = requiredContracts == null ? List.of() : List.copyOf(requiredContracts);
        var unique = new HashMap<String,String>();
        for (var route : routes) {
            if (route.eventType() == null || route.eventType().isBlank() || route.eventVersion() < 1
                    || ConsumerTopics.parse(route.topic()).length != 1
                    || unique.put(route.eventType() + ":" + route.eventVersion(), route.topic()) != null) {
                throw new IllegalArgumentException("Invalid or duplicate publisher contract route");
            }
        }
        if (enabled && routes.isEmpty()) { throw new IllegalArgumentException("Enabled routing requires explicit contract routes"); }
        if (enabled && !unique.keySet().containsAll(requiredContracts)) { throw new IllegalArgumentException("Missing required publisher routes"); }
    }
    public String destination(String type, int version, String legacy) {
        if (!enabled) { return legacy; }
        return routes.stream().filter(r -> r.eventType().equals(type) && r.eventVersion()==version)
                .map(Route::topic).findFirst().orElseThrow(() -> new IllegalStateException("Missing publisher contract route"));
    }
}
