package com.apex.platform.messaging.jdbc;

import java.util.LinkedHashSet;

/** One exact topic-list contract for listener registration and metadata preflight. */
public final class ConsumerTopics {
    private ConsumerTopics() { }

    public static String[] parse(String configured) {
        if (configured == null) { throw new IllegalArgumentException("APEX consumer topics are required"); }
        var topics = new LinkedHashSet<String>();
        for (String part : configured.split(",", -1)) {
            String topic = part.trim();
            if (!topic.matches("[a-zA-Z0-9._-]{1,249}") || topic.equals(".") || topic.equals("..")
                    || !topics.add(topic)) {
                throw new IllegalArgumentException("APEX consumer topics must be distinct, nonempty exact Kafka topic names");
            }
        }
        return topics.toArray(String[]::new);
    }
}
