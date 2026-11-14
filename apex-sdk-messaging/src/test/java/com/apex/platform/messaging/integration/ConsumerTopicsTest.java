package com.apex.platform.messaging.integration;

import com.apex.platform.messaging.jdbc.ConsumerTopics;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import static org.assertj.core.api.Assertions.*;

@Tag("unit")
class ConsumerTopicsTest {
    @Test void singleAndMultipleExactTopics() {
        assertThat(ConsumerTopics.parse(" source ")).containsExactly("source");
        assertThat(ConsumerTopics.parse(" source , replay ")).containsExactly("source", "replay");
    }
    @ParameterizedTest @ValueSource(strings={"", " ", ",", "source,", "source,,replay", "source,source", "bad/topic", ".", ".."})
    void rejectsUnsafeTopicLists(String configured) {
        assertThatThrownBy(() -> ConsumerTopics.parse(configured)).isInstanceOf(IllegalArgumentException.class);
    }
}
