package com.apex.cell;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Map;

import org.junit.jupiter.api.Test;

import tools.jackson.databind.json.JsonMapper;

/**
 * Resource validation only — not a translation resolver.
 * Localization remains PENDING until an external APEX translation facility is wired.
 */
class I18nResourceValidationTest {

    private static final JsonMapper JSON = JsonMapper.builder().build();

    @Test
    void englishMessagesAreValidUtf8JsonObject() throws Exception {
        assertValidEmptyJsonObject("i18n/messages_en.json");
    }

    @Test
    void persianMessagesAreValidUtf8JsonObject() throws Exception {
        assertValidEmptyJsonObject("i18n/messages_fa.json");
    }

    private static void assertValidEmptyJsonObject(String classpathLocation) throws Exception {
        try (InputStream in = Thread.currentThread()
                .getContextClassLoader()
                .getResourceAsStream(classpathLocation)) {
            assertThat(in)
                    .as("%s must exist on the classpath", classpathLocation)
                    .isNotNull();

            byte[] bytes = in.readAllBytes();
            assertThat(bytes.length).isGreaterThan(0);

            String text = new String(bytes, StandardCharsets.UTF_8);
            @SuppressWarnings("unchecked")
            Map<String, Object> object = JSON.readValue(text, Map.class);

            assertThat(object).isNotNull();
        }
    }
}
