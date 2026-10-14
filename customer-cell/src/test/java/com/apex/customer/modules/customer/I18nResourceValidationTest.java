package com.apex.customer.modules.customer;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Map;

import org.junit.jupiter.api.Test;

import tools.jackson.databind.json.JsonMapper;

/**
 * Resource validation only. HTTP Accept-Language localization is BLOCKED until
 * an external APEX JSON translation resolver exists.
 */
class I18nResourceValidationTest {

    private static final JsonMapper JSON = JsonMapper.builder().build();

    @Test
    void englishMessagesAreValidUtf8Json() throws Exception {
        Map<String, Object> map = readObject("i18n/messages_en.json");
        assertThat(map.get("customer.error.notFound")).isEqualTo("Customer not found");
    }

    @Test
    void persianMessagesPreserveUtf8() throws Exception {
        Map<String, Object> map = readObject("i18n/messages_fa.json");
        assertThat(map.get("customer.error.notFound")).isEqualTo("مشتری پیدا نشد");
        assertThat(map.get("customer.error.duplicateReference")).isEqualTo("شناسه مرجع مشتری تکراری است");
    }

    private static Map<String, Object> readObject(String location) throws Exception {
        try (InputStream in = Thread.currentThread().getContextClassLoader().getResourceAsStream(location)) {
            assertThat(in).as(location).isNotNull();
            String text = new String(in.readAllBytes(), StandardCharsets.UTF_8);
            @SuppressWarnings("unchecked")
            Map<String, Object> map = JSON.readValue(text, Map.class);
            return map;
        }
    }
}
