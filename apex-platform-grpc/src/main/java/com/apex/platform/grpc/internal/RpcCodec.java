package com.apex.platform.grpc.internal;

import static com.apex.platform.grpc.api.GatewayException.Code.*;

import com.apex.platform.grpc.api.GatewayException;
import java.security.MessageDigest;
import java.util.HexFormat;
import tools.jackson.core.StreamReadConstraints;
import tools.jackson.core.StreamReadFeature;
import tools.jackson.core.json.JsonFactory;
import tools.jackson.databind.DeserializationFeature;
import tools.jackson.databind.MapperFeature;
import tools.jackson.databind.json.JsonMapper;

/** Typed allowlisted records only; never default typing or Java class names on wire. */
public final class RpcCodec {
  private final int limit;
  private final JsonMapper mapper;

  public RpcCodec(int limit) {
    this.limit = limit;
    mapper =
        JsonMapper.builder(
                JsonFactory.builder()
                    .streamReadConstraints(
                        StreamReadConstraints.builder()
                            .maxNestingDepth(16)
                            .maxStringLength(limit)
                            .maxNumberLength(128)
                            .maxDocumentLength(limit)
                            .build())
                    .enable(StreamReadFeature.STRICT_DUPLICATE_DETECTION)
                    .build())
            .enable(
                DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES,
                DeserializationFeature.FAIL_ON_NULL_FOR_PRIMITIVES)
            .enable(MapperFeature.SORT_PROPERTIES_ALPHABETICALLY)
            .build();
  }

  public byte[] encode(Object value, Class<?> expected) {
    if (value == null || !expected.isInstance(value)) throw new GatewayException(INTERNAL, false);
    try {
      byte[] bytes = mapper.writeValueAsBytes(value);
      check(bytes);
      return bytes;
    } catch (GatewayException e) {
      throw e;
    } catch (Exception e) {
      throw new GatewayException(INVALID_ARGUMENT, false);
    }
  }

  public <T> T decode(byte[] bytes, Class<T> expected) {
    check(bytes);
    try {
      T value = mapper.readValue(bytes, expected);
      if (value == null) throw new IllegalArgumentException();
      return value;
    } catch (Exception e) {
      throw new GatewayException(INVALID_ARGUMENT, false);
    }
  }

  private void check(byte[] value) {
    if (value.length > limit) throw new GatewayException(RESOURCE_EXHAUSTED, false);
  }

  public String fingerprint(Object value, Class<?> type) {
    try {
      return HexFormat.of()
          .formatHex(MessageDigest.getInstance("SHA-256").digest(encode(value, type)));
    } catch (java.security.NoSuchAlgorithmException impossible) {
      throw new IllegalStateException(impossible);
    }
  }
}
