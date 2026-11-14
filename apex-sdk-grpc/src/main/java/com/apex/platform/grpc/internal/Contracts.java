package com.apex.platform.grpc.internal;

import com.apex.platform.grpc.api.*;
import java.lang.reflect.*;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.*;

public final class Contracts {
  private Contracts() {}

  public record Contract(
      boolean query, String type, int version, Class<?> payload, Class<?> response) {}

  public static Contract inspect(Class<?> input, boolean query) {
    var r = input.getAnnotation(CellRequest.class);
    var q = input.getAnnotation(CellQuery.class);
    if ((query && q == null) || (!query && r == null) || (r != null && q != null))
      throw new IllegalArgumentException(
          "Missing or ambiguous Java Cell contract: " + input.getName());
    var c =
        query
            ? new Contract(true, q.type(), q.version(), input, q.response())
            : new Contract(false, r.type(), r.version(), input, r.response());
    if (!c.type().matches("[A-Z][A-Z0-9_]{0,63}") || c.version() < 1 || c.version() > 10000)
      throw new IllegalArgumentException("Invalid Cell contract: " + input.getName());
    validateRecord(input, new HashSet<>(), 0);
    validateRecord(c.response(), new HashSet<>(), 0);
    return c;
  }

  private static void validateRecord(Class<?> type, Set<Class<?>> visiting, int depth) {
    if (!type.isRecord()
        || !Modifier.isPublic(type.getModifiers())
        || depth > 8
        || !visiting.add(type))
      throw new IllegalArgumentException(
          "Contract DTO must be a public nonrecursive record: " + type.getName());
    for (var component : type.getRecordComponents())
      validateType(component.getGenericType(), visiting, depth + 1);
    visiting.remove(type);
  }

  private static void validateType(Type type, Set<Class<?>> visiting, int depth) {
    if (type instanceof ParameterizedType p
        && p.getRawType() == List.class
        && p.getActualTypeArguments().length == 1) {
      validateType(p.getActualTypeArguments()[0], visiting, depth + 1);
      return;
    }
    if (!(type instanceof Class<?> c))
      throw new IllegalArgumentException("Concrete DTO component type required");
    if (Set.of(
                String.class,
                UUID.class,
                Instant.class,
                BigDecimal.class,
                int.class,
                long.class,
                boolean.class,
                Integer.class,
                Long.class,
                Boolean.class)
            .contains(c)
        || c.isEnum()) return;
    validateRecord(c, visiting, depth);
  }
}
