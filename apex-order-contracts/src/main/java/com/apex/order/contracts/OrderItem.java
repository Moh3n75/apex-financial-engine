package com.apex.order.contracts;

import java.util.Objects;
import java.util.UUID;

public record OrderItem(UUID productId, int quantity) {
  public OrderItem {
    Objects.requireNonNull(productId, "productId");
    if (quantity < 1) throw new IllegalArgumentException("Positive quantity required");
  }
}
