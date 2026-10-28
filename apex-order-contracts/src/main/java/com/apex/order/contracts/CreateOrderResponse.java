package com.apex.order.contracts;

import java.util.Objects;
import java.util.UUID;

/** A committed placement receipt, not a completed Inventory/Payment Saga. */
public record CreateOrderResponse(UUID orderId, String status) {
  public CreateOrderResponse {
    Objects.requireNonNull(orderId);
    Objects.requireNonNull(status);
  }
}
