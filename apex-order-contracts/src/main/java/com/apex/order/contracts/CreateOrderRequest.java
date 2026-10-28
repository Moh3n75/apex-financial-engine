package com.apex.order.contracts;

import com.apex.platform.grpc.api.CellRequest;
import java.util.*;

@CellRequest(type = "CREATE_ORDER", version = 1, response = CreateOrderResponse.class)
public record CreateOrderRequest(
    String externalReference, String description, List<OrderItem> items) {
  public CreateOrderRequest {
    if (externalReference == null
        || externalReference.isBlank()
        || externalReference.length() > 64
        || description == null
        || description.isBlank()
        || description.length() > 200
        || items == null
        || items.isEmpty()
        || items.size() > 100) throw new IllegalArgumentException("Invalid Order request");
    items = List.copyOf(items);
    if (items.stream().map(OrderItem::productId).distinct().count() != items.size())
      throw new IllegalArgumentException("Duplicate product");
  }
}
