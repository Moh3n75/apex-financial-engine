package com.apex.order.contracts;

import com.apex.platform.grpc.api.CellQuery;
import java.util.Objects;
import java.util.UUID;

@CellQuery(type = "GET_ORDER_PERSPECTIVE", version = 1, response = OrderPerspective.class)
public record GetOrderPerspective(UUID orderId) {
  public GetOrderPerspective {
    Objects.requireNonNull(orderId, "orderId");
  }
}
