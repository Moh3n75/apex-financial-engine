package com.apex.order.contracts;

import com.apex.platform.grpc.api.CellQuery;
import java.util.Objects;
import java.util.UUID;

@CellQuery(type = "GET_ORDER", version = 1, response = OrderView.class)
public record GetOrderQuery(UUID orderId) {
  public GetOrderQuery {
    Objects.requireNonNull(orderId);
  }
}
