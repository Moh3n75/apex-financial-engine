package com.apex.order.modules.order.grpc;

import com.apex.order.contracts.*;
import com.apex.order.modules.order.domain.OrderRepository;
import com.apex.platform.grpc.api.*;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

@Component
@Profile("grpc")
public class OrderPerspectiveHandler {
  private final OrderRepository orders;

  public OrderPerspectiveHandler(OrderRepository orders) {
    this.orders = orders;
  }

  @CellQueryHandler(type = "GET_ORDER_PERSPECTIVE", version = 1)
  @Transactional(readOnly = true, timeout = 5)
  public OrderPerspective perspective(GetOrderPerspective query) {
    return new OrderPerspective(
        query.orderId(),
        "order-cell",
        orders
            .findStatus(query.orderId())
            .orElseThrow(() -> new GatewayException(GatewayException.Code.NOT_FOUND, false))
            .name());
  }
}
