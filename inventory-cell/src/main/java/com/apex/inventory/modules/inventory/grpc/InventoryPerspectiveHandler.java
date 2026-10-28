package com.apex.inventory.modules.inventory.grpc;

import com.apex.order.contracts.*;
import com.apex.platform.grpc.api.*;
import org.springframework.context.annotation.Profile;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

@Component
@Profile("grpc")
public class InventoryPerspectiveHandler {
  private final JdbcTemplate jdbc;

  public InventoryPerspectiveHandler(JdbcTemplate jdbc) {
    this.jdbc = jdbc;
  }

  @CellQueryHandler(type = "GET_ORDER_PERSPECTIVE", version = 1)
  @Transactional(readOnly = true, timeout = 5)
  public OrderPerspective perspective(GetOrderPerspective query) {
    var rows =
        jdbc.query(
            "SELECT status FROM inventory_reservations WHERE order_id=?",
            (rs, n) -> rs.getString(1),
            query.orderId());
    if (rows.isEmpty()) throw new GatewayException(GatewayException.Code.NOT_FOUND, false);
    return new OrderPerspective(query.orderId(), "inventory-cell", rows.getFirst());
  }
}
