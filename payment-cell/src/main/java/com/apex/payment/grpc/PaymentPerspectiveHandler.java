package com.apex.payment.grpc;

import com.apex.order.contracts.*;
import com.apex.platform.grpc.api.*;
import org.springframework.context.annotation.Profile;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

@Component
@Profile("grpc")
public class PaymentPerspectiveHandler {
  private final JdbcTemplate jdbc;

  public PaymentPerspectiveHandler(JdbcTemplate jdbc) {
    this.jdbc = jdbc;
  }

  @CellQueryHandler(type = "GET_ORDER_PERSPECTIVE", version = 1)
  @Transactional(readOnly = true, timeout = 5)
  public OrderPerspective perspective(GetOrderPerspective query) {
    var rows =
        jdbc.query(
            "SELECT status FROM payment_authorizations WHERE order_id=?",
            (rs, n) -> rs.getString(1),
            query.orderId());
    if (rows.isEmpty()) throw new GatewayException(GatewayException.Code.NOT_FOUND, false);
    return new OrderPerspective(query.orderId(), "payment-cell", rows.getFirst());
  }
}
