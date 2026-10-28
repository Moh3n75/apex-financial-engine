package com.apex.order.modules.order.grpc;

import com.apex.order.contracts.*;
import com.apex.order.modules.order.application.PlaceOrderUseCase;
import com.apex.order.modules.order.domain.OrderRepository;
import com.apex.platform.grpc.api.*;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

@Component
@Profile("grpc")
public class OrderRpcHandlers {
  private final PlaceOrderUseCase placeOrder;
  private final OrderRepository orders;

  public OrderRpcHandlers(PlaceOrderUseCase placeOrder, OrderRepository orders) {
    this.placeOrder = placeOrder;
    this.orders = orders;
  }

  @CellRequestHandler(type = "CREATE_ORDER", version = 1)
  public CreateOrderResponse create(CreateOrderRequest request, RequestContext context) {
    var order =
        placeOrder.execute(
            request.externalReference(),
            request.description(),
            request.items().stream()
                .map(
                    i ->
                        new com.apex.order.modules.order.domain.OrderItem(
                            i.productId(), i.quantity()))
                .toList());
    return new CreateOrderResponse(order.id(), order.status().name());
  }

  @CellQueryHandler(type = "GET_ORDER", version = 1)
  @Transactional(readOnly = true, timeout = 5)
  public OrderView get(GetOrderQuery query) {
    var status =
        orders
            .findStatus(query.orderId())
            .orElseThrow(() -> new GatewayException(GatewayException.Code.NOT_FOUND, false));
    return new OrderView(query.orderId(), status.name());
  }
}
