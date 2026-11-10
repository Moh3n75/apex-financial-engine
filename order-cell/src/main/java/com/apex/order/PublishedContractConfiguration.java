package com.apex.order;

import com.apex.platform.contracts.PublishedContracts;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration(proxyBeanMethods = false)
public class PublishedContractConfiguration {
  @Bean PublishedContracts publishedContracts() {
    return new PublishedContracts(java.util.List.of(com.apex.order.modules.order.events.OrderContracts.CREATED_V1,
        com.apex.order.modules.order.events.OrderContracts.PLACED_V1,
        com.apex.order.modules.order.application.ThreeCellOrderHandler.READY,
        com.apex.order.modules.order.application.ThreeCellOrderHandler.CANCEL));
  }
}
