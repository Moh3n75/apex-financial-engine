package com.apex.customer;

import com.apex.platform.contracts.PublishedContracts;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration(proxyBeanMethods = false)
public class PublishedContractConfiguration {
  @Bean PublishedContracts publishedContracts() {
    return new PublishedContracts(java.util.List.of(com.apex.customer.modules.customer.events.CustomerContracts.CREATED_V1));
  }
}
