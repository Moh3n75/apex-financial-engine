package com.apex.registry;

import static org.assertj.core.api.Assertions.*;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.springframework.mock.env.MockEnvironment;

@Tag("unit")
class RegistryExposureTest {
  @Test
  void defaultAndPublicRegistryExposureFailClosed() {
    var app = new DiscoveryServerApplication();
    assertThatThrownBy(
            () -> app.registryExposureGuard(new MockEnvironment()).afterSingletonsInstantiated())
        .hasMessageContaining("explicit only");
    var publicBind =
        new MockEnvironment()
            .withProperty("apex.registry.loopback-development", "true")
            .withProperty("server.address", "0.0.0.0");
    assertThatThrownBy(() -> app.registryExposureGuard(publicBind).afterSingletonsInstantiated())
        .hasMessageContaining("explicit only");
  }

  @Test
  void onlyExplicitLoopbackDevelopmentCanStart() {
    var env =
        new MockEnvironment()
            .withProperty("apex.registry.loopback-development", "true")
            .withProperty("server.address", "127.0.0.1");
    new DiscoveryServerApplication().registryExposureGuard(env).afterSingletonsInstantiated();
  }
}
