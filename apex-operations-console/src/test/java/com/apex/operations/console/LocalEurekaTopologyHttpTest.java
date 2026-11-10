package com.apex.operations.console;

import static org.mockito.Mockito.*;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.*;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;
import com.apex.operations.console.security.OpsM36aLegacySecurityConfiguration;
import com.netflix.appinfo.InstanceInfo;
import com.netflix.discovery.DiscoveryClient;
import com.netflix.discovery.EurekaClient;
import com.netflix.discovery.EurekaClientConfig;
import com.netflix.discovery.shared.Application;
import com.netflix.discovery.shared.Applications;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.webmvc.test.autoconfigure.WebMvcTest;
import org.springframework.context.annotation.*;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.servlet.MockMvc;

/** HTTP integration with a mocked registry cache; not live Eureka acceptance. */
@WebMvcTest(controllers = TopologyApi.class)
@ActiveProfiles("local")
@TestPropertySource(properties = {"apex.ops.m36a.enabled=false", "apex.ops.local-inventory.environment=local",
    "apex.ops.local-inventory.enabled=true"})
@Import({LocalEurekaTopologyInventory.class, OpsM36aLegacySecurityConfiguration.class,
    LocalEurekaTopologyHttpTest.Registry.class})
class LocalEurekaTopologyHttpTest {
  @Autowired MockMvc mvc;
  @Autowired EurekaClient client;
  @MockitoBean OwnerPoller owners;
  @TestConfiguration static class Registry {
    @Bean EurekaClient eureka() { return mock(DiscoveryClient.class); }
    @Bean EurekaClientConfig config() {
      var config = mock(EurekaClientConfig.class);
      when(config.getEurekaServerServiceUrls("defaultZone")).thenReturn(List.of("http://127.0.0.1:8761/eureka/"));
      return config;
    }
  }
  @Test void noAuthVersionTwoUsesRegistryWhileVersionOneAndReadOnlyPolicyStayIntact() throws Exception {
    when(owners.views()).thenReturn(List.of());
    when(((DiscoveryClient)client).getLastSuccessfulRegistryFetchTimePeriod()).thenReturn(500L);
    var applications = new Applications(); var app = new Application("ORDER-CELL");
    app.addInstance(InstanceInfo.Builder.newBuilder().setAppName("ORDER-CELL").setInstanceId("actual-registry-id")
        .setHostName("private-host").setStatus(InstanceInfo.InstanceStatus.UP).build());
    applications.addApplication(app); when(client.getApplications()).thenReturn(applications);
    mvc.perform(get("/ops/v1/topology?schemaVersion=2")).andExpect(status().isOk())
        .andExpect(jsonPath("$.nodes[0].id").value("local:service:order-cell"))
        .andExpect(jsonPath("$.nodes[0].health").value("UNKNOWN"))
        .andExpect(jsonPath("$.nodes[0].evidence[0].source").value("EUREKA"))
        .andExpect(jsonPath("$.connections").isEmpty()).andExpect(jsonPath("$.memberships").isEmpty());
    mvc.perform(get("/ops/v1/topology?schemaVersion=1")).andExpect(status().isOk()).andExpect(jsonPath("$.nodes").isEmpty());
    mvc.perform(post("/ops/v1/topology?schemaVersion=2")).andExpect(status().isMethodNotAllowed());
    when(client.getApplications()).thenReturn(new Applications());
    mvc.perform(get("/ops/v1/topology?schemaVersion=2")).andExpect(status().isOk()).andExpect(jsonPath("$.nodes").isEmpty());
  }
}
